// Background downloader: paste a URL, the server downloads it, you grab it later.
// Zero dependencies. Needs Node 18+.
//
// Speed: when the host supports range requests, the file is split into chunks and
// fetched over several connections at once (like IDM / aria2). If the host doesn't
// support ranges, it falls back to a single connection automatically.

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { Readable } = require('stream');
const { pipeline } = require('stream/promises');

const PORT = process.env.PORT || 3000;
const PASSWORD = process.env.PASSWORD;
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const FILES_DIR = path.join(DATA_DIR, 'files');
const JOBS_FILE = path.join(DATA_DIR, 'jobs.json');
const MAX_CONCURRENT = parseInt(process.env.MAX_CONCURRENT || '2', 10);
const DEFAULT_CONNECTIONS = Math.min(
  32,
  Math.max(1, parseInt(process.env.CONNECTIONS || '8', 10) || 8)
);
const MIN_SEGMENT_BYTES = 8 * 1024 * 1024; // smaller files just use one connection
const MAX_RETRIES = 8;
const UA =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36';

if (!PASSWORD) {
  console.error('Set the PASSWORD environment variable before starting.');
  process.exit(1);
}

fs.mkdirSync(FILES_DIR, { recursive: true });

// Login session: one long-lived cookie derived from the password.
// Changing PASSWORD logs everyone out.
const COOKIE_NAME = 'dl_session';
const COOKIE_MAX_AGE = 180 * 24 * 3600; // 180 days
const SESSION_TOKEN = crypto.createHmac('sha256', PASSWORD).update('dl-session-v1').digest('hex');

// ---------- state ----------
let jobs = [];
try {
  jobs = JSON.parse(fs.readFileSync(JOBS_FILE, 'utf8'));
} catch {}
// anything that was mid-download when the server stopped gets resumed
for (const j of jobs) {
  if (j.status === 'downloading') j.status = 'queued';
  j.speed = 0;
  j.retryAt = null;
}

const controllers = new Map();
let saveTimer = null;
function save() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    fs.writeFile(JOBS_FILE, JSON.stringify(jobs), () => {});
  }, 500);
}

// ---------- helpers ----------
function sanitize(name) {
  try {
    name = decodeURIComponent(name || '');
  } catch {
    name = String(name || '');
  }
  name = name.replace(/[\\/:*?"<>|\x00-\x1f]/g, '_').trim();
  name = name.replace(/^\.+/, '');
  return name.slice(0, 200) || 'download';
}

function nameFromUrl(url) {
  try {
    const p = new URL(url).pathname;
    return sanitize(path.basename(p));
  } catch {
    return 'download';
  }
}

function nameFromHeaders(res) {
  const cd = res.headers.get('content-disposition') || '';
  let m = cd.match(/filename\*=UTF-8''([^;]+)/i);
  if (m) return sanitize(m[1]);
  m = cd.match(/filename="?([^";]+)"?/i);
  if (m) return sanitize(m[1]);
  return null;
}

function uniqueName(name, selfId) {
  const taken = new Set(jobs.filter((j) => j.id !== selfId).map((j) => j.filename));
  const ext = path.extname(name);
  const base = name.slice(0, name.length - ext.length);
  let candidate = name;
  let i = 1;
  while (taken.has(candidate) || fs.existsSync(path.join(FILES_DIR, candidate))) {
    candidate = `${base} (${i++})${ext}`;
  }
  return candidate;
}

function partPath(job) {
  return path.join(FILES_DIR, job.filename + '.part');
}
function finalPath(job) {
  return path.join(FILES_DIR, job.filename);
}

const sleep = (ms, signal) =>
  new Promise((resolve) => {
    const t = setTimeout(resolve, ms);
    signal.addEventListener(
      'abort',
      () => {
        clearTimeout(t);
        resolve();
      },
      { once: true }
    );
  });

// ---------- download engine ----------
function pump() {
  let active = jobs.filter((j) => j.status === 'downloading').length;
  for (const j of jobs) {
    if (active >= MAX_CONCURRENT) break;
    if (j.status === 'queued' && !j.retryAt) {
      active++;
      runJob(j);
    }
  }
}

// Ask for the first byte only: tells us the file size, whether ranges work,
// the real filename, and the final URL after redirects.
async function probe(job, signal) {
  const res = await fetch(job.url, {
    headers: { 'User-Agent': UA, Range: 'bytes=0-0' },
    signal,
    redirect: 'follow',
  });
  const info = {
    supported: false,
    total: null,
    size: null,
    finalUrl: res.url || job.url,
    nameHeader: nameFromHeaders(res),
  };
  if (!res.ok) {
    try {
      await res.body?.cancel();
    } catch {}
    const e = new Error(`Server responded ${res.status} ${res.statusText}`);
    e.status = res.status;
    throw e;
  }
  if (res.status === 206) {
    const m = (res.headers.get('content-range') || '').match(/\/(\d+)$/);
    if (m) {
      info.total = parseInt(m[1], 10);
      info.supported = true;
    }
  } else {
    const len = parseInt(res.headers.get('content-length') || '', 10);
    if (!isNaN(len)) info.size = len;
  }
  if (info.total) info.size = info.total;
  try {
    await res.body?.cancel();
  } catch {}
  return info;
}

function makeChunks(total, existing, connections) {
  const size = Math.max(1 << 20, Math.min(16 << 20, Math.ceil(total / (connections * 4))));
  const chunks = [];
  for (let s = 0; s < total; s += size) {
    const e = Math.min(s + size, total) - 1;
    chunks.push({ start: s, end: e, pos: Math.min(Math.max(existing, s), e + 1) });
  }
  return chunks;
}

async function fetchChunk(job, chunk, url, signal, fh) {
  const res = await fetch(url, {
    headers: { 'User-Agent': UA, Range: `bytes=${chunk.pos}-${chunk.end}` },
    signal,
    redirect: 'follow',
  });
  const bail = async (msg, extra) => {
    try {
      await res.body?.cancel();
    } catch {}
    throw Object.assign(new Error(msg), extra);
  };
  if (res.status !== 206) {
    if (res.status === 200) await bail('Server stopped supporting ranges', { fatal: true });
    await bail(`Server responded ${res.status} ${res.statusText}`, { status: res.status });
  }
  const cr = (res.headers.get('content-range') || '').match(/bytes (\d+)-(\d+)\/(\d+|\*)/);
  if (!cr || parseInt(cr[1], 10) !== chunk.pos) {
    await bail('Unexpected range response', { fatal: true });
  }
  if (cr[3] !== '*' && parseInt(cr[3], 10) !== job.total) {
    await bail('File changed on the server', { fatal: true });
  }

  for await (const buf of Readable.fromWeb(res.body)) {
    const remaining = chunk.end - chunk.pos + 1;
    if (remaining <= 0) break;
    const data = buf.length > remaining ? buf.subarray(0, remaining) : buf;
    let off = 0;
    while (off < data.length) {
      const { bytesWritten } = await fh.write(data, off, data.length - off, chunk.pos + off);
      off += bytesWritten;
    }
    chunk.pos += data.length;
    job.received += data.length;
  }
  if (chunk.pos <= chunk.end) throw new Error('Connection closed early');
}

async function runSegmented(job, info, connections, ctrl) {
  const file = partPath(job);

  if (!job.chunks) {
    // if a single-connection run left a partial file, keep what it already got
    let existing = 0;
    if (job.mode !== 'segmented') {
      try {
        existing = fs.statSync(file).size;
      } catch {}
    }
    job.chunks = makeChunks(info.total, Math.min(existing, info.total), connections);
  }
  job.mode = 'segmented';
  job.total = info.total;
  job.connections = connections;
  job.received = job.chunks.reduce((a, c) => a + (c.pos - c.start), 0);
  save();

  const fh = await fs.promises.open(file, fs.existsSync(file) ? 'r+' : 'w+');
  const inner = new AbortController();
  const onAbort = () => inner.abort();
  ctrl.signal.addEventListener('abort', onAbort);

  let lastTick = Date.now();
  let lastBytes = job.received;
  const timer = setInterval(() => {
    const now = Date.now();
    job.speed = ((job.received - lastBytes) * 1000) / (now - lastTick);
    lastTick = now;
    lastBytes = job.received;
    save();
  }, 1000);

  const busy = new Set();
  const worker = async () => {
    try {
      for (;;) {
        if (inner.signal.aborted) return;
        const idx = job.chunks.findIndex((c, i) => c.pos <= c.end && !busy.has(i));
        if (idx === -1) return;
        busy.add(idx);
        try {
          for (let tries = 1; ; tries++) {
            try {
              await fetchChunk(job, job.chunks[idx], info.finalUrl, inner.signal, fh);
              break;
            } catch (e) {
              if (
                inner.signal.aborted ||
                e.fatal ||
                e.code === 'ENOSPC' ||
                e.status === 429 ||
                e.status === 503 ||
                tries >= 3
              ) {
                throw e;
              }
              await sleep(1000 * tries, inner.signal);
            }
          }
        } finally {
          busy.delete(idx);
        }
      }
    } catch (e) {
      inner.abort();
      throw e;
    }
  };

  try {
    await fh.truncate(info.total);
    const pending = job.chunks.filter((c) => c.pos <= c.end).length;
    const results = await Promise.allSettled(
      Array.from({ length: Math.min(connections, pending) }, () => worker())
    );
    const rejected = results.filter((r) => r.status === 'rejected').map((r) => r.reason);
    if (rejected.length) {
      throw rejected.find((e) => e && e.name !== 'AbortError') || rejected[0];
    }
    if (job.chunks.some((c) => c.pos <= c.end)) throw new Error('Download incomplete');
  } finally {
    clearInterval(timer);
    ctrl.signal.removeEventListener('abort', onAbort);
    await fh.close().catch(() => {});
  }
}

async function runSingle(job, ctrl) {
  job.mode = 'single';
  job.chunks = null;

  let start = 0;
  try {
    start = fs.statSync(partPath(job)).size;
  } catch {}

  const headers = { 'User-Agent': UA };
  if (start > 0) headers.Range = `bytes=${start}-`;

  let res = await fetch(job.url, { headers, signal: ctrl.signal, redirect: 'follow' });

  if (res.status === 416) {
    // our partial file doesn't match what the server has, start over
    try {
      fs.unlinkSync(partPath(job));
    } catch {}
    start = 0;
    delete headers.Range;
    res = await fetch(job.url, { headers, signal: ctrl.signal, redirect: 'follow' });
  }
  if (!res.ok) throw new Error(`Server responded ${res.status} ${res.statusText}`);

  if (start > 0 && res.status === 200) start = 0; // server ignored Range, restart file

  let total = null;
  if (res.status === 206) {
    const m = (res.headers.get('content-range') || '').match(/\/(\d+)$/);
    if (m) total = parseInt(m[1], 10);
  } else {
    const len = parseInt(res.headers.get('content-length') || '', 10);
    if (!isNaN(len)) total = len;
  }
  job.total = total;
  job.received = start;
  save();

  const input = Readable.fromWeb(res.body);
  const out = fs.createWriteStream(partPath(job), { flags: start > 0 ? 'a' : 'w' });

  let lastTick = Date.now();
  let lastBytes = job.received;
  input.on('data', (chunk) => {
    job.received += chunk.length;
    const now = Date.now();
    if (now - lastTick >= 1000) {
      job.speed = ((job.received - lastBytes) * 1000) / (now - lastTick);
      lastTick = now;
      lastBytes = job.received;
      save();
    }
  });

  await pipeline(input, out, { signal: ctrl.signal });

  if (job.total && job.received !== job.total) {
    throw new Error(`Incomplete download (${job.received}/${job.total} bytes)`);
  }
}

function fmtBytes(n) {
  const u = ['B', 'KB', 'MB', 'GB', 'TB'];
  let i = 0;
  while (n >= 1024 && i < u.length - 1) {
    n /= 1024;
    i++;
  }
  return n.toFixed(i > 1 ? 1 : 0) + ' ' + u[i];
}

// free / total bytes on the volume (null if the platform can't tell us)
async function diskStats() {
  try {
    const st = await fs.promises.statfs(FILES_DIR);
    return { free: st.bavail * st.bsize, total: st.blocks * st.bsize };
  } catch {
    return null;
  }
}

// Refuse up front if the file can't fit, instead of failing hours in.
async function checkSpace(job, info) {
  if (!info.size) return;
  const st = await diskStats();
  if (!st) return;
  let have = 0;
  if (job.chunks) have = job.received || 0;
  else {
    try {
      have = fs.statSync(partPath(job)).size;
    } catch {}
  }
  const need = Math.max(0, info.size - have);
  if (st.free < need + 64 * 1024 * 1024) {
    const e = new Error(
      `Not enough space on the volume: needs ${fmtBytes(need)} more but only ${fmtBytes(st.free)} is free. ` +
        'Remove old files or enlarge the volume, then hit Resume.'
    );
    e.noRetry = true;
    throw e;
  }
}

async function runJob(job) {
  const ctrl = new AbortController();
  controllers.set(job.id, ctrl);
  job.status = 'downloading';
  job.error = null;
  job.speed = 0;
  const attemptStart = job.received || 0;
  save();

  try {
    const info = await probe(job, ctrl.signal);

    // a previously chunked download no longer matches the server: start clean
    if (job.chunks && (!info.supported || info.total !== job.total)) {
      try {
        fs.unlinkSync(partPath(job));
      } catch {}
      job.chunks = null;
      job.received = 0;
      job.mode = null;
    }

    if (!job.nameResolved) {
      if (job.autoName && info.nameHeader) job.filename = uniqueName(info.nameHeader, job.id);
      job.nameResolved = true;
    }

    await checkSpace(job, info);

    const connections = job.connections || DEFAULT_CONNECTIONS;
    const segmented =
      info.supported && info.total >= MIN_SEGMENT_BYTES && (connections > 1 || job.chunks);
    if (segmented) await runSegmented(job, info, connections, ctrl);
    else await runSingle(job, ctrl);

    fs.renameSync(partPath(job), finalPath(job));
    job.status = 'done';
    job.speed = 0;
    job.finishedAt = Date.now();
    job.total = job.total || job.received;
    job.chunks = null;
    job.retries = 0;
    job.error = null;
  } catch (err) {
    job.speed = 0;
    if (ctrl.signal.aborted) {
      job.status = job.abortTo || 'paused';
    } else {
      if (err.code === 'ENOSPC') {
        err.message = 'The volume is full. Remove old files or enlarge the volume, then hit Resume.';
        err.noRetry = true;
      }
      if (err.noRetry) {
        // retrying can't fix this one
        job.status = 'failed';
        job.error = err.message;
        return;
      }
      // host is rejecting us for too many connections: back off to fewer
      const current = job.connections || DEFAULT_CONNECTIONS;
      if (job.mode === 'segmented' && (err.status === 429 || err.status === 503) && current > 1) {
        job.connections = Math.max(1, Math.floor(current / 2));
      }
      // real progress means the retry budget starts over
      if (job.received > attemptStart + 1024 * 1024) job.retries = 0;
      job.retries = (job.retries || 0) + 1;
      job.error = err.message || String(err);
      if (job.retries <= MAX_RETRIES) {
        // back off and resume automatically
        job.status = 'queued';
        const wait = Math.min(60000, 2000 * 2 ** (job.retries - 1));
        job.retryAt = Date.now() + wait;
        setTimeout(() => {
          job.retryAt = null;
          pump();
        }, wait);
      } else {
        job.status = 'failed';
      }
    }
  } finally {
    controllers.delete(job.id);
    save();
    pump();
  }
}

function stopJob(job, to) {
  job.abortTo = to;
  const c = controllers.get(job.id);
  if (c) c.abort();
  else job.status = to;
}

// ---------- link resolving ----------
// Turns what you paste (a repo, a release page, a share link, a web page that lists
// files) into a direct file URL, or a list of files to pick from.
const GITHUB_API = process.env.GITHUB_API || 'https://api.github.com';
const FILE_EXTS = new Set(
  (
    'zip 7z rar iso img bin cue tar gz tgz bz2 xz zst exe msi dmg pkg deb rpm apk ipa appimage ' +
    'pdf epub mp4 mkv avi mov webm mp3 flac wav ogg m4a chd cso rvz wbfs gcz'
  ).split(' ')
);
const timeout = () => AbortSignal.timeout(20000);

async function ghJson(apiPath) {
  const headers = { 'User-Agent': 'background-downloader', Accept: 'application/vnd.github+json' };
  if (process.env.GITHUB_TOKEN) headers.Authorization = `Bearer ${process.env.GITHUB_TOKEN}`;
  const res = await fetch(GITHUB_API + apiPath, { headers, signal: timeout() });
  if (res.status === 404) throw new Error('GitHub says that repo or release does not exist (or it is private).');
  if (res.status === 403 || res.status === 429) {
    throw new Error('GitHub is rate limiting this server right now. Try again in a bit.');
  }
  if (!res.ok) throw new Error(`GitHub responded ${res.status}`);
  return res.json();
}

async function resolveGithub(parts) {
  const owner = parts[0];
  const repo = parts[1].replace(/\.git$/, '');
  const section = parts[2];
  const archive = (ref) => `https://github.com/${owner}/${repo}/archive/${ref}.zip`;

  if (section === 'blob' && parts.length >= 5) {
    return { kind: 'direct', url: `https://raw.githubusercontent.com/${owner}/${repo}/${parts.slice(3).join('/')}` };
  }
  if (section === 'releases' && parts[3] !== 'download') {
    const tag = parts[3] === 'tag' && parts[4] ? decodeURIComponent(parts[4]) : null;
    const rel = await ghJson(
      `/repos/${owner}/${repo}/releases/${tag ? 'tags/' + encodeURIComponent(tag) : 'latest'}`
    );
    const choices = (rel.assets || []).map((a) => ({
      name: a.name,
      url: a.browser_download_url,
      size: a.size,
    }));
    const source = archive(`refs/tags/${rel.tag_name}`);
    if (choices.length === 0) return { kind: 'direct', url: source };
    if (choices.length === 1) return { kind: 'direct', url: choices[0].url };
    choices.push({ name: `Source code (${rel.tag_name}).zip`, url: source });
    return { kind: 'choices', title: `${repo} ${rel.name || rel.tag_name}`, choices };
  }
  if (section === 'tree' && parts[3]) return { kind: 'direct', url: archive(`refs/heads/${parts[3]}`) };
  if (!section) return { kind: 'direct', url: archive('HEAD') };
  return null; // archive/raw/etc: let the generic handling deal with it
}

// share links that need a tweak to become real downloads
function rewriteKnown(u) {
  const host = u.hostname.replace(/^www\./, '');
  if (host === 'dropbox.com') {
    u.searchParams.delete('dl');
    u.searchParams.set('dl', '1');
    return u.href;
  }
  if (host === 'huggingface.co' && u.pathname.includes('/blob/')) {
    u.pathname = u.pathname.replace('/blob/', '/resolve/');
    return u.href;
  }
  if (host === 'drive.google.com') {
    const m = u.pathname.match(/\/file\/d\/([\w-]+)/);
    const id = m ? m[1] : u.searchParams.get('id');
    if (id) return `https://drive.usercontent.google.com/download?id=${id}&export=download&confirm=t`;
  }
  return null;
}

async function readLimited(res, limit) {
  const reader = res.body.getReader();
  const dec = new TextDecoder();
  let out = '';
  let size = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    size += value.length;
    out += dec.decode(value, { stream: true });
    if (size >= limit) {
      await reader.cancel().catch(() => {});
      break;
    }
  }
  return out;
}

// A plain file link passes straight through. A web page gets scanned for file links.
async function sniff(url) {
  const res = await fetch(url, {
    headers: { 'User-Agent': UA, Range: 'bytes=0-0' },
    redirect: 'follow',
    signal: timeout(),
  });
  const type = (res.headers.get('content-type') || '').toLowerCase();
  const finalUrl = res.url || url;
  try {
    await res.body?.cancel();
  } catch {}
  if (!res.ok) throw new Error(`The server responded ${res.status} ${res.statusText}.`);
  if (!/text\/html|application\/xhtml/.test(type)) return { kind: 'direct', url };

  const page = await fetch(finalUrl, { headers: { 'User-Agent': UA }, redirect: 'follow', signal: timeout() });
  if (!page.ok) throw new Error(`The server responded ${page.status} ${page.statusText}.`);
  const html = await readLimited(page, 2 * 1024 * 1024);

  const seen = new Map();
  for (const m of html.matchAll(/href\s*=\s*["']([^"']+)["']/gi)) {
    let link;
    try {
      link = new URL(m[1].replace(/&amp;/g, '&'), finalUrl);
    } catch {
      continue;
    }
    link.hash = '';
    if (!/^https?:$/.test(link.protocol)) continue;
    const ext = path.extname(link.pathname).slice(1).toLowerCase();
    if (!FILE_EXTS.has(ext) || seen.has(link.href)) continue;
    seen.set(link.href, { name: nameFromUrl(link.href), url: link.href });
    if (seen.size >= 40) break;
  }
  const choices = [...seen.values()];
  if (choices.length === 0) {
    throw new Error(
      "That link is a web page, not a file, and I couldn't find any downloadable files on it. " +
        "Use the file's direct download link instead."
    );
  }
  if (choices.length === 1) return { kind: 'direct', url: choices[0].url };
  return { kind: 'choices', title: 'Pick a file', choices };
}

async function resolveLink(input) {
  const u = new URL(input);
  const host = u.hostname.replace(/^www\./, '');
  const parts = u.pathname.split('/').filter(Boolean);
  if (host === 'github.com' && parts.length >= 2) {
    const r = await resolveGithub(parts);
    if (r) return r;
  }
  const rewritten = rewriteKnown(new URL(input));
  if (rewritten) return { kind: 'direct', url: rewritten };
  return sniff(u.href);
}

// ---------- http ----------
const sha = (s) => crypto.createHash('sha256').update(String(s)).digest();
const safeEq = (a, b) => crypto.timingSafeEqual(sha(a), sha(b));

function checkPassword(pass) {
  return safeEq(pass, PASSWORD);
}

function parseCookies(req) {
  const out = {};
  for (const part of (req.headers.cookie || '').split(';')) {
    const i = part.indexOf('=');
    if (i > 0) out[part.slice(0, i).trim()] = part.slice(i + 1).trim();
  }
  return out;
}

// Logged in = valid session cookie (the login page sets it). HTTP Basic still works too.
function authOk(req) {
  const cookie = parseCookies(req)[COOKIE_NAME];
  if (cookie && safeEq(cookie, SESSION_TOKEN)) return true;
  const h = req.headers.authorization || '';
  if (!h.startsWith('Basic ')) return false;
  const pass = Buffer.from(h.slice(6), 'base64').toString().split(':').slice(1).join(':');
  return checkPassword(pass);
}

// last entry is the one added by Railway's proxy, so it can't be spoofed by the client
function clientIp(req) {
  const xff = (req.headers['x-forwarded-for'] || '').split(',').pop().trim();
  return xff || req.socket.remoteAddress || 'unknown';
}

function json(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(obj));
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (c) => {
      data += c;
      if (data.length > 1e6) req.destroy();
    });
    req.on('end', () => {
      try {
        resolve(data ? JSON.parse(data) : {});
      } catch (e) {
        reject(e);
      }
    });
  });
}

function publicJob(j) {
  const { abortTo, chunks, ...rest } = j;
  return rest;
}

function serveFile(req, res, job) {
  const fp = finalPath(job);
  let stat;
  try {
    stat = fs.statSync(fp);
  } catch {
    res.writeHead(404);
    return res.end('File missing');
  }
  const headers = {
    'Content-Type': 'application/octet-stream',
    'Content-Disposition': `attachment; filename*=UTF-8''${encodeURIComponent(job.filename)}`,
    'Accept-Ranges': 'bytes',
  };
  const range = req.headers.range;
  if (range) {
    const m = range.match(/bytes=(\d*)-(\d*)/);
    let s = m && m[1] ? parseInt(m[1], 10) : 0;
    let e = m && m[2] ? parseInt(m[2], 10) : stat.size - 1;
    if (s >= stat.size || e >= stat.size || s > e) {
      res.writeHead(416, { 'Content-Range': `bytes */${stat.size}` });
      return res.end();
    }
    res.writeHead(206, {
      ...headers,
      'Content-Range': `bytes ${s}-${e}/${stat.size}`,
      'Content-Length': e - s + 1,
    });
    return fs.createReadStream(fp, { start: s, end: e }).pipe(res);
  }
  res.writeHead(200, { ...headers, 'Content-Length': stat.size });
  fs.createReadStream(fp).pipe(res);
}

// Public on purpose: the phone fetches the icon and manifest without cookies when
// adding to the Home Screen. None of it contains anything private.
const PUBLIC_FILES = {
  '/manifest.webmanifest': ['manifest.webmanifest', 'application/manifest+json'],
  '/apple-touch-icon.png': ['apple-touch-icon.png', 'image/png'],
  '/icon-192.png': ['icon-192.png', 'image/png'],
  '/icon-512.png': ['icon-512.png', 'image/png'],
  '/login': ['login.html', 'text/html; charset=utf-8'],
};

function sendPublic(res, name, type, noStore) {
  const fp = path.join(__dirname, 'public', name);
  fs.stat(fp, (err, st) => {
    if (err) {
      res.writeHead(404);
      return res.end('Not found');
    }
    res.writeHead(200, {
      'Content-Type': type,
      'Content-Length': st.size,
      'Cache-Control': noStore ? 'no-store' : 'public, max-age=3600',
    });
    fs.createReadStream(fp).pipe(res);
  });
}

const loginAttempts = new Map(); // ip -> { count, resetAt }
const MAX_LOGIN_ATTEMPTS = 10;
const LOGIN_WINDOW_MS = 15 * 60 * 1000;

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  const p = url.pathname;

  try {
    if (req.method === 'GET' && PUBLIC_FILES[p]) {
      return sendPublic(res, ...PUBLIC_FILES[p]);
    }

    if (req.method === 'POST' && p === '/api/login') {
      const ip = clientIp(req);
      const now = Date.now();
      const rec = loginAttempts.get(ip);
      const live = rec && now < rec.resetAt ? rec : null;
      if (live && live.count >= MAX_LOGIN_ATTEMPTS) {
        return json(res, 429, { error: 'Too many attempts. Try again in a few minutes.' });
      }
      const body = await readBody(req).catch(() => ({}));
      if (checkPassword(String(body.password || ''))) {
        loginAttempts.delete(ip);
        const secure = req.headers['x-forwarded-proto'] === 'https' ? '; Secure' : '';
        res.setHeader(
          'Set-Cookie',
          `${COOKIE_NAME}=${SESSION_TOKEN}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${COOKIE_MAX_AGE}${secure}`
        );
        return json(res, 200, { ok: true });
      }
      const cur = live || { count: 0, resetAt: now + LOGIN_WINDOW_MS };
      cur.count++;
      loginAttempts.set(ip, cur);
      return json(res, 401, { error: 'Wrong password' });
    }

    if (req.method === 'POST' && p === '/api/logout') {
      res.setHeader('Set-Cookie', `${COOKIE_NAME}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0`);
      return json(res, 200, { ok: true });
    }

    if (!authOk(req)) {
      if (req.method === 'GET' && (p === '/' || p === '/index.html')) {
        return sendPublic(res, 'login.html', 'text/html; charset=utf-8', true);
      }
      if (req.method === 'GET' && !p.startsWith('/api/')) {
        res.writeHead(302, { Location: '/' });
        return res.end();
      }
      return json(res, 401, { error: 'Not logged in' });
    }

    if (req.method === 'GET' && (p === '/' || p === '/index.html')) {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
      return fs.createReadStream(path.join(__dirname, 'public', 'index.html')).pipe(res);
    }

    if (req.method === 'GET' && p === '/api/stats') {
      return json(res, 200, (await diskStats()) || {});
    }

    if (req.method === 'GET' && p === '/api/jobs') {
      return json(res, 200, jobs.map(publicJob));
    }

    if (req.method === 'POST' && p === '/api/jobs') {
      const body = await readBody(req);
      let target;
      try {
        target = new URL(String(body.url || '').trim());
        if (!['http:', 'https:'].includes(target.protocol)) throw new Error();
      } catch {
        return json(res, 400, { error: 'Enter a valid http(s) URL' });
      }
      if (!body.direct) {
        let resolved;
        try {
          resolved = await resolveLink(target.href);
        } catch (e) {
          const msg =
            e.name === 'TimeoutError'
              ? 'That link took too long to respond.'
              : e.message === 'fetch failed'
                ? "Couldn't reach that site. Check the link."
                : e.message;
          return json(res, 400, { error: msg || 'Could not read that link' });
        }
        if (resolved.kind === 'choices') {
          return json(res, 200, { title: resolved.title, choices: resolved.choices });
        }
        target = new URL(resolved.url);
      }
      const custom = body.filename ? sanitize(String(body.filename)) : null;
      const connections = Math.min(
        32,
        Math.max(1, parseInt(body.connections, 10) || DEFAULT_CONNECTIONS)
      );
      const job = {
        id: crypto.randomUUID(),
        url: target.href,
        filename: uniqueName(custom || nameFromUrl(target.href)),
        autoName: !custom,
        nameResolved: false,
        status: 'queued',
        mode: null,
        connections,
        received: 0,
        total: null,
        speed: 0,
        retries: 0,
        error: null,
        createdAt: Date.now(),
      };
      jobs.unshift(job);
      save();
      pump();
      return json(res, 201, publicJob(job));
    }

    let m = p.match(/^\/api\/jobs\/([\w-]+)\/(pause|resume)$/);
    if (req.method === 'POST' && m) {
      const job = jobs.find((j) => j.id === m[1]);
      if (!job) return json(res, 404, { error: 'Not found' });
      if (m[2] === 'pause') {
        if (job.status === 'downloading' || job.status === 'queued') stopJob(job, 'paused');
      } else if (['paused', 'failed'].includes(job.status)) {
        job.status = 'queued';
        job.retries = 0;
        job.retryAt = null;
        job.error = null;
      }
      save();
      pump();
      return json(res, 200, publicJob(job));
    }

    m = p.match(/^\/api\/jobs\/([\w-]+)$/);
    if (req.method === 'DELETE' && m) {
      const job = jobs.find((j) => j.id === m[1]);
      if (!job) return json(res, 404, { error: 'Not found' });
      stopJob(job, 'removed');
      jobs = jobs.filter((j) => j.id !== job.id);
      setTimeout(() => {
        for (const f of [partPath(job), finalPath(job)]) fs.unlink(f, () => {});
      }, 300);
      save();
      return json(res, 200, { ok: true });
    }

    m = p.match(/^\/files\/([\w-]+)$/);
    if (req.method === 'GET' && m) {
      const job = jobs.find((j) => j.id === m[1] && j.status === 'done');
      if (!job) {
        res.writeHead(404);
        return res.end('Not found');
      }
      return serveFile(req, res, job);
    }

    res.writeHead(404);
    res.end('Not found');
  } catch (err) {
    console.error(err);
    json(res, 500, { error: 'Server error' });
  }
});

server.listen(PORT, () => {
  console.log(`Downloader listening on :${PORT}, data in ${DATA_DIR}`);
  pump();
});
