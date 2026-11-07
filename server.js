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
  }
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
              if (inner.signal.aborted || e.fatal || e.status === 429 || e.status === 503 || tries >= 3) {
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

// ---------- http ----------
function authOk(req) {
  const h = req.headers.authorization || '';
  if (!h.startsWith('Basic ')) return false;
  const pass = Buffer.from(h.slice(6), 'base64').toString().split(':').slice(1).join(':');
  const a = crypto.createHash('sha256').update(pass).digest();
  const b = crypto.createHash('sha256').update(PASSWORD).digest();
  return crypto.timingSafeEqual(a, b);
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

const server = http.createServer(async (req, res) => {
  if (!authOk(req)) {
    res.writeHead(401, { 'WWW-Authenticate': 'Basic realm="Downloader"' });
    return res.end('Auth required');
  }

  const url = new URL(req.url, 'http://x');
  const p = url.pathname;

  try {
    if (req.method === 'GET' && (p === '/' || p === '/index.html')) {
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      return fs.createReadStream(path.join(__dirname, 'public', 'index.html')).pipe(res);
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
