# Background Downloader

Paste a link, the server downloads it in the background, and you grab the file whenever you want. Close the tab, lock your phone, it keeps going. Downloads resume automatically after network errors or server restarts.

## Deploy on Railway

1. Push this folder to a GitHub repo.
2. Railway: **New Project → Deploy from GitHub repo** and pick it.
3. In the service, **Settings → Volumes → Add Volume**, mount path `/data`.
4. **Variables**:
   - `PASSWORD` = something long (required, the app refuses to start without it)
   - `DATA_DIR` = `/data`
   - `MAX_CONCURRENT` = `2` (optional, files downloading at once)
   - `CONNECTIONS` = `8` (optional, connections per file; can also be set per download in the form)
5. **Settings → Networking → Generate Domain**, then open the URL. Your browser will ask for a login: any username, your `PASSWORD` as the password.

## Notes

- Make sure your volume is bigger than the files you download (ISOs can be several GB).
- Delete files from the app once you've saved them to free up space.
- Speed: if the host supports it, each file is split into chunks and downloaded over several connections at once (shown as e.g. `8x` on the card). Hosts without range support automatically use one connection. If a host rejects you for too many connections, the app backs off to fewer; you can also set Connections to 1.
- Some file hosts block datacenter IPs or need a cookie or click-through page. If a link returns an error, it needs a direct file URL.

## Run locally

```
PASSWORD=test node server.js
```
