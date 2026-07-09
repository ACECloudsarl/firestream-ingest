# FireStream Ingest

HLS ingest server for the FireStream encoding pipeline. Receives `.ts` segments and `.m3u8` playlists from encoder servers via HTTP PUT with atomic writes.

## Setup

```bash
cp .env.example .env
# Edit .env if needed

./updateservers.sh    # populate allowed_ips.json from DB
pm2 start ecosystem.config.js
```

## Additional services

| Service | Port | Description |
|---------|------|-------------|
| `mini-poster-server.js` | 3000 | Auto-generate 10 thumbnail frames from source video |
| `poster-upload-server.js` | 3001 | Custom poster upload + grid-poster regeneration |
| `subtitle-server.js` | 3002 | Subtitle upload (.vtt/.srt) with SRT→VTT conversion |

### Subtitle upload

```
POST /upload_subtitle/{videoId}
  FormData: file (.vtt or .srt), userId (or key for API key auth)
  Returns: { "filename": "{uuid}.vtt" }
```

Subtitles are served from `misc/{userId}/subtitles/` with `Content-Type: text/vtt` and CORS headers for `<video>` text tracks.

## How it works

Encoder servers push HLS output via ffmpeg's DAV protocol:

```
PUT /{userId}/{videoId}/video_000.ts
PUT /{userId}/{videoId}/video.m3u8
PUT /{userId}/{videoId}/master.m3u8
```

Files are written atomically (`.tmp` → rename) to `INGEST_DIR/{userId}/{videoId}/`.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `PUT` | `/{userId}/{videoId}/{filename}` | Ingest an HLS segment/playlist |
| `GET` | `/health` | Health check (bypasses IP whitelist) |

## IP Whitelist

Only encoder servers listed in `allowed_ips.json` can PUT files. Update the whitelist:

```bash
./updateservers.sh    # queries Server table, writes allowed_ips.json, SIGHUP ingest
```

## Accepted extensions

`.ts`, `.m3u8`, `.jpg`, `.png`, `.vtt`
