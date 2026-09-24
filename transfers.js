import fs from 'node:fs'
import path from 'node:path'
import { spawn, execSync } from 'node:child_process'
import pg from 'pg'
import { INGEST_DIR, MISC_DIR } from './config.js'

const DATABASE_URL = process.env.DATABASE_URL || 'postgresql://firestream:SiMoX1234%40%40@204.168.236.110:5432/openvibe'

// ── Config ──────────────────────────────────────────────────────────
const POLL_INTERVAL_MS = 5_000            // Check for new videos every 5s
const MAX_CONCURRENT = 3                  // Max concurrent video transfers
const ONCE = process.argv.includes('--once')  // Exit after first batch (testing)
const PROGRESS_INTERVAL_MS = 3_000        // DB progress update interval
const ARIA2C_SPLITS = 16                  // -s (split count, only for files > min-split-size)
const ARIA2C_CONNS = 16                   // -x (connections per server)
const CONFIRM_TIMEOUT_MS = 30_000         // Timeout for encoder confirm POST

// ── DB Pool ─────────────────────────────────────────────────────────
let pool = null
function getPool() {
  if (!pool) {
    pool = new pg.Pool({
      connectionString: DATABASE_URL,
      max: 5,
      idleTimeoutMillis: 30_000,
    })
  }
  return pool
}

// ── Stats ───────────────────────────────────────────────────────────
let activeTransfers = 0
let totalTransferred = 0
let totalFailed = 0
const startTime = Date.now()

// ── Logger ──────────────────────────────────────────────────────────
function log(msg) {
  const ts = new Date().toISOString()
  console.log(`[${ts}] ${msg}`)
}

function formatBytes(bytes) {
  if (bytes < 1024) return `${bytes}B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)}KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)}MB`
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)}GB`
}

// ── Resolve local storage server ID ─────────────────────────────────
let _storageId = null
async function getStorageId() {
  if (_storageId) return _storageId
  const ip = execSync('hostname -I', { encoding: 'utf8' }).trim().split(/\s+/)[0]
  const { rows } = await getPool().query(
    'SELECT id FROM "Server" WHERE ip = $1',
    [ip]
  )
  if (rows.length === 0) {
    throw new Error(`No Server row found for local IP ${ip}`)
  }
  _storageId = rows[0].id
  log(`Local storage server: ${_storageId} (${ip})`)
  return _storageId
}

// ── Resolve encoder URL ─────────────────────────────────────────────
async function resolveEncoderUrl(encoderId) {
  const { rows } = await getPool().query(
    'SELECT hostname, port FROM "Server" WHERE id = $1',
    [encoderId]
  )
  if (rows.length === 0) {
    throw new Error(`No Server row for encoderId ${encoderId}`)
  }
  let hostname = rows[0].hostname
  if (!hostname) {
    throw new Error(`Encoder ${encoderId} has no hostname`)
  }
  // Strip any existing scheme prefix
  hostname = hostname.replace(/^https?:\/\//, '')
  const scheme = hostname.includes('localhost') || hostname.startsWith('127.') ? 'http' : 'https'
  return `${scheme}://${hostname}`
}

// ── Claim videos atomically ─────────────────────────────────────────
// pending → transferring (avoids duplicate work if multiple processes)
async function claimPendingVideos(limit) {
  const client = await getPool().connect()
  try {
    await client.query('BEGIN')
    // Lock rows to prevent race conditions
    const { rows } = await client.query(
      `SELECT id, "userId", "encoderId", "encodedSize", "isMulti"
       FROM "Video"
       WHERE status = 'active'
         AND "encodingStatus" = 'completed'
         AND "transferStatus" = 'pending'
         AND ("storageId" = $1 OR "storageId" IS NULL)
       ORDER BY "createdAt" ASC
       LIMIT $2
       FOR UPDATE SKIP LOCKED`,
      [await getStorageId(), limit]
    )
    if (rows.length > 0) {
      const ids = rows.map(r => r.id)
      await client.query(
        `UPDATE "Video" SET "transferStatus" = 'transferring', "updatedAt" = NOW()
         WHERE id = ANY($1::text[])`,
        [ids]
      )
    }
    await client.query('COMMIT')
    return rows
  } catch (err) {
    await client.query('ROLLBACK')
    throw err
  } finally {
    client.release()
  }
}

// ── Build asset URL list for a video ────────────────────────────────
async function buildAssetList(encoderUrl, videoId, userId, isMulti) {
  const base = `${encoderUrl}/encodings/_tmp/${videoId}`
  const assets = []

  // Primary video — try both MP4 and HLS playlist (one may not exist)
  assets.push({
    url: `${base}/video.mp4`,
    dir: path.join(INGEST_DIR, userId, videoId),
    out: 'video.mp4',
  })

  // SD companion video (multi-bitrate)
  if (isMulti) {
    assets.push({
      url: `${base}/video_sd.mp4`,
      dir: path.join(INGEST_DIR, userId, videoId),
      out: 'video_sd.mp4',
    })
  }
  assets.push({
    url: `${base}/video.m3u8`,
    dir: path.join(INGEST_DIR, userId, videoId),
    out: 'video.m3u8',
  })

  // Thumbnail
  assets.push({
    url: `${base}/${videoId}-thumb.jpg`,
    dir: path.join(INGEST_DIR, userId, videoId),
    out: `${videoId}-thumb.jpg`,
  })

  // Sprite sheet (sprite images, if any)
  // Encoder writes sprite as: {videoId}-sprite.jpg and potentially numbered sprites
  assets.push({
    url: `${base}/${videoId}-sprite.jpg`,
    dir: path.join(INGEST_DIR, userId, videoId),
    out: `${videoId}-sprite.jpg`,
  })

  // Poster
  assets.push({
    url: `${encoderUrl}/misc/${userId}/posters/${videoId}-poster.jpg`,
    dir: path.join(MISC_DIR, userId, 'posters'),
    out: `${videoId}-poster.jpg`,
  })

  // Mini poster
  assets.push({
    url: `${encoderUrl}/misc/${userId}/posters/${videoId}-mini-poster.png`,
    dir: path.join(MISC_DIR, userId, 'posters'),
    out: `${videoId}-mini-poster.png`,
  })

  // VTT thumbnail sprite metadata
  assets.push({
    url: `${base}/${videoId}.vtt`,
    dir: path.join(INGEST_DIR, userId, videoId),
    out: `${videoId}.vtt`,
  })

  // Subtitle VTT files — query DB for filenames
  try {
    const { rows } = await getPool().query(
      `SELECT filename FROM subtitles WHERE "videoId" = $1 AND status = 'active'`,
      [videoId]
    )
    for (const row of rows) {
      assets.push({
        url: `${encoderUrl}/misc/${userId}/subtitles/${row.filename}`,
        dir: path.join(MISC_DIR, userId, 'subtitles'),
        out: row.filename,
      })
    }
  } catch (err) {
    // Non-fatal — subtitles will be skipped if DB is unreachable
    log(`[${videoId}] WARN failed to query subtitles: ${err.message}`)
  }

  return assets
}

// ── Create aria2c input file ────────────────────────────────────────
function writeAria2cInput(assets, tmpDir) {
  const inputPath = path.join(tmpDir, 'aria2c-input.txt')
  const lines = []
  for (const a of assets) {
    lines.push(a.url)
    lines.push(`  dir=${a.dir}`)
    lines.push(`  out=${a.out}`)
    lines.push('') // blank line separator
  }
  fs.writeFileSync(inputPath, lines.join('\n'))
  return inputPath
}

// ── Download assets via aria2c ──────────────────────────────────────
function aria2cDownload(inputFile, onProgressLine) {
  return new Promise((resolve, reject) => {
    const args = [
      `--input-file=${inputFile}`,
      `--max-connection-per-server=${ARIA2C_CONNS}`,
      `--split=${ARIA2C_SPLITS}`,
      `--min-split-size=1M`,
      '--file-allocation=none',         // Don't preallocate — needed for progress via file size
      '--allow-overwrite=true',
      '--continue=true',                // Resume partial downloads on retry
      '--max-file-not-found=10',        // Tolerate missing assets (poster, sprite, etc.)
      '--retry-wait=10',                // Retry on connection errors
      '--max-tries=5',
      '--connect-timeout=30',
      '--timeout=120',
      '--summary-interval=3',           // Progress output every 3s
      '--console-log-level=notice',
      '--human-readable=false',         // Machine-parseable sizes
      '--show-console-readout=true',
      '--quiet=false',
    ]

    log(`aria2c ${args.join(' ')}`)

    const proc = spawn('aria2c', args, {
      stdio: ['pipe', 'pipe', 'pipe'],
    })

    let stderr = ''

    proc.stderr.on('data', (data) => {
      const text = data.toString()
      stderr += text
      // Parse progress lines from aria2c output
      for (const line of text.split('\n')) {
        const trimmed = line.trim()
        if (trimmed) onProgressLine(trimmed)
      }
    })

    proc.on('close', (code) => {
      if (code === 0) {
        resolve()
      } else if (code === 3) {
        // Exit 3 = some files not found (e.g. video.m3u8 when video is MP4,
        // or missing poster/sprite). Non-fatal — disk check will verify.
        log(`aria2c exit 3 (some files not found — expected, disk check will verify)`)
        resolve()
      } else {
        const lastLines = stderr.split('\n').slice(-5).join(' | ')
        reject(new Error(`aria2c exit ${code}: ${lastLines}`))
      }
    })

    proc.on('error', (err) => {
      reject(new Error(`aria2c spawn failed: ${err.message}`))
    })
  })
}

// ── Parse HLS playlist for referenced .m3u8 variants and .ts segments ─
function parseM3u8Playlist(content) {
  const variants = []
  const segments = []
  for (const line of content.split('\n')) {
    const trimmed = line.trim()
    if (!trimmed || trimmed.startsWith('#') || trimmed.startsWith('http')) continue
    if (trimmed.endsWith('.m3u8')) {
      variants.push(trimmed)
    } else if (trimmed.endsWith('.ts')) {
      segments.push(trimmed)
    }
  }
  return { variants, segments }
}

// ── Recursively resolve all .ts segments from a master HLS playlist ───
async function resolveHlsSegments(encoderUrl, videoId, m3u8Path) {
  let content
  try {
    content = fs.readFileSync(m3u8Path, 'utf8')
  } catch {
    return []
  }

  const baseUrl = `${encoderUrl}/encodings/_tmp/${videoId}`
  const { variants, segments } = parseM3u8Playlist(content)

  // If this is a master playlist with variant playlists, fetch each one
  if (variants.length > 0) {
    log(`  HLS master playlist with ${variants.length} variants`)
    const allSegments = [...segments]

    for (const variantUri of variants) {
      try {
        const resp = await fetch(`${baseUrl}/${variantUri}`)
        if (resp.ok) {
          const variantContent = await resp.text()
          const parsed = parseM3u8Playlist(variantContent)
          allSegments.push(...parsed.segments)
          // Also write the variant playlist to disk
          const variantPath = path.join(path.dirname(m3u8Path), variantUri)
          fs.mkdirSync(path.dirname(variantPath), { recursive: true })
          fs.writeFileSync(variantPath, variantContent)
        }
      } catch (err) {
        log(`  WARN: failed to fetch variant ${variantUri}: ${err.message}`)
      }
    }
    return allSegments
  }

  // Simple playlist — just return the segments
  return segments
}

// ── Calculate total size of files in a directory (for progress) ─────
function getDirFileSizes(dirs) {
  let total = 0
  for (const dir of dirs) {
    try {
      const entries = fs.readdirSync(dir, { withFileTypes: true })
      for (const entry of entries) {
        if (entry.isFile() && !entry.name.endsWith('.tmp') && !entry.name.endsWith('.aria2')) {
          try {
            total += fs.statSync(path.join(dir, entry.name)).size
          } catch { /* file disappeared */ }
        }
      }
    } catch { /* dir doesn't exist yet */ }
  }
  return total
}

// ── Transfer one video ──────────────────────────────────────────────
async function transferVideo(video) {
  const { id: videoId, userId, encoderId, encodedSize, isMulti } = video
  const label = `${videoId}`
  log(`[${label}] Starting transfer (expected: ${encodedSize ? formatBytes(parseInt(encodedSize)) : 'unknown'})${isMulti ? ' [MULTI]' : ''}`)

  // Resolve encoder URL
  let encoderUrl
  try {
    encoderUrl = await resolveEncoderUrl(encoderId)
    log(`[${label}] Encoder: ${encoderUrl}`)
  } catch (err) {
    log(`[${label}] ERROR resolving encoder: ${err.message}`)
    await failTransfer(videoId, `encoder resolve: ${err.message}`)
    return
  }

  // Build the manifest and local directories before starting aria2c. These
  // operations can fail with ENOSPC; handle that per video so one full disk
  // cannot crash the worker and strand every claimed row as "transferring".
  let assets
  let tmpDir
  let inputFile
  try {
    assets = await buildAssetList(encoderUrl, videoId, userId, isMulti)
    tmpDir = path.join(INGEST_DIR, userId, videoId)
    for (const asset of assets) fs.mkdirSync(asset.dir, { recursive: true })
    inputFile = writeAria2cInput(assets, tmpDir)
  } catch (err) {
    log(`[${label}] ERROR preparing transfer: ${err.message}`)
    await failTransfer(videoId, `transfer setup: ${err.message}`)
    return
  }

  // Track progress: poll file sizes every 3s and update DB
  let progressInterval = null
  let lastProgressPct = -1
  let lastBytes = 0

  const startProgressPolling = () => {
    progressInterval = setInterval(() => {
      const dirs = [...new Set(assets.map(a => a.dir))]
      const currentBytes = getDirFileSizes(dirs)
      const expectedBytes = encodedSize ? parseInt(encodedSize) : 0

      // Only update if data actually arrived since last tick
      if (currentBytes === lastBytes) return
      lastBytes = currentBytes

      let pct = 0
      if (expectedBytes > 0) {
        pct = Math.min(99, Math.round((currentBytes / expectedBytes) * 100))
      }

      // Log and update DB whenever bytes change (keeps updatedAt alive)
      if (pct !== lastProgressPct) {
        lastProgressPct = pct
      }
      log(`[${label}] Progress ${formatBytes(currentBytes)}${expectedBytes ? ` / ~${formatBytes(expectedBytes)}` : ''} (${pct}%)`)

      getPool().query(
        'UPDATE "Video" SET "encodingProgress" = $1, "updatedAt" = NOW() WHERE id = $2',
        [pct, videoId]
      ).catch(() => { /* silent */ })
    }, PROGRESS_INTERVAL_MS)
  }

  // Parse aria2c output for additional logging
  const handleProgressLine = (line) => {
    // aria2c progress lines look like:
    // [#1 50MiB/100MiB(50%) CN:8 DL:10MiB ETA:5s]
    // or with human-readable=false:
    // [#1 52428800/104857600(50) CN:8 DL:10485760]
    const match = line.match(/CN:(\d+)\s+DL:(\S+)/)
    if (match) {
      log(`[${label}] aria2c: CN=${match[1]} DL=${match[2]} ${line.match(/ETA:(\S+)/)?.[1] || ''}`)
    }
  }

  // ── Phase 1: Download primary assets ───────────────────────────────
  try {
    startProgressPolling()

    await aria2cDownload(inputFile, handleProgressLine)
    log(`[${label}] Primary assets downloaded`)

    // ── Phase 2: HLS segments (if video.m3u8 exists) ──────────────────
    const m3u8Path = path.join(INGEST_DIR, userId, videoId, 'video.m3u8')
    const segments = await resolveHlsSegments(encoderUrl, videoId, m3u8Path)

    if (segments.length > 0) {
      log(`[${label}] Found ${segments.length} HLS segments, downloading...`)

      const segAssets = segments.map(seg => ({
        url: `${encoderUrl}/encodings/_tmp/${videoId}/${seg}`,
        dir: path.join(INGEST_DIR, userId, videoId),
        out: seg,
      }))

      const segInputFile = writeAria2cInput(segAssets, tmpDir)
      await aria2cDownload(segInputFile, handleProgressLine)
      log(`[${label}] HLS segments downloaded`)
      try { fs.unlinkSync(segInputFile) } catch {}
    }

    // ── Success ───────────────────────────────────────────────────────
    clearInterval(progressInterval)

    // Verify at least the main video file exists on disk
    const mp4Path = path.join(INGEST_DIR, userId, videoId, 'video.mp4')
    const hasMp4 = fs.existsSync(mp4Path)
    const hasM3u8 = fs.existsSync(m3u8Path)

    if (!hasMp4 && !hasM3u8) {
      await failTransfer(videoId, 'No video file downloaded (neither mp4 nor m3u8 found)')
      try { fs.unlinkSync(inputFile) } catch {}
      return
    }

    // ── SD video check (multi-bitrate) ────────────────────────────────
    if (isMulti) {
      const sdPath = path.join(INGEST_DIR, userId, videoId, 'video_sd.mp4')
      if (!fs.existsSync(sdPath)) {
        log(`[${label}] video_sd.mp4 missing for multi-bitrate video — failing`)
        await failTransfer(videoId, 'video_sd.mp4 not found (multi-bitrate required)')
        try { fs.unlinkSync(inputFile) } catch {}
        return
      }
      const sdStat = fs.statSync(sdPath)
      log(`[${label}] video_sd.mp4 verified (${formatBytes(sdStat.size)})`)
    }

    // Final progress = 100
    await getPool().query(
      'UPDATE "Video" SET "transferStatus" = $1, "encodingProgress" = 100, "updatedAt" = NOW() WHERE id = $2',
      ['completed', videoId]
    )

    totalTransferred++
    log(`[${label}] Transfer complete ✓`)

    // ── Confirm cleanup with encoder ──────────────────────────────────
    try {
      const confirmUrl = `${encoderUrl}/transfer/confirm/${videoId}`
      log(`[${label}] Confirming cleanup: POST ${confirmUrl}`)
      const controller = new AbortController()
      const timeout = setTimeout(() => controller.abort(), CONFIRM_TIMEOUT_MS)
      await fetch(confirmUrl, { method: 'POST', signal: controller.signal })
      clearTimeout(timeout)
      log(`[${label}] Encoder confirmed cleanup`)
    } catch (err) {
      // Non-fatal — encoder's cleanup cron will handle it eventually
      log(`[${label}] WARN confirm cleanup failed: ${err.message}`)
    }

    // Clean up aria2c input files
    try { fs.unlinkSync(inputFile) } catch {}
  } catch (err) {
    // ── Failure ───────────────────────────────────────────────────────
    clearInterval(progressInterval)
    log(`[${label}] ERROR: ${err.message}`)
    await failTransfer(videoId, err.message)
  }
}

// ── Mark transfer as failed ─────────────────────────────────────────
async function failTransfer(videoId, reason) {
  totalFailed++
  try {
    await getPool().query(
      `UPDATE "Video"
       SET "transferStatus" = 'error',
           "transferRetries" = COALESCE("transferRetries", 0) + 1,
           "reason" = $2,
           "updatedAt" = NOW()
       WHERE id = $1`,
      [videoId, reason.slice(0, 500)] // Truncate to avoid column overflow
    )
  } catch (err) {
    log(`[${videoId}] DB error on fail: ${err.message}`)
  }
}

// ── Main poll loop ──────────────────────────────────────────────────
async function poll() {
  let claimed = 0
  try {
    const available = MAX_CONCURRENT - activeTransfers
    if (available <= 0) return

    const videos = await claimPendingVideos(available)
    claimed = videos.length

    for (const video of videos) {
      activeTransfers++
      transferVideo(video).finally(() => {
        activeTransfers--
      })
    }
  } catch (err) {
    log(`Poll error: ${err.message}`)
  }
  return claimed
}

// ── Graceful shutdown ───────────────────────────────────────────────
let shuttingDown = false

function shutdown(signal) {
  if (shuttingDown) return
  shuttingDown = true
  log(`${signal} received — shutting down (active transfers: ${activeTransfers})`)

  // Wait for active transfers to drain (max 5 min)
  const start = Date.now()
  const check = setInterval(() => {
    if (activeTransfers === 0 || Date.now() - start > 300_000) {
      clearInterval(check)
      log(`Shutdown complete — transferred: ${totalTransferred}, failed: ${totalFailed}`)
      process.exit(0)
    }
  }, 1000)
}

process.on('SIGTERM', () => shutdown('SIGTERM'))
process.on('SIGINT', () => shutdown('SIGINT'))

// ── Startup ─────────────────────────────────────────────────────────
async function main() {
  log('transfers.js starting — pull-based aria2c transfer agent')
  log(`INGEST_DIR: ${INGEST_DIR}`)
  log(`MISC_DIR: ${MISC_DIR}`)
  log(`MAX_CONCURRENT: ${MAX_CONCURRENT}`)

  await getStorageId()

  log('Entering poll loop')

  // Poll loop
  while (!shuttingDown) {
    const claimed = await poll()
    if (ONCE && claimed === 0 && activeTransfers === 0) {
      log('--once: no more pending videos, exiting')
      break
    }
    if (ONCE && claimed > 0) {
      // Wait for active transfers to finish, then exit
      while (activeTransfers > 0) {
        await new Promise(resolve => setTimeout(resolve, 1000))
      }
      log('--once: transfer batch complete, exiting')
      break
    }
    await new Promise(resolve => setTimeout(resolve, POLL_INTERVAL_MS))
  }

  // If --once, exit after loop breaks
  if (ONCE) {
    log(`Done — transferred: ${totalTransferred}, failed: ${totalFailed}`)
    process.exit(0)
  }
}

main().catch(err => {
  log(`FATAL: ${err.message}`)
  process.exit(1)
})
