import http from 'node:http'
import fs from 'node:fs'
import path from 'node:path'
import { INGEST_DIR, PORT, isAllowed } from './config.js'

// Allowed file extensions for ingest
const ALLOWED_EXTENSIONS = new Set(['.ts', '.m3u8', '.jpg', '.png', '.vtt'])

// Request timeout (120s) — only triggers on stalled transfers
const REQUEST_TIMEOUT_MS = 120_000

// Stale .tmp cleanup interval (60s)
const TMP_CLEANUP_INTERVAL_MS = 60_000
const TMP_MAX_AGE_MS = 300_000 // 5 minutes

// Stats
let totalIngested = 0
let activeRequests = 0
const startTime = Date.now()

function log(msg) {
  const ts = new Date().toISOString()
  console.log(`[${ts}] ${msg}`)
}

// Validate and resolve the PUT path
function resolvePath(url) {
  const decoded = decodeURIComponent(url.split('?')[0])
  const normalized = path.normalize(decoded)

  if (normalized.includes('..')) return null

  const parts = normalized.split('/').filter(Boolean)
  if (parts.length < 3) return null

  const filename = parts[parts.length - 1]
  const ext = path.extname(filename).toLowerCase()
  if (!ALLOWED_EXTENSIONS.has(ext)) return null

  const filePath = path.join(INGEST_DIR, normalized)
  if (!filePath.startsWith(INGEST_DIR)) return null

  return filePath
}

// Atomic write: write to .tmp, then rename on completion
function atomicWrite(filePath, req, res) {
  const tmpPath = filePath + '.tmp'
  const dir = path.dirname(filePath)

  fs.mkdir(dir, { recursive: true }, (err) => {
    if (err) {
      log(`ERROR mkdir ${dir}: ${err.message}`)
      res.writeHead(500)
      return res.end()
    }

    const ws = fs.createWriteStream(tmpPath)
    let bytes = 0
    let finished = false

    // Track bytes from request
    req.on('data', (chunk) => { bytes += chunk.length })

    // Pipe request body to file
    req.pipe(ws)

    // Write stream finished — all data flushed to disk
    ws.on('finish', () => {
      finished = true
      fs.rename(tmpPath, filePath, (renameErr) => {
        if (renameErr) {
          log(`ERROR rename ${tmpPath} → ${filePath}: ${renameErr.message}`)
          fs.unlink(tmpPath, () => {})
          res.writeHead(500)
          return res.end()
        }

        totalIngested++
        const relPath = path.relative(INGEST_DIR, filePath)
        log(`PUT /${relPath} 201 (${formatBytes(bytes)})`)
        res.writeHead(201)
        res.end()
      })
    })

    // Write stream error — cleanup tmp
    ws.on('error', (writeErr) => {
      if (finished) return
      finished = true
      log(`ERROR write ${tmpPath}: ${writeErr.message}`)
      fs.unlink(tmpPath, () => {})
      res.writeHead(500)
      res.end()
    })

    // Request connection dropped before write stream finished
    req.on('close', () => {
      if (finished) return
      finished = true
      // Destroy the write stream to stop flushing
      ws.destroy()
      log(`ABORT partial write: ${tmpPath} (${formatBytes(bytes)})`)
      fs.unlink(tmpPath, () => {})
    })
  })
}

function formatBytes(bytes) {
  if (bytes < 1024) return `${bytes}B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)}KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)}MB`
}

function handleHealth(req, res) {
  const uptime = Math.floor((Date.now() - startTime) / 1000)
  const body = JSON.stringify({
    status: 'ok',
    uptime,
    totalIngested,
    activeRequests,
    ingestDir: INGEST_DIR
  })
  res.writeHead(200, { 'Content-Type': 'application/json' })
  res.end(body)
}

// Periodic cleanup of stale .tmp files
function cleanupTmpFiles() {
  const now = Date.now()
  let cleaned = 0

  function scanDir(dir) {
    let entries
    try { entries = fs.readdirSync(dir, { withFileTypes: true }) } catch { return }
    for (const entry of entries) {
      const full = path.join(dir, entry.name)
      if (entry.isDirectory()) {
        scanDir(full)
      } else if (entry.name.endsWith('.tmp')) {
        try {
          const stat = fs.statSync(full)
          if (now - stat.mtimeMs > TMP_MAX_AGE_MS) {
            fs.unlinkSync(full)
            cleaned++
          }
        } catch {}
      }
    }
  }

  scanDir(INGEST_DIR)
  if (cleaned > 0) {
    log(`cleanup: removed ${cleaned} stale .tmp files`)
  }
}

const server = http.createServer((req, res) => {
  const ip = req.socket.remoteAddress?.replace('::ffff:', '')

  // Health check — bypasses IP whitelist (for local monitoring/PM2)
  if (req.method === 'GET' && req.url === '/health') {
    return handleHealth(req, res)
  }

  // IP whitelist
  if (!isAllowed(ip)) {
    log(`BLOCKED ${ip} ${req.method} ${req.url}`)
    res.writeHead(403)
    return res.end()
  }

  // Only PUT for ingest
  if (req.method !== 'PUT') {
    res.writeHead(405)
    return res.end()
  }

  // Resolve and validate path
  const filePath = resolvePath(req.url)
  if (!filePath) {
    log(`REJECT ${ip} PUT ${req.url} (invalid path)`)
    res.writeHead(400)
    return res.end()
  }

  // Track for graceful shutdown
  activeRequests++

  // Request timeout — stall detection
  const timer = setTimeout(() => {
    log(`TIMEOUT PUT ${req.url} (${REQUEST_TIMEOUT_MS}ms)`)
    req.destroy()
  }, REQUEST_TIMEOUT_MS)

  req.on('close', () => {
    clearTimeout(timer)
    activeRequests--
  })

  atomicWrite(filePath, req, res)
})

// Graceful shutdown
let shuttingDown = false

function shutdown(signal) {
  if (shuttingDown) return
  shuttingDown = true
  log(`${signal} received — shutting down (active: ${activeRequests})`)

  server.close(() => {
    log('Server closed')
    process.exit(0)
  })

  setTimeout(() => {
    log('Forced shutdown after timeout')
    process.exit(1)
  }, 15_000)
}

process.on('SIGTERM', () => shutdown('SIGTERM'))
process.on('SIGINT', () => shutdown('SIGINT'))

// Start stale .tmp cleanup
setInterval(cleanupTmpFiles, TMP_CLEANUP_INTERVAL_MS)

server.listen(PORT, () => {
  log(`ingest listening on :${PORT}`)
  log(`ingest dir: ${INGEST_DIR}`)
  // Run cleanup on startup
  cleanupTmpFiles()
})
