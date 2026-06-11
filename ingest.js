import http from 'node:http'
import fs from 'node:fs'
import path from 'node:path'
import { INGEST_DIR, PORT, isAllowed } from './config.js'

// Allowed file extensions for ingest
const ALLOWED_EXTENSIONS = new Set(['.ts', '.m3u8', '.jpg', '.png', '.vtt'])

// Request timeout (120s)
const REQUEST_TIMEOUT_MS = 120_000

// Stats
let totalIngested = 0
let activeRequests = 0
const startTime = Date.now()

// Track in-flight requests for graceful shutdown
const inflightRequests = new Set()

function log(msg) {
  const ts = new Date().toISOString()
  console.log(`[${ts}] ${msg}`)
}

// Validate and resolve the PUT path
function resolvePath(url) {
  // Decode URL, strip query string
  const decoded = decodeURIComponent(url.split('?')[0])

  // Normalize — resolves /../ etc.
  const normalized = path.normalize(decoded)

  // Reject any path that tries to escape (has .. segments after normalize)
  if (normalized.includes('..')) {
    return null
  }

  // Must start with / and have at least /{userId}/{videoId}/{filename}
  const parts = normalized.split('/').filter(Boolean)
  if (parts.length < 3) {
    return null
  }

  const filename = parts[parts.length - 1]
  const ext = path.extname(filename).toLowerCase()
  if (!ALLOWED_EXTENSIONS.has(ext)) {
    return null
  }

  // Build absolute path
  const filePath = path.join(INGEST_DIR, normalized)

  // Final safety — must be under INGEST_DIR
  if (!filePath.startsWith(INGEST_DIR)) {
    return null
  }

  return filePath
}

// Atomic write: write to .tmp, then rename
function atomicWrite(filePath, req, res) {
  const tmpPath = filePath + '.tmp'
  const dir = path.dirname(filePath)

  // Ensure directory exists
  fs.mkdir(dir, { recursive: true }, (err) => {
    if (err) {
      log(`ERROR mkdir ${dir}: ${err.message}`)
      res.writeHead(500)
      return res.end()
    }

    const ws = fs.createWriteStream(tmpPath)
    let bytes = 0

    req.on('data', (chunk) => {
      bytes += chunk.length
    })

    req.pipe(ws)

    ws.on('finish', () => {
      fs.rename(tmpPath, filePath, (renameErr) => {
        if (renameErr) {
          log(`ERROR rename ${tmpPath} → ${filePath}: ${renameErr.message}`)
          // Cleanup tmp on failed rename
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

    ws.on('error', (writeErr) => {
      log(`ERROR write ${tmpPath}: ${writeErr.message}`)
      // Cleanup tmp on write error
      fs.unlink(tmpPath, () => {})
      res.writeHead(500)
      res.end()
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
  inflightRequests.add(req)

  // Request timeout
  const timer = setTimeout(() => {
    log(`TIMEOUT PUT ${req.url} (${REQUEST_TIMEOUT_MS}ms)`)
    req.destroy()
  }, REQUEST_TIMEOUT_MS)

  // Cleanup on request close
  const cleanup = () => {
    clearTimeout(timer)
    activeRequests--
    inflightRequests.delete(req)
  }

  req.on('close', cleanup)
  req.on('error', cleanup)

  atomicWrite(filePath, req, res)
})

// Graceful shutdown
let shuttingDown = false

function shutdown(signal) {
  if (shuttingDown) return
  shuttingDown = true
  log(`${signal} received — shutting down (active: ${activeRequests})`)

  // Stop accepting new connections
  server.close(() => {
    log('Server closed')
    process.exit(0)
  })

  // Hard cutoff after 15s
  setTimeout(() => {
    log('Forced shutdown after timeout')
    process.exit(1)
  }, 15_000)
}

process.on('SIGTERM', () => shutdown('SIGTERM'))
process.on('SIGINT', () => shutdown('SIGINT'))

server.listen(PORT, () => {
  log(`ingest listening on :${PORT}`)
  log(`ingest dir: ${INGEST_DIR}`)
})
