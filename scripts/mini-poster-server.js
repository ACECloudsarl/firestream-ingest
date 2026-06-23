import http from 'node:http'
import fs from 'node:fs'
import path from 'node:path'
import { spawn } from 'node:child_process'
import { INGEST_DIR, MISC_DIR } from '../config.js'

const PORT = parseInt(process.env.MINI_POSTER_PORT || '3000', 10)
const MINI_POSTER_PERCENTS = [5, 10, 20, 30, 40, 50, 60, 70, 80, 90]

function log(msg) {
  const ts = new Date().toISOString()
  console.log(`[${ts}] ${msg}`)
}

// ─── Helpers ───────────────────────────────────────────────────────────────

function spawnAsync(cmd, args) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, { stdio: ['ignore', 'pipe', 'pipe'] })
    let stderr = ''
    child.stderr.on('data', d => { stderr += d.toString() })
    child.on('close', code => {
      if (code === 0) resolve()
      else reject(new Error(`${cmd} exited ${code}: ${stderr.slice(-300).trim()}`))
    })
    child.on('error', reject)
  })
}

function getDuration(inputPath) {
  return new Promise((resolve) => {
    const child = spawn('ffprobe', [
      '-v', 'quiet', '-print_format', 'json', '-show_format', inputPath
    ], { stdio: ['ignore', 'pipe', 'ignore'] })
    let stdout = ''
    child.stdout.on('data', d => { stdout += d.toString() })
    child.on('close', code => {
      if (code !== 0) return resolve(0)
      try { resolve(parseFloat(JSON.parse(stdout).format?.duration) || 0) }
      catch { resolve(0) }
    })
    child.on('error', () => resolve(0))
  })
}

async function extractFrame(inputPath, outputPath, percent, durationSec) {
  const duration = durationSec || await getDuration(inputPath)
  if (duration <= 0) throw new Error('Could not determine video duration')

  const seekSec = duration * (percent / 100)
  await fs.promises.mkdir(path.dirname(outputPath), { recursive: true })

  await spawnAsync('ffmpeg', [
    '-y', '-ss', seekSec.toString(),
    '-i', inputPath,
    '-vf', 'scale=300:300:force_original_aspect_ratio=increase,crop=300:300',
    '-frames:v', '1',
    outputPath
  ])
}

async function findSourceFile(videoDir) {
  let entries
  try { entries = await fs.promises.readdir(videoDir, { withFileTypes: true }) }
  catch { return null }

  for (const e of entries) {
    if (e.isFile() && e.name.toLowerCase().endsWith('.mp4'))
      return path.join(videoDir, e.name)
  }
  for (const e of entries) {
    if (e.isFile() && e.name.toLowerCase().endsWith('.m3u8'))
      return path.join(videoDir, e.name)
  }
  return null
}

// ─── Request handler ───────────────────────────────────────────────────────

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
}

function json(res, status, body) {
  res.writeHead(status, { ...CORS_HEADERS, 'Content-Type': 'application/json' })
  res.end(JSON.stringify(body))
}

async function handleRequest(req, res) {
  // CORS preflight
  if (req.method === 'OPTIONS') {
    res.writeHead(204, CORS_HEADERS)
    return res.end()
  }

  // Only GET /generate_miniPoster/{videoId}?userId=xxx
  if (req.method !== 'GET') {
    return json(res, 405, { error: 'method not allowed' })
  }

  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`)
  const parts = url.pathname.split('/').filter(Boolean)

  if (parts.length < 2 || parts[0] !== 'generate_miniPoster') {
    return json(res, 400, { error: 'invalid path' })
  }

  const videoId = parts[1]
  const userId = url.searchParams.get('userId')

  if (!videoId || !userId) {
    return json(res, 400, { error: 'videoId and userId are required' })
  }

  if (videoId.includes('..') || userId.includes('..') ||
      videoId.includes('/') || userId.includes('/')) {
    return json(res, 400, { error: 'invalid videoId or userId' })
  }

  const posterDir = path.join(MISC_DIR, userId, 'posters')
  const filenames = [`${videoId}-mini-poster.png`]
  for (let i = 1; i < 10; i++) filenames.push(`${videoId}-mini-poster-${i}.png`)

  // Return early if all posters already exist
  let allCached = true
  for (const f of filenames) {
    try { await fs.promises.stat(path.join(posterDir, f)) }
    catch { allCached = false; break }
  }
  if (allCached) {
    log(`miniPoster ${videoId}: cached`)
    return json(res, 200, { videoId, posters: filenames, cached: true })
  }

  // Locate source video
  const videoDir = path.join(INGEST_DIR, userId, videoId)
  const sourcePath = await findSourceFile(videoDir)
  if (!sourcePath) {
    log(`miniPoster ${videoId}: no source video in ${videoDir}`)
    return json(res, 404, { error: 'source video not found' })
  }

  log(`miniPoster ${videoId}: generating from ${path.relative(INGEST_DIR, sourcePath)}`)

  try {
    const duration = await getDuration(sourcePath)
    if (duration <= 0) {
      return json(res, 500, { error: 'Could not determine video duration' })
    }

    await fs.promises.mkdir(posterDir, { recursive: true })

    const tasks = MINI_POSTER_PERCENTS.map((pct, i) =>
      extractFrame(sourcePath, path.join(posterDir, filenames[i]), pct, duration)
    )
    await Promise.all(tasks)

    log(`miniPoster ${videoId}: generated ${filenames.length} posters`)
    return json(res, 200, { videoId, posters: filenames, cached: false })
  } catch (e) {
    log(`miniPoster ${videoId}: ERROR ${e.message}`)
    return json(res, 500, { error: `Mini poster generation failed: ${e.message}` })
  }
}

// ─── Server ────────────────────────────────────────────────────────────────

const server = http.createServer((req, res) => {
  handleRequest(req, res).catch(err => {
    log(`UNHANDLED ${err.message}`)
    json(res, 500, { error: 'internal server error' })
  })
})

server.listen(PORT, () => {
  log(`mini-poster-server listening on :${PORT}`)
  log(`ingest dir:  ${INGEST_DIR}`)
  log(`misc dir:    ${MISC_DIR}`)
})
