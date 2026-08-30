import { createHash } from 'node:crypto'
import { cp, lstat, mkdir, opendir, realpath, rm, stat, statfs } from 'node:fs/promises'
import path from 'node:path'
import { execFileSync } from 'node:child_process'
import pg from 'pg'
import { INGEST_DIR } from '../config.js'

const DATABASE_URL = process.env.DATABASE_URL
const STAGE_ROOT = process.env.COLD_STAGE_ROOT || '/cold-stage'
const STAGE_CAP_BYTES = 1_500_000_000_000
const MIN_ROOT_AVAILABLE_BYTES = 400_000_000_000
const COPY_CONCURRENCY = 2
const SCAN_INTERVAL_MS = 5_000
const RETRY_MINUTES = 30
const SAFE_COMPONENT = /^[A-Za-z0-9][A-Za-z0-9._-]*$/

let shuttingDown = false

function log(message) {
  console.log(`[${new Date().toISOString()}] [cold-stage] ${message}`)
}

function formatBytes(bytes) {
  const value = Number(bytes)
  if (!Number.isFinite(value) || value <= 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  const index = Math.min(Math.floor(Math.log(value) / Math.log(1000)), units.length - 1)
  return `${(value / (1000 ** index)).toFixed(index > 2 ? 2 : 1)} ${units[index]}`
}

function formatRate(bytesPerSecond) {
  return `${formatBytes(bytesPerSecond)}/s`
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

function safeComponent(value, label) {
  const text = String(value || '')
  if (!SAFE_COMPONENT.test(text)) throw new Error(`${label} contains unsafe characters`)
  return text
}

function parseArgs(argv) {
  const options = { execute: false, once: false, limit: null }
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--execute') options.execute = true
    else if (argv[i] === '--once') options.once = true
    else if (argv[i] === '--limit') {
      const value = Number.parseInt(argv[++i], 10)
      if (!Number.isSafeInteger(value) || value <= 0) throw new Error('--limit must be a positive integer')
      options.limit = value
    } else throw new Error(`unknown option: ${argv[i]}`)
  }
  return options
}

async function inspectDirectory(root) {
  const overall = createHash('sha256')
  let totalBytes = 0
  let files = 0

  async function walk(directory, relativeDirectory = '') {
    const entries = []
    const handle = await opendir(directory)
    for await (const entry of handle) entries.push(entry)
    entries.sort((a, b) => a.name.localeCompare(b.name))
    if (entries.length === 0) overall.update(`D\0${relativeDirectory}\n`)
    for (const entry of entries) {
      const fullPath = path.join(directory, entry.name)
      const relativePath = path.posix.join(relativeDirectory, entry.name)
      if (entry.isSymbolicLink()) throw new Error(`symlink rejected: ${relativePath}`)
      if (entry.isDirectory()) {
        await walk(fullPath, relativePath)
        continue
      }
      if (!entry.isFile()) throw new Error(`non-regular file rejected: ${relativePath}`)
      const info = await stat(fullPath)
      overall.update(`F\0${relativePath}\0${info.size}\n`)
      totalBytes += info.size
      files++
    }
  }

  await walk(root)
  if (files === 0) throw new Error('video directory is empty')
  const checksum = `size-sha256:${overall.digest('hex')}`
  return { totalBytes, files, checksum }
}

async function localStorageId(client) {
  const ip = execFileSync('hostname', ['-I'], { encoding: 'utf8' }).trim().split(/\s+/)[0]
  const { rows } = await client.query(`SELECT id FROM "Server" WHERE ip = $1 AND type = 'storage'`, [ip])
  if (rows.length !== 1) throw new Error(`local IP ${ip} does not identify exactly one storage server`)
  return rows[0].id
}

async function inactivityDays(client) {
  const { rows } = await client.query(
    `SELECT min("inactivityDays")::int AS days FROM storage_boxes WHERE "isActive"`
  )
  if (!rows[0]?.days) throw new Error('no active Storage Box inactivity policy is configured')
  return rows[0].days
}

async function stageUsage(client, storageId) {
  const { rows } = await client.query(
    `SELECT COALESCE(sum(bytes), 0)::text AS bytes
     FROM cold_storage_staging
     WHERE "storageId" = $1 AND status IN ('copying', 'ready', 'uploading', 'restoring')`,
    [storageId]
  )
  return Number(rows[0].bytes)
}

async function fetchCandidates(client, storageId, days, limit) {
  const { rows } = await client.query(
    `SELECT v.id, v."userId", v."encodedPath", v."encodedSize", v."lastView", v."createdAt"
     FROM "Video" v
     WHERE v."storageId" = $1
       AND v.status = 'active'
       AND v."encodingStatus" = 'completed'
       AND v."transferStatus" = 'completed'
       AND NOT v."coldStorage"
       AND v."coldStorageStatus" IN ('local', 'failed')
       AND COALESCE(v."lastView", v."createdAt") <= NOW() - make_interval(days => $2)
       AND v."clonedFrom" IS NULL
       AND NOT v."hasCopies"
       AND NOT EXISTS (SELECT 1 FROM "Video" child WHERE child."clonedFrom" = v.id)
       AND NOT EXISTS (
         SELECT 1 FROM "Video" shared
         WHERE shared.id <> v.id AND v."encodedPath" <> '' AND shared."encodedPath" = v."encodedPath"
       )
       AND NOT EXISTS (SELECT 1 FROM cold_storage_staging s WHERE s."videoId" = v.id)
     ORDER BY NULLIF(v."encodedSize", 0) DESC NULLS LAST, v.id
     LIMIT $3`,
    [storageId, days, limit]
  )
  return rows
}

async function resolveSource(video) {
  const root = await realpath(INGEST_DIR)
  const relative = video.encodedPath?.trim()
    ? path.dirname(video.encodedPath)
    : path.join(video.userId, video.id)
  const expected = path.resolve(root, relative)
  if (expected === root || !expected.startsWith(`${root}${path.sep}`)) throw new Error('source path escapes INGEST_DIR')
  const actual = await realpath(expected)
  if (actual !== expected || !actual.startsWith(`${root}${path.sep}`)) throw new Error('source is a symlink or escapes INGEST_DIR')
  const info = await lstat(actual)
  if (!info.isDirectory() || info.isSymbolicLink()) throw new Error('source is not a safe directory')
  return actual
}

async function claimCandidate(client, storageId, days, video, sourcePath, stagePath) {
  await client.query('BEGIN')
  try {
    const { rows } = await client.query(
      `SELECT v.id
       FROM "Video" v
       WHERE v.id = $3
         AND v."storageId" = $1
         AND v.status = 'active'
         AND v."encodingStatus" = 'completed'
         AND v."transferStatus" = 'completed'
         AND NOT v."coldStorage"
         AND v."coldStorageStatus" IN ('local', 'failed')
         AND COALESCE(v."lastView", v."createdAt") <= NOW() - make_interval(days => $2)
         AND v."clonedFrom" IS NULL AND NOT v."hasCopies"
         AND NOT EXISTS (SELECT 1 FROM "Video" child WHERE child."clonedFrom" = v.id)
         AND NOT EXISTS (
           SELECT 1 FROM "Video" shared
           WHERE shared.id <> v.id AND v."encodedPath" <> '' AND shared."encodedPath" = v."encodedPath"
         )
       FOR UPDATE`,
      [storageId, days, video.id]
    )
    if (rows.length !== 1) {
      await client.query('ROLLBACK')
      return false
    }
    const inserted = await client.query(
      `INSERT INTO cold_storage_staging
         ("videoId", "storageId", "sourcePath", "stagePath", bytes, status, "updatedAt")
       VALUES ($1, $2, $3, $4, $5, 'copying', NOW())
       ON CONFLICT ("videoId") DO NOTHING
       RETURNING "videoId"`,
      [video.id, storageId, sourcePath, stagePath, Number(video.encodedSize || 0)]
    )
    await client.query('COMMIT')
    return inserted.rowCount === 1
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {})
    throw error
  }
}

async function finalizeStage(client, storageId, days, video, sourcePath, stagePath, inspection) {
  await client.query('BEGIN')
  try {
    const { rows } = await client.query(
      `SELECT v.* FROM "Video" v WHERE v.id = $1 FOR UPDATE`,
      [video.id]
    )
    const current = rows[0]
    const activityAt = new Date(current?.lastView || current?.createdAt).getTime()
    const stillEligible = current
      && current.storageId === storageId
      && current.status === 'active'
      && current.encodingStatus === 'completed'
      && current.transferStatus === 'completed'
      && !current.coldStorage
      && ['local', 'failed'].includes(current.coldStorageStatus)
      && !current.clonedFrom
      && !current.hasCopies
      && Number.isFinite(activityAt)
      && activityAt <= Date.now() - days * 86_400_000
    if (!stillEligible) {
      await client.query(`DELETE FROM cold_storage_staging WHERE "videoId" = $1`, [video.id])
      await client.query('COMMIT')
      return false
    }
    const conflicts = await client.query(
      `SELECT EXISTS (
         SELECT 1 FROM "Video" other
         WHERE other.id <> $1 AND (
           other."clonedFrom" = $1 OR ($2::text <> '' AND other."encodedPath" = $2)
         )
       ) AS conflict`,
      [video.id, current.encodedPath || '']
    )
    if (conflicts.rows[0].conflict) {
      await client.query(`DELETE FROM cold_storage_staging WHERE "videoId" = $1`, [video.id])
      await client.query('COMMIT')
      return false
    }
    await client.query(
      `UPDATE cold_storage_staging
       SET bytes = $2, files = $3, checksum = $4, status = 'ready', error = NULL,
           "retryAt" = NULL, "readyAt" = NOW(), "updatedAt" = NOW()
       WHERE "videoId" = $1 AND status = 'copying'`,
      [video.id, inspection.totalBytes, inspection.files, inspection.checksum]
    )
    await client.query(
      `UPDATE "Video"
       SET "coldStorage" = true,
           "coldStorageStatus" = 'staged',
           "coldStorageId" = NULL,
           "coldStoragePath" = $2,
           "coldStorageChecksum" = $3,
           "coldStorageBytes" = $4,
           "coldStorageUpdatedAt" = NOW(),
           "coldStorageRetryAt" = NULL,
           "coldStorageError" = NULL
       WHERE id = $1`,
      [video.id, stagePath, inspection.checksum, inspection.totalBytes]
    )
    await client.query('COMMIT')
    return true
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {})
    throw error
  }
}

async function abandonStage(client, videoId, stagePath, error) {
  await rm(stagePath, { recursive: true, force: true, maxRetries: 3, retryDelay: 1000 }).catch(() => {})
  await client.query(
    `DELETE FROM cold_storage_staging WHERE "videoId" = $1 AND status = 'copying'`,
    [videoId]
  ).catch(() => {})
  log(`${videoId}: ERROR ${error.message || error}`)
}

async function stageVideo(pool, storageId, days, video, stageRoot) {
  const client = await pool.connect()
  let sourcePath = null
  let stagePath = null
  let claimed = false
  let finalized = false
  try {
    const userId = safeComponent(video.userId, 'user ID')
    const videoId = safeComponent(video.id, 'video ID')
    sourcePath = await resolveSource(video)
    stagePath = path.join(stageRoot, 'videos', userId, videoId)
    if (!stagePath.startsWith(`${stageRoot}${path.sep}`)) throw new Error('stage path escapes staging root')
    await mkdir(path.dirname(stagePath), { recursive: true, mode: 0o750 })
    try {
      await lstat(stagePath)
      throw new Error('stage destination already exists')
    } catch (error) {
      if (error.code !== 'ENOENT') throw error
    }
    claimed = await claimCandidate(client, storageId, days, video, sourcePath, stagePath)
    if (!claimed) return { action: 'stale', bytes: 0 }

    const before = await inspectDirectory(sourcePath)
    const startedAt = Date.now()
    log(`${video.id}: copying ${formatBytes(before.totalBytes)} from /home to staging`)
    await cp(sourcePath, stagePath, {
      recursive: true,
      force: false,
      errorOnExist: true,
      preserveTimestamps: true,
    })
    const staged = await inspectDirectory(stagePath)
    if (staged.totalBytes !== before.totalBytes || staged.files !== before.files || staged.checksum !== before.checksum) {
      throw new Error('staged filename/size manifest does not match source')
    }
    const currentSource = await inspectDirectory(sourcePath)
    if (currentSource.totalBytes !== before.totalBytes || currentSource.files !== before.files || currentSource.checksum !== before.checksum) {
      throw new Error('source changed during staging copy')
    }
    if (!await finalizeStage(client, storageId, days, video, sourcePath, stagePath, staged)) {
      await rm(stagePath, { recursive: true, force: true })
      return { action: 'stale', bytes: 0 }
    }
    finalized = true

    await rm(sourcePath, { recursive: true, force: false, maxRetries: 3, retryDelay: 1000 })
    const elapsedMs = Math.max(Date.now() - startedAt, 1)
    log(`${video.id}: staged and freed ${formatBytes(staged.totalBytes)} from /home in ${(elapsedMs / 1000).toFixed(1)}s (${formatRate(staged.totalBytes / (elapsedMs / 1000))})`)
    return { action: 'staged', bytes: staged.totalBytes }
  } catch (error) {
    if (claimed && stagePath && !finalized) await abandonStage(client, video.id, stagePath, error)
    else if (finalized) log(`${video.id}: staged copy is safe but local source cleanup failed: ${error.message || error}`)
    else log(`${video.id}: ERROR ${error.message || error}`)
    return { action: 'error', bytes: 0 }
  } finally {
    client.release()
  }
}

async function main() {
  const options = parseArgs(process.argv.slice(2))
  if (!DATABASE_URL) throw new Error('DATABASE_URL is not configured')
  if (!options.execute) throw new Error('refusing to stage without --execute')

  await mkdir(STAGE_ROOT, { recursive: true, mode: 0o750 })
  const stageRoot = await realpath(STAGE_ROOT)
  const ingestRoot = await realpath(INGEST_DIR)
  const [stageInfo, ingestInfo] = await Promise.all([stat(stageRoot), stat(ingestRoot)])
  if (stageInfo.dev === ingestInfo.dev) throw new Error('staging root must be on a different filesystem from INGEST_DIR')

  const pool = new pg.Pool({ connectionString: DATABASE_URL, max: COPY_CONCURRENCY + 3, application_name: 'cold-storage-stage' })
  const control = await pool.connect()
  let lockHeld = false
  try {
    const storageId = await localStorageId(control)
    const lock = await control.query(`SELECT pg_try_advisory_lock(hashtext($1)) AS acquired`, [`cold-storage-stage:${storageId}`])
    if (!lock.rows[0].acquired) throw new Error('another cold staging worker is active')
    lockHeld = true
    const days = await inactivityDays(control)
    log(`staging root ${stageRoot}; cap ${formatBytes(STAGE_CAP_BYTES)}; reserve ${formatBytes(MIN_ROOT_AVAILABLE_BYTES)}; ${days} inactive days; concurrency ${COPY_CONCURRENCY}`)

    let handled = 0
    while (!shuttingDown && (options.limit === null || handled < options.limit)) {
      const used = await stageUsage(control, storageId)
      const filesystem = await statfs(stageRoot)
      const available = Number(filesystem.bavail) * Number(filesystem.bsize)
      const allowance = Math.min(STAGE_CAP_BYTES - used, available - MIN_ROOT_AVAILABLE_BYTES)
      if (allowance <= 0) {
        log(`staging paused: ${formatBytes(used)}/${formatBytes(STAGE_CAP_BYTES)} used; root has ${formatBytes(available)} available`)
        if (options.once) break
        await sleep(SCAN_INTERVAL_MS)
        continue
      }
      const remaining = options.limit === null ? COPY_CONCURRENCY : Math.min(COPY_CONCURRENCY, options.limit - handled)
      const candidates = await fetchCandidates(control, storageId, days, remaining * 4)
      const selected = []
      let reserved = 0
      for (const video of candidates) {
        const estimate = Math.max(Number(video.encodedSize || 0), 1)
        if (reserved + estimate > allowance) continue
        selected.push(video)
        reserved += estimate
        if (selected.length >= remaining) break
      }
      if (selected.length === 0) {
        log(candidates.length > 0 ? 'no candidate fits the remaining staging allowance' : 'no eligible videos found for local staging')
        if (options.once) break
        await sleep(SCAN_INTERVAL_MS)
        continue
      }
      const results = await Promise.all(selected.map(video => stageVideo(pool, storageId, days, video, stageRoot)))
      handled += results.length
      const freed = results.reduce((sum, result) => sum + result.bytes, 0)
      log(`batch complete: ${results.filter(result => result.action === 'staged').length}/${results.length} staged; ${formatBytes(freed)} freed; ${handled} handled this run`)
      if (options.once) break
    }
  } finally {
    if (lockHeld) await control.query(`SELECT pg_advisory_unlock(hashtext($1))`, [`cold-storage-stage:${await localStorageId(control)}`]).catch(() => {})
    control.release()
    await pool.end()
  }
}

process.on('SIGINT', () => { shuttingDown = true })
process.on('SIGTERM', () => { shuttingDown = true })

main().catch(error => {
  console.error(`[cold-stage] ERROR: ${error.message}`)
  process.exitCode = 1
})
