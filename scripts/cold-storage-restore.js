import http from 'node:http'
import { createHash, timingSafeEqual } from 'node:crypto'
import {
  lstat,
  mkdir,
  opendir,
  realpath,
  rename,
  stat,
  statfs,
} from 'node:fs/promises'
import path from 'node:path'
import { spawn, execFileSync } from 'node:child_process'
import pg from 'pg'
import { createClient as createRedisClient } from 'redis'
import { INGEST_DIR, isAllowed } from '../config.js'

const DATABASE_URL = process.env.DATABASE_URL
const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379'
const STAGING_ROOT = path.join(path.dirname(path.resolve(INGEST_DIR)), 'cold-restore')
const LOCAL_RESERVE_BYTES = 50_000_000_000
const QUEUE_INTERVAL_MS = 2_000
const PROGRESS_TTL_SECONDS = 7 * 24 * 60 * 60
const SAFE_COMPONENT = /^[A-Za-z0-9][A-Za-z0-9._-]*$/

let shuttingDown = false
let redis = null

function log(message) {
  console.log(`[${new Date().toISOString()}] [cold-restore] ${message}`)
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

function formatDuration(milliseconds) {
  const seconds = Math.max(0, milliseconds) / 1000
  return seconds < 10 ? `${seconds.toFixed(1)}s` : `${Math.round(seconds)}s`
}

const SIZE_UNITS = new Map([
  ['B', 1], ['kB', 1000], ['MB', 1000 ** 2], ['GB', 1000 ** 3], ['TB', 1000 ** 4],
  ['KiB', 1024], ['MiB', 1024 ** 2], ['GiB', 1024 ** 3], ['TiB', 1024 ** 4],
])

function parseRcloneProgress(text) {
  const clean = text.replace(/\x1b\[[0-9;]*[A-Za-z]/g, '')
  const matches = [...clean.matchAll(/(?:Transferred:\s*)?([\d.]+)\s*([kKMGT]i?B)\s*\/\s*[\d.]+\s*[kKMGT]i?B,\s*(\d+)%\s*,\s*([\d.]+)\s*([kKMGT]i?B)\/s/gi)]
  const match = matches.at(-1)
  if (!match) return null
  const normalizeUnit = unit => unit.length === 2
    ? `${unit[0] === 'K' ? 'k' : unit[0].toUpperCase()}B`
    : `${unit[0] === 'K' ? 'K' : unit[0].toUpperCase()}iB`
  const byteUnit = normalizeUnit(match[2])
  const rateUnit = normalizeUnit(match[5])
  return {
    bytes: Number(match[1]) * (SIZE_UNITS.get(byteUnit) || 1),
    percent: Number(match[3]),
    speedBytesPerSecond: Number(match[4]) * (SIZE_UNITS.get(rateUnit) || 1),
  }
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

function assertSafeComponent(value, label) {
  if (!SAFE_COMPONENT.test(String(value || ''))) throw new Error(`${label} contains unsafe characters`)
  return String(value)
}

function assertSafeRemotePath(value) {
  const normalized = String(value || '').replace(/^\.\//, '').replace(/\/$/, '')
  if (!normalized || normalized.startsWith('/') || normalized.includes('..')) throw new Error('unsafe remote path')
  for (const component of normalized.split('/')) assertSafeComponent(component, 'remote path')
  return normalized
}

function sshBaseArgs(box) {
  if (/\s/.test(box.sshKeyPath) || /\s/.test(box.knownHostsPath)) throw new Error('SSH paths must not contain whitespace')
  return [
    '-p', String(box.sshPort), '-i', box.sshKeyPath,
    '-o', 'BatchMode=yes', '-o', 'IdentitiesOnly=yes',
    '-o', 'StrictHostKeyChecking=yes', '-o', `UserKnownHostsFile=${box.knownHostsPath}`,
    '-o', 'ConnectTimeout=15', '-o', 'ServerAliveInterval=30', '-o', 'ServerAliveCountMax=4',
  ]
}

function rsyncSsh(box) {
  return ['ssh', ...sshBaseArgs(box)].join(' ')
}

function remoteSpec(box, remotePath) {
  return `${assertSafeComponent(box.username, 'Storage Box username')}@${box.hostname}:./${assertSafeRemotePath(remotePath)}/`
}

function runCommand(command, args, { onOutput = null, timeoutMs = 0 } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { stdio: ['ignore', 'pipe', 'pipe'] })
    let stdout = ''
    let stderr = ''
    let timer = null
    const collect = (chunk, target) => {
      const text = chunk.toString()
      if (target === 'stdout') stdout = (stdout + text).slice(-1_000_000)
      else stderr = (stderr + text).slice(-1_000_000)
      onOutput?.(text, target)
    }
    child.stdout.on('data', chunk => collect(chunk, 'stdout'))
    child.stderr.on('data', chunk => collect(chunk, 'stderr'))
    child.on('error', reject)
    if (timeoutMs > 0) {
      timer = setTimeout(() => child.kill('SIGTERM'), timeoutMs)
      timer.unref()
    }
    child.on('close', (code, signal) => {
      if (timer) clearTimeout(timer)
      if (code === 0) resolve({ code, signal, stdout, stderr })
      else reject(new Error(`${command} exited ${code}${stderr.trim() ? `: ${stderr.trim()}` : ''}`))
    })
  })
}

async function runSsh(box, command, commandArgs = [], options = {}) {
  for (const value of commandArgs) {
    if (['-rf', '--'].includes(value)) continue
    assertSafeRemotePath(value)
  }
  return runCommand('ssh', [
    ...sshBaseArgs(box),
    `${assertSafeComponent(box.username, 'Storage Box username')}@${box.hostname}`,
    command,
    ...commandArgs,
  ], options)
}

async function removeRemoteArchive(box, remotePath) {
  const safePath = assertSafeRemotePath(remotePath)
  const basePath = assertSafeRemotePath(box.basePath)
  if (!safePath.startsWith(`${basePath}/videos/`)) throw new Error('remote archive path is outside the configured videos root')
  await runSsh(box, 'rm', ['-rf', safePath], { timeoutMs: 120_000 })
}

async function inspectAndHashDirectory(root) {
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
      const relativePath = path.posix.join(relativeDirectory.split(path.sep).join('/'), entry.name)
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
  if (files === 0) throw new Error('restored asset directory is empty')
  return { checksum: `size-sha256:${overall.digest('hex')}`, totalBytes, files }
}

function localStorageIp() {
  return execFileSync('hostname', ['-I'], { encoding: 'utf8' }).trim().split(/\s+/)[0]
}

async function loadLocalServer(client) {
  const { rows } = await client.query(
    `SELECT id, "apiKey", "coldStorageApiHost", "coldStorageApiPort"
     FROM "Server" WHERE ip = $1 AND type = 'storage'`,
    [localStorageIp()]
  )
  if (rows.length !== 1) throw new Error('local host does not identify exactly one storage server')
  return rows[0]
}

async function loadBox(client, boxId) {
  const { rows } = await client.query(
    `SELECT id, hostname, username, "sshPort", "sshKeyPath", "knownHostsPath",
            "basePath", "archiveConcurrency", "restoreConcurrency", "maxConnections", "isActive"
     FROM storage_boxes WHERE id = $1`,
    [boxId]
  )
  if (rows.length !== 1 || !rows[0].isActive) throw new Error(`active Storage Box ${boxId} not found`)
  const box = rows[0]
  if (!SAFE_COMPONENT.test(box.username) || !/^[-A-Za-z0-9.]+$/.test(box.hostname)) throw new Error('unsafe Storage Box connection details')
  if (!box.sshKeyPath || !box.knownHostsPath) throw new Error('Storage Box SSH paths are missing')
  return box
}

function assetDirectory(video) {
  const relative = video.encodedPath?.trim()
    ? path.dirname(video.encodedPath)
    : path.join(video.userId, video.id)
  const root = path.resolve(INGEST_DIR)
  const resolved = path.resolve(root, relative)
  if (resolved === root || !resolved.startsWith(`${root}${path.sep}`)) throw new Error('asset directory escapes INGEST_DIR')
  return resolved
}

async function ensureStagingRoot() {
  await mkdir(STAGING_ROOT, { recursive: true, mode: 0o750 })
  const [ingest, staging] = await Promise.all([stat(await realpath(INGEST_DIR)), stat(await realpath(STAGING_ROOT))])
  if (ingest.dev !== staging.dev) throw new Error('restore staging directory must share the INGEST_DIR filesystem')
}

async function exists(directory) {
  try {
    const info = await lstat(directory)
    return info.isDirectory() && !info.isSymbolicLink()
  } catch (error) {
    if (error.code === 'ENOENT') return false
    throw error
  }
}

async function connectRedis() {
  const client = createRedisClient({ url: REDIS_URL, socket: { connectTimeout: 1_500, reconnectStrategy: false } })
  client.on('error', () => {})
  try {
    await client.connect()
    log('Redis progress store connected')
    return client
  } catch (error) {
    log(`Redis unavailable; progress remains state-only (${error.message})`)
    try { client.destroy() } catch {}
    return null
  }
}

function progressKey(videoId) {
  return `firestream:cold-restore:${videoId}`
}

async function setProgress(videoId, values) {
  if (!redis) return
  const normalized = Object.fromEntries(Object.entries(values).map(([key, value]) => [key, String(value ?? '')]))
  await redis.hSet(progressKey(videoId), normalized).catch(() => {})
  await redis.expire(progressKey(videoId), PROGRESS_TTL_SECONDS).catch(() => {})
}

async function getProgress(videoId) {
  if (!redis) return null
  const values = await redis.hGetAll(progressKey(videoId)).catch(() => null)
  return values && Object.keys(values).length > 0 ? values : null
}

async function acquireRestoreSlot(client, box) {
  const first = Number(box.archiveConcurrency) + 1
  const last = first + Number(box.restoreConcurrency) - 1
  while (!shuttingDown) {
    for (let slot = first; slot <= last; slot++) {
      const { rows } = await client.query(
        `SELECT pg_try_advisory_lock(hashtext($1), $2) AS acquired`,
        [`storage-box-connections:${box.id}`, slot]
      )
      if (rows[0].acquired) return slot
    }
    await sleep(500)
  }
  throw new Error('shutdown requested while waiting for a Storage Box connection')
}

async function releaseRestoreSlot(client, box, slot) {
  if (slot === null) return
  await client.query(
    `SELECT pg_advisory_unlock(hashtext($1), $2)`,
    [`storage-box-connections:${box.id}`, slot]
  ).catch(() => {})
}

function makeProgressReporter(video) {
  let lastApiUpdate = 0
  let lastLogUpdate = 0
  let completionLogged = false
  let latestBytes = 0
  let buffered = ''
  const startedAt = Date.now()
  const onOutput = chunk => {
    buffered = (buffered + chunk).slice(-8192)
    const progress = parseRcloneProgress(buffered)
    if (!progress) return
    const { bytes, percent } = progress
    latestBytes = Math.max(latestBytes, bytes)
    const now = Date.now()
    const elapsedSeconds = Math.max((now - startedAt) / 1000, 0.001)
    const speedBytesPerSecond = progress.speedBytesPerSecond || bytes / elapsedSeconds
    if (now - lastApiUpdate >= 5_000 || percent === 100) {
      lastApiUpdate = now
      setProgress(video.id, {
        state: 'restoring',
        bytesTransferred: bytes,
        totalBytes: video.coldStorageBytes,
        percent,
        speedBytesPerSecond: Math.round(speedBytesPerSecond),
        speed: formatRate(speedBytesPerSecond),
        error: '',
        updatedAt: new Date().toISOString(),
      })
    }
    if (now - lastLogUpdate >= 1_000 || (percent === 100 && !completionLogged)) {
      lastLogUpdate = now
      if (percent === 100) completionLogged = true
      log(`${video.id}: restore ${percent}% (${formatBytes(bytes)}/${formatBytes(video.coldStorageBytes)}) at ${formatRate(speedBytesPerSecond)}`)
    }
  }
  return {
    onOutput,
    stats() {
      const elapsedMs = Math.max(Date.now() - startedAt, 1)
      const bytes = latestBytes || Number(video.coldStorageBytes)
      return { elapsedMs, speedBytesPerSecond: bytes / (elapsedMs / 1000) }
    },
  }
}

async function downloadVideo(box, video, stagingDirectory) {
  const reporter = makeProgressReporter(video)
  await runCommand('rclone', [
    'sync', `:sftp:./${assertSafeRemotePath(video.coldStoragePath)}`, stagingDirectory,
    '--sftp-host', box.hostname,
    '--sftp-user', box.username,
    '--sftp-port', String(box.sshPort),
    '--sftp-key-file', box.sshKeyPath,
    '--sftp-known-hosts-file', box.knownHostsPath,
    '--sftp-chunk-size', '255Ki',
    '--sftp-concurrency', '128',
    '--sftp-disable-hashcheck',
    '--sftp-set-modtime=false',
    '--size-only',
    '--transfers', '1', '--checkers', '1', '--check-first',
    '--stats', '1s', '--stats-one-line', '--stats-log-level', 'NOTICE',
    '--log-level', 'NOTICE',
  ], { onOutput: reporter.onOutput })
  return reporter.stats()
}

async function verifyExactRemoteSizes(box, video, localDirectory) {
  await runCommand('rclone', [
    'check', localDirectory, `:sftp:./${assertSafeRemotePath(video.coldStoragePath)}`,
    '--size-only', '--checkers', '1',
    '--sftp-host', box.hostname,
    '--sftp-user', box.username,
    '--sftp-port', String(box.sshPort),
    '--sftp-key-file', box.sshKeyPath,
    '--sftp-known-hosts-file', box.knownHostsPath,
    '--sftp-disable-hashcheck',
    '--log-level', 'NOTICE',
  ])
}

function verifyStoredManifest(video, inspection) {
  if (inspection.totalBytes !== Number(video.coldStorageBytes)) {
    throw new Error('restored byte count does not match the archive')
  }
  if (String(video.coldStorageChecksum || '').startsWith('size-sha256:') &&
      inspection.checksum !== video.coldStorageChecksum) {
    throw new Error('restored filename/size manifest does not match the archive')
  }
}

async function markRestoreFailure(client, videoId, error) {
  const message = String(error.message || error).slice(0, 2000)
  await client.query(
    `UPDATE "Video"
     SET "coldStorageStatus" = 'failed', "coldStorageUpdatedAt" = NOW(), "coldStorageError" = $2
     WHERE id = $1 AND "coldStorage"`,
    [videoId, message]
  )
  await setProgress(videoId, { state: 'failed', error: message, updatedAt: new Date().toISOString() })
}

async function markLocal(client, video, box) {
  await client.query('BEGIN')
  try {
    const locked = await client.query(
      `SELECT status, "coldStorage", "coldStorageStatus" FROM "Video" WHERE id = $1 FOR UPDATE`,
      [video.id]
    )
    const current = locked.rows[0]
    if (!current || !['active', 'offline'].includes(current.status) || !current.coldStorage) {
      throw new Error('video became unavailable while completing restore')
    }
    await removeRemoteArchive(box, video.coldStoragePath)
    const result = await client.query(
    `UPDATE "Video"
     SET "coldStorage" = false,
         "coldStorageStatus" = 'local',
         "coldStorageId" = NULL,
         "coldStoragePath" = NULL,
         "coldStorageChecksum" = NULL,
         "coldStorageBytes" = NULL,
         "archivedAt" = NULL,
         "restoreRequestedAt" = NULL,
         "coldStorageUpdatedAt" = NOW(),
         "coldStorageRetryAt" = NULL,
         "coldStorageError" = NULL
     WHERE id = $1 AND "coldStorage"
     RETURNING id`,
      [video.id]
    )
    if (result.rowCount !== 1) throw new Error('video changed while completing restore')
    await client.query(
      `UPDATE storage_boxes
       SET "usedBytes" = GREATEST(0, "usedBytes" - $2), "updatedAt" = NOW()
       WHERE id = $1`,
      [box.id, Number(video.coldStorageBytes || 0)]
    )
    await client.query('COMMIT')
    await setProgress(video.id, {
      state: 'ready', percent: 100, error: '', updatedAt: new Date().toISOString(),
    })
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {})
    throw error
  }
}

async function restoreVideo(pool, video) {
  const client = await pool.connect()
  let box = null
  let slot = null
  let videoLock = false
  const stagingDirectory = path.join(STAGING_ROOT, assertSafeComponent(video.id, 'video ID'))
  try {
    const lock = await client.query(
      `SELECT pg_try_advisory_lock(hashtext('cold-storage-restore'), hashtext($1)) AS acquired`,
      [video.id]
    )
    if (!lock.rows[0].acquired) return
    videoLock = true
    box = await loadBox(client, video.coldStorageId)
    slot = await acquireRestoreSlot(client, box)
    const destination = assetDirectory(video)

    if (await exists(destination)) {
      const current = await inspectAndHashDirectory(destination)
      verifyStoredManifest(video, current)
      await verifyExactRemoteSizes(box, video, destination)
      await markLocal(client, video, box)
      log(`${video.id}: verified existing local copy; marked ready`)
      return
    }

    const filesystem = await statfs(INGEST_DIR, { bigint: true })
    const availableBlocks = typeof process.getuid === 'function' && process.getuid() === 0
      ? filesystem.bfree
      : filesystem.bavail
    const available = Number(availableBlocks * filesystem.bsize)
    const required = Number(video.coldStorageBytes) + LOCAL_RESERVE_BYTES
    if (available < required) throw new Error(`insufficient local space: need ${required} bytes including reserve, have ${available}`)

    await mkdir(stagingDirectory, { recursive: true, mode: 0o750 })
    await setProgress(video.id, {
      state: 'restoring', bytesTransferred: 0, totalBytes: video.coldStorageBytes,
      percent: 0, speedBytesPerSecond: 0, speed: '0 B/s',
      error: '', updatedAt: new Date().toISOString(),
    })
    const transferStats = await downloadVideo(box, video, stagingDirectory)
    const restored = await inspectAndHashDirectory(stagingDirectory)
    verifyStoredManifest(video, restored)
    await verifyExactRemoteSizes(box, video, stagingDirectory)

    await mkdir(path.dirname(destination), { recursive: true, mode: 0o755 })
    if (await exists(destination)) throw new Error('local destination appeared during restore')
    await rename(stagingDirectory, destination)
    await markLocal(client, video, box)
    log(`${video.id}: restored ${formatBytes(restored.totalBytes)} in ${formatDuration(transferStats.elapsedMs)}; average ${formatRate(transferStats.speedBytesPerSecond)}; marked ready`)
  } catch (error) {
    await markRestoreFailure(client, video.id, error).catch(() => {})
    log(`${video.id}: ERROR ${error.message}`)
  } finally {
    if (box) await releaseRestoreSlot(client, box, slot)
    if (videoLock) {
      await client.query(
        `SELECT pg_advisory_unlock(hashtext('cold-storage-restore'), hashtext($1))`,
        [video.id]
      ).catch(() => {})
    }
    client.release()
  }
}

async function claimQueuedRestores(client, storageId, limit) {
  const { rows } = await client.query(
    `SELECT v.id, v."userId", v."encodedPath", v."coldStorageId",
            v."coldStoragePath", v."coldStorageChecksum", v."coldStorageBytes"
     FROM "Video" v
     WHERE v."storageId" = $1
       AND v.status IN ('active', 'offline')
       AND v."coldStorage"
       AND v."coldStorageStatus" = 'restoring'
       AND v."restoreRequestedAt" IS NOT NULL
     ORDER BY v."restoreRequestedAt", v.id
     LIMIT $2`,
    [storageId, limit]
  )
  return rows
}

async function restoreLoop(pool, storageId, concurrency) {
  const running = new Set()
  while (!shuttingDown) {
    try {
      if (running.size < concurrency) {
        const client = await pool.connect()
        const queued = await claimQueuedRestores(client, storageId, concurrency - running.size)
        client.release()
        for (const video of queued) {
          if ([...running].some(item => item.videoId === video.id)) continue
          const promise = restoreVideo(pool, video).finally(() => running.delete(wrapper))
          const wrapper = { videoId: video.id, promise }
          running.add(wrapper)
        }
      }
    } catch (error) {
      log(`queue error: ${error.message}`)
    }
    await sleep(QUEUE_INTERVAL_MS)
  }
  await Promise.allSettled([...running].map(item => item.promise))
}

function authorized(req, apiKey) {
  const ip = req.socket.remoteAddress?.replace('::ffff:', '')
  if (!isAllowed(ip) && ip !== '127.0.0.1' && ip !== '::1') return false
  if (!apiKey) return true
  const header = req.headers.authorization || ''
  if (!header.startsWith('Bearer ')) return false
  const supplied = Buffer.from(header.slice(7))
  const expected = Buffer.from(apiKey)
  return supplied.length === expected.length && timingSafeEqual(supplied, expected)
}

function sendJson(res, status, body, extraHeaders = {}) {
  const json = JSON.stringify(body)
  res.writeHead(status, {
    'Content-Type': 'application/json',
    'Content-Length': Buffer.byteLength(json),
    'Cache-Control': 'no-store',
    ...extraHeaders,
  })
  res.end(json)
}

async function requestRestore(pool, storageId, videoId) {
  const client = await pool.connect()
  try {
    await client.query('BEGIN')
    const { rows } = await client.query(
      `SELECT id, "storageId", status, "coldStorage", "coldStorageStatus", "coldStorageId",
              "coldStoragePath", "coldStorageChecksum", "coldStorageBytes"
       FROM "Video" WHERE id = $1 FOR UPDATE`,
      [videoId]
    )
    const video = rows[0]
    if (!video || video.storageId !== storageId) {
      await client.query('ROLLBACK')
      return { status: 404, body: { error: 'video_not_found' } }
    }
    if (!['active', 'offline'].includes(video.status)) {
      await client.query('ROLLBACK')
      return { status: 410, body: { error: 'video_unavailable' } }
    }
    if (!video.coldStorage) {
      await client.query(
        `UPDATE "Video"
         SET "lastView" = NOW(),
             "coldStorageStatus" = CASE WHEN "coldStorageStatus" = 'archiving' THEN 'local' ELSE "coldStorageStatus" END,
             "coldStorageUpdatedAt" = NOW()
         WHERE id = $1`,
        [videoId]
      )
      await client.query('COMMIT')
      await setProgress(videoId, { state: 'ready', percent: 100, error: '', updatedAt: new Date().toISOString() })
      return {
        status: 200,
        body: { videoId, state: 'ready', ready: true, percent: 100, speedBytesPerSecond: 0, speed: '0 B/s' },
      }
    }
    if (!video.coldStorageId || !video.coldStoragePath || !video.coldStorageChecksum || !video.coldStorageBytes) {
      await client.query('ROLLBACK')
      return { status: 409, body: { error: 'archive_metadata_incomplete' } }
    }
    await client.query(
      `UPDATE "Video"
       SET "coldStorageStatus" = 'restoring',
           "restoreRequestedAt" = COALESCE("restoreRequestedAt", NOW()),
           "coldStorageUpdatedAt" = NOW(),
           "coldStorageError" = NULL,
           "lastView" = NOW()
       WHERE id = $1`,
      [videoId]
    )
    await client.query('COMMIT')
    await setProgress(videoId, {
      state: 'queued', bytesTransferred: 0, totalBytes: video.coldStorageBytes,
      percent: 0, speedBytesPerSecond: 0, speed: '0 B/s',
      error: '', updatedAt: new Date().toISOString(),
    })
    return {
      status: 202,
      body: { videoId, state: 'queued', ready: false, percent: 0, speedBytesPerSecond: 0, speed: '0 B/s' },
      headers: { 'Retry-After': '2' },
    }
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {})
    throw error
  } finally {
    client.release()
  }
}

async function restoreStatus(pool, storageId, videoId) {
  const { rows } = await pool.query(
    `SELECT id, "coldStorage", "coldStorageStatus", "coldStorageBytes",
            "coldStorageError", "restoreRequestedAt", "coldStorageUpdatedAt"
     FROM "Video" WHERE id = $1 AND "storageId" = $2`,
    [videoId, storageId]
  )
  if (rows.length !== 1) return { status: 404, body: { error: 'video_not_found' } }
  const video = rows[0]
  const progress = await getProgress(videoId)
  const ready = !video.coldStorage && video.coldStorageStatus === 'local'
  return {
    status: 200,
    body: {
      videoId,
      state: ready ? 'ready' : (progress?.state || video.coldStorageStatus),
      ready,
      percent: ready ? 100 : Number(progress?.percent || 0),
      bytesTransferred: Number(progress?.bytesTransferred || 0),
      totalBytes: Number(progress?.totalBytes || video.coldStorageBytes || 0),
      speedBytesPerSecond: Number(progress?.speedBytesPerSecond || 0),
      speed: progress?.speed || '0 B/s',
      error: video.coldStorageError || progress?.error || null,
      requestedAt: video.restoreRequestedAt,
      updatedAt: progress?.updatedAt || video.coldStorageUpdatedAt,
    },
  }
}

function createApiServer(pool, localServer) {
  return http.createServer(async (req, res) => {
    try {
      if (req.method === 'GET' && req.url === '/health') {
        return sendJson(res, 200, { status: 'ok', storageServerId: localServer.id })
      }
      if (!authorized(req, localServer.apiKey)) return sendJson(res, 403, { error: 'forbidden' })
      const url = new URL(req.url, 'http://localhost')
      const match = url.pathname.match(/^\/restore\/([A-Za-z0-9._-]+)$/)
      if (!match) return sendJson(res, 404, { error: 'not_found' })
      const videoId = assertSafeComponent(match[1], 'video ID')
      const result = req.method === 'POST'
        ? await requestRestore(pool, localServer.id, videoId)
        : req.method === 'GET'
          ? await restoreStatus(pool, localServer.id, videoId)
          : { status: 405, body: { error: 'method_not_allowed' } }
      sendJson(res, result.status, result.body, result.headers)
    } catch (error) {
      log(`API error: ${error.message}`)
      sendJson(res, 500, { error: 'internal_error' })
    }
  })
}

async function main() {
  if (!DATABASE_URL) throw new Error('DATABASE_URL is not configured')
  await ensureStagingRoot()
  const pool = new pg.Pool({ connectionString: DATABASE_URL, max: 12, application_name: 'cold-storage-restore' })
  const client = await pool.connect()
  const localServer = await loadLocalServer(client)
  const boxes = await client.query(`SELECT COALESCE(max("restoreConcurrency"), 1)::int AS concurrency FROM storage_boxes WHERE "isActive"`)
  client.release()
  const concurrency = Math.max(1, boxes.rows[0].concurrency)
  redis = await connectRedis()
  const server = createApiServer(pool, localServer)
  server.listen(localServer.coldStorageApiPort, localServer.coldStorageApiHost, () => {
    log(`API listening on ${localServer.coldStorageApiHost}:${localServer.coldStorageApiPort}`)
  })
  const loop = restoreLoop(pool, localServer.id, concurrency)

  const shutdown = async signal => {
    if (shuttingDown) return
    shuttingDown = true
    log(`${signal} received; draining restores`)
    server.close()
    await loop
    if (redis?.isOpen) await redis.quit().catch(() => redis.destroy())
    await pool.end()
  }
  process.on('SIGINT', () => shutdown('SIGINT'))
  process.on('SIGTERM', () => shutdown('SIGTERM'))
}

main().catch(error => {
  console.error(`[cold-restore] ERROR: ${error.message}`)
  process.exitCode = 1
})
