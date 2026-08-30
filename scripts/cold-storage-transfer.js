import { createHash } from 'node:crypto'
import {
  lstat,
  mkdir,
  opendir,
  realpath,
  rename,
  rm,
  stat,
} from 'node:fs/promises'
import path from 'node:path'
import { spawn, execFileSync } from 'node:child_process'
import pg from 'pg'
import { INGEST_DIR } from '../config.js'

const DATABASE_URL = process.env.DATABASE_URL
const RETRY_MINUTES = 30
const MISSING_SOURCE_RETRY_MINUTES = 24 * 60
const STAGE_ROOT = process.env.COLD_STAGE_ROOT || '/cold-stage'
const DEFAULT_MINIMUM_FREE_BYTES = 100_000_000_000
const MAX_WAITING_INGEST_TRANSFERS = 8
const INGEST_QUEUE_POLL_MS = 5_000
const INGEST_QUEUE_LOG_MS = 60_000
const DELETE_ROOT = path.join(path.dirname(path.resolve(INGEST_DIR)), 'cold-delete')
const SAFE_COMPONENT = /^[A-Za-z0-9][A-Za-z0-9._-]*$/

let shuttingDown = false

function usage() {
  console.log(`Usage: node scripts/cold-storage-transfer.js [options]

Continuously archives clone-free videos according to database-managed Storage Box policy.
Dry-run is the default and performs no database, remote, or filesystem writes.

Options:
  --execute       Enable verified remote uploads and local space reclamation
  --once          Perform one scan/batch and exit
  --limit N       Inspect/process at most N videos
  --video-id ID   Process one specifically eligible video (pilot/operations)
  --verbose       Print candidate details
  --help          Show this help
`)
}

function positiveInt(value, name) {
  const parsed = Number.parseInt(value, 10)
  if (!Number.isSafeInteger(parsed) || parsed <= 0) throw new Error(`${name} must be a positive integer`)
  return parsed
}

function parseArgs(argv) {
  const options = { execute: false, once: false, limit: null, videoId: null, verbose: false }
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]
    if (arg === '--execute') options.execute = true
    else if (arg === '--once') options.once = true
    else if (arg === '--verbose') options.verbose = true
    else if (arg === '--limit') options.limit = positiveInt(argv[++i], '--limit')
    else if (arg === '--video-id') options.videoId = assertSafeComponent(argv[++i], '--video-id')
    else if (arg === '--help') options.help = true
    else throw new Error(`Unknown option: ${arg}`)
  }
  if (!options.execute) options.once = true
  if (options.videoId) {
    options.once = true
    options.limit = 1
  }
  return options
}

function log(message) {
  console.log(`[${new Date().toISOString()}] [cold-transfer] ${message}`)
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
  if (/\s/.test(box.sshKeyPath) || /\s/.test(box.knownHostsPath)) throw new Error('SSH key and known-hosts paths must not contain whitespace')
  return [
    '-p', String(box.sshPort),
    '-i', box.sshKeyPath,
    '-o', 'BatchMode=yes',
    '-o', 'IdentitiesOnly=yes',
    '-o', 'StrictHostKeyChecking=yes',
    '-o', `UserKnownHostsFile=${box.knownHostsPath}`,
    '-o', 'ConnectTimeout=15',
    '-o', 'ServerAliveInterval=30',
    '-o', 'ServerAliveCountMax=4',
  ]
}

function rsyncSsh(box) {
  return ['ssh', ...sshBaseArgs(box)].join(' ')
}

function runCommand(command, args, { allowFailure = false, onOutput = null, timeoutMs = 0 } = {}) {
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
      const result = { code, signal, stdout, stderr }
      if (code === 0 || allowFailure) resolve(result)
      else reject(new Error(`${command} exited ${code}${stderr.trim() ? `: ${stderr.trim()}` : ''}`))
    })
  })
}

async function runSsh(box, command, commandArgs = [], options = {}) {
  for (const value of commandArgs) {
    if (['-p', '-rf', '--'].includes(value)) continue
    assertSafeRemotePath(value)
  }
  const destination = `${assertSafeComponent(box.username, 'Storage Box username')}@${box.hostname}`
  return runCommand('ssh', [...sshBaseArgs(box), destination, command, ...commandArgs], options)
}

function remoteSpec(box, remotePath) {
  return `${assertSafeComponent(box.username, 'Storage Box username')}@${box.hostname}:./${assertSafeRemotePath(remotePath)}/`
}

async function remoteExists(box, remotePath) {
  const result = await runSsh(box, 'stat', [remotePath], { allowFailure: true, timeoutMs: 30_000 })
  return result.code === 0
}

async function remoteUsage(box) {
  const result = await runCommand('ssh', [
    ...sshBaseArgs(box),
    `${assertSafeComponent(box.username, 'Storage Box username')}@${box.hostname}`,
    'df', '-B1', '.',
  ], { timeoutMs: 30_000 })
  const lines = result.stdout.trim().split('\n')
  const values = lines.at(-1)?.trim().split(/\s+/)
  if (!values || values.length < 4) throw new Error(`cannot parse Storage Box df output: ${result.stdout.trim()}`)
  const total = Number(values[1])
  const used = Number(values[2])
  const available = Number(values[3])
  if (![total, used, available].every(Number.isFinite)) throw new Error('Storage Box returned invalid capacity values')
  return { total, used, available, ratio: total > 0 ? used / total : 1 }
}

async function resolveAssetDirectory(video) {
  const relative = video.encodedPath?.trim()
    ? path.dirname(video.encodedPath)
    : path.join(video.userId, video.id)
  const root = await realpath(INGEST_DIR)
  const resolved = path.resolve(root, relative)
  if (resolved === root || !resolved.startsWith(`${root}${path.sep}`)) throw new Error('asset directory escapes INGEST_DIR')
  const actual = await realpath(resolved)
  if (actual !== resolved || !actual.startsWith(`${root}${path.sep}`)) throw new Error('asset directory is a symlink or escapes INGEST_DIR')
  const info = await lstat(actual)
  if (!info.isDirectory() || info.isSymbolicLink()) throw new Error('asset path is not a safe directory')
  return actual
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
  if (files === 0) throw new Error('asset directory is empty')
  return { checksum: `size-sha256:${overall.digest('hex')}`, totalBytes, files }
}

async function inspectAndHashForArchive(root, videoId) {
  const startedAt = Date.now()
  const inspection = await inspectAndHashDirectory(root)
  const elapsedMs = Math.max(Date.now() - startedAt, 1)
  log(`${videoId}: exact-size manifest complete: ${inspection.files} files, ${formatBytes(inspection.totalBytes)} in ${formatDuration(elapsedMs)}`)
  return inspection
}

function progressReporter(videoId, totalBytes, startedAt) {
  let lastLogAt = 0
  let completionLogged = false
  let buffered = ''
  return chunk => {
    buffered = (buffered + chunk).slice(-8192)
    const progress = parseRcloneProgress(buffered)
    if (!progress) return
    const { bytes, percent } = progress
    const now = Date.now()
    if (now - lastLogAt >= 1_000 || (percent === 100 && !completionLogged)) {
      lastLogAt = now
      if (percent === 100) completionLogged = true
      const speedBytesPerSecond = progress.speedBytesPerSecond ||
        bytes / Math.max((Date.now() - startedAt) / 1000, 0.001)
      log(`${videoId}: upload ${percent}% (${formatBytes(bytes)}/${formatBytes(totalBytes)}) at ${formatRate(speedBytesPerSecond)}`)
    }
  }
}

async function uploadDirectory(box, sourceDir, remotePath, videoId, totalBytes) {
  const startedAt = Date.now()
  const args = [
    'sync', sourceDir, `:sftp:./${assertSafeRemotePath(remotePath)}`,
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
  ]
  await runCommand('rclone', args, { onOutput: progressReporter(videoId, totalBytes, startedAt) })
  const elapsedMs = Math.max(Date.now() - startedAt, 1)
  log(`${videoId}: upload complete in ${formatDuration(elapsedMs)}; average ${formatRate(totalBytes / (elapsedMs / 1000))}`)
}

async function verifyRemoteDirectory(box, sourceDir, remotePath) {
  await runCommand('rclone', [
    'check', sourceDir, `:sftp:./${assertSafeRemotePath(remotePath)}`,
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

async function finalizeRemote(box, incomingPath, finalPath) {
  if (await remoteExists(box, finalPath)) return
  await runSsh(box, 'mkdir', ['-p', path.posix.dirname(finalPath)], { timeoutMs: 30_000 })
  await runSsh(box, 'mv', [incomingPath, finalPath], { timeoutMs: 30_000 })
}

async function removeRemoteArchive(box, remotePath) {
  const safePath = assertSafeRemotePath(remotePath)
  if (!safePath.startsWith(`${box.basePath}/videos/`)) throw new Error('remote archive path is outside the configured videos root')
  await runSsh(box, 'rm', ['-rf', safePath], { timeoutMs: 120_000 })
}

const ELIGIBLE_SQL = `
  v."storageId" = $1
  AND v.status = 'active'
  AND v."encodingStatus" = 'completed'
  AND NOT v."coldStorage"
  AND v."coldStorageStatus" IN ('local', 'failed')
  AND (v."coldStorageRetryAt" IS NULL OR v."coldStorageRetryAt" <= NOW())
  AND COALESCE(v."lastView", v."createdAt") <= NOW() - make_interval(days => $2)
  AND v."clonedFrom" IS NULL
  AND NOT v."hasCopies"
  AND NOT EXISTS (SELECT 1 FROM "Video" child WHERE child."clonedFrom" = v.id)
  AND NOT EXISTS (
    SELECT 1 FROM "Video" shared
    WHERE shared.id <> v.id
      AND v."encodedPath" <> ''
      AND shared."encodedPath" = v."encodedPath"
  )`

async function localStorageId(client) {
  const ip = execFileSync('hostname', ['-I'], { encoding: 'utf8' }).trim().split(/\s+/)[0]
  const { rows } = await client.query(`SELECT id FROM "Server" WHERE ip = $1 AND type = 'storage'`, [ip])
  if (rows.length !== 1) throw new Error(`local IP ${ip} does not identify exactly one storage server`)
  return rows[0].id
}

async function countWaitingIngestTransfers(client, storageId) {
  const { rows } = await client.query(
    `SELECT count(*)::int AS count
     FROM "Video"
     WHERE status = 'active'
       AND "encodingStatus" = 'completed'
       AND "transferStatus" = 'pending'
       AND ("storageId" = $1 OR "storageId" IS NULL)`,
    [storageId]
  )
  return rows[0].count
}

async function waitForIngestCapacity(client, storageId) {
  let paused = false
  let lastLogAt = 0
  while (!shuttingDown) {
    if ((await fetchStagedBatch(client, storageId, 1)).ids.length > 0) return true
    const waiting = await countWaitingIngestTransfers(client, storageId)
    if (waiting <= MAX_WAITING_INGEST_TRANSFERS) {
      if (paused) {
        log(`RESUMING: production ingest queue is down to ${waiting} waiting video(s)`)
      }
      return true
    }

    const now = Date.now()
    if (!paused || now - lastLogAt >= INGEST_QUEUE_LOG_MS) {
      log(`PAUSED: ${waiting} production video(s) are waiting transfer; cold archiving resumes at ${MAX_WAITING_INGEST_TRANSFERS} or fewer (checking every ${INGEST_QUEUE_POLL_MS / 1000}s)`)
      lastLogAt = now
    }
    paused = true
    await sleep(INGEST_QUEUE_POLL_MS)
  }
  return false
}

async function loadStorageBoxes(client) {
  const { rows } = await client.query(
    `SELECT id, hostname, username, "sshPort", "sshKeyPath",
            "knownHostsPath", "basePath", "capacityBytes", "maxConnections",
            priority, "inactivityDays", "maxUsagePercent", "archiveConcurrency",
            "restoreConcurrency", "scanIntervalSeconds", "isActive",
            "acceptingUploads", "minimumFreeBytes"
     FROM storage_boxes
     WHERE "isActive"
     ORDER BY priority, id`
  )
  if (rows.length === 0) throw new Error('no active Storage Box is configured')
  return rows.map(box => {
    if (!SAFE_COMPONENT.test(box.username) || !/^[-A-Za-z0-9.]+$/.test(box.hostname)) throw new Error(`unsafe connection details for Storage Box ${box.id}`)
    if (!box.sshKeyPath || !box.knownHostsPath) throw new Error(`SSH key paths are not configured for Storage Box ${box.id}`)
    box.basePath = assertSafeRemotePath(box.basePath)
    box.inactivityDays = Number(box.inactivityDays)
    box.archiveConcurrency = Number(box.archiveConcurrency)
    box.restoreConcurrency = Number(box.restoreConcurrency)
    box.scanIntervalMs = Number(box.scanIntervalSeconds) * 1000
    box.minimumFreeBytes = Number(box.minimumFreeBytes || DEFAULT_MINIMUM_FREE_BYTES)
    if (box.archiveConcurrency + box.restoreConcurrency > box.maxConnections) throw new Error(`connection policy exceeds the limit for Storage Box ${box.id}`)
    return box
  })
}

async function refreshStorageBoxUsage(client, boxes) {
  const checks = await Promise.all(boxes.map(async box => {
    try {
      return { box, usage: await remoteUsage(box), error: null }
    } catch (error) {
      return { box, usage: null, error }
    }
  }))
  const refreshed = []
  for (const check of checks) {
    const { box, usage, error } = check
    if (error) {
      log(`box ${box.id}: usage check failed; skipping its upload lane this round: ${error.message}`)
      refreshed.push({ ...box, usage: null, reachable: false })
      continue
    }
    const remaining = Math.max(0, usage.total - usage.used)
    const acceptingUploads = remaining > box.minimumFreeBytes
    const update = await client.query(
      `UPDATE storage_boxes
       SET "capacityBytes" = $2, "usedBytes" = $3, "acceptingUploads" = $4,
           "updatedAt" = NOW()
       WHERE id = $1
       RETURNING "acceptingUploads"`,
      [box.id, usage.total, usage.used, acceptingUploads]
    )
    if (Boolean(box.acceptingUploads) !== acceptingUploads) {
      log(`box ${box.id}: upload lane ${acceptingUploads ? 'AUTO-ACTIVATED' : 'AUTO-DISABLED'}; ${formatBytes(remaining)} free (threshold ${formatBytes(box.minimumFreeBytes)})`)
    }
    refreshed.push({
      ...box,
      acceptingUploads: Boolean(update.rows[0]?.acceptingUploads),
      usage,
      remaining,
      reachable: true,
    })
  }
  return refreshed
}

async function recoverStaleOperations(client, storageId) {
  const { rowCount } = await client.query(
    `UPDATE "Video"
     SET "coldStorageStatus" = 'failed',
         "coldStorageRetryAt" = NOW(),
         "coldStorageUpdatedAt" = NOW(),
         "coldStorageError" = 'recovered stale cold-storage operation after worker restart'
     WHERE "storageId" = $1
       AND "coldStorageStatus" IN ('archiving', 'purging')`,
    [storageId]
  )
  if (rowCount > 0) log(`recovered ${rowCount} stale archive/purge operation(s)`)
}

async function recoverStaleStaging(client, storageId) {
  const { rows } = await client.query(
    `SELECT s."videoId", s."stagePath", s.bytes AS "stageBytes",
            s.files AS "stageFiles", s.checksum AS "stageChecksum",
            v."coldStorageStatus", v."coldStorageId"
     FROM cold_storage_staging s
     JOIN "Video" v ON v.id = s."videoId"
     WHERE s."storageId" = $1 AND s.status = 'uploading'`,
    [storageId]
  )
  let reset = 0
  let drained = 0
  for (const row of rows) {
    if (row.coldStorageStatus === 'cold' && row.coldStorageId) {
      await rm(row.stagePath, { recursive: true, force: true, maxRetries: 3, retryDelay: 1000 }).catch(() => {})
      await client.query(`DELETE FROM cold_storage_staging WHERE "videoId" = $1`, [row.videoId])
      drained++
      continue
    }
    await resetStagedCandidate(client, {
      id: row.videoId,
      stagePath: row.stagePath,
      stageBytes: row.stageBytes,
      stageChecksum: row.stageChecksum,
    }, new Error('recovered interrupted staged upload'))
    reset++
  }
  if (reset > 0 || drained > 0) log(`recovered staged uploads: ${reset} reset, ${drained} already-remote duplicate(s) removed`)
}

async function dryRun(client, storageId, box, options) {
  const limit = options.limit || 100
  const { rows } = await client.query(
    `SELECT v.id, v."userId", v."encodedPath", v."encodedSize", v."lastView", v."createdAt"
     FROM "Video" v WHERE ${ELIGIBLE_SQL}
     ORDER BY NULLIF(v."encodedSize", 0) ASC NULLS LAST,
              COALESCE(v."lastView", v."createdAt"), v.id LIMIT $3`,
    [storageId, box.inactivityDays, limit]
  )
  const totals = await client.query(
    `SELECT count(*)::int AS videos, COALESCE(sum(v."encodedSize"), 0)::text AS bytes
     FROM "Video" v WHERE ${ELIGIBLE_SQL}`,
    [storageId, box.inactivityDays]
  )
  log(`DRY RUN: ${totals.rows[0].videos} eligible clone-free videos after ${box.inactivityDays} days (${formatBytes(totals.rows[0].bytes)} estimated)`)
  log(`DRY RUN: showing ${rows.length}; no database, remote, or filesystem writes were made`)
  if (options.verbose) {
    for (const row of rows) log(`${row.id} ${formatBytes(row.encodedSize)} inactive since ${row.lastView || row.createdAt}`)
  }
}

async function fetchStagedBatch(client, storageId, limit) {
  const { rows } = await client.query(
    `SELECT s."videoId" AS id, s.bytes AS "encodedSize"
     FROM cold_storage_staging s
     JOIN "Video" v ON v.id = s."videoId"
     WHERE s."storageId" = $1
       AND s.status = 'ready'
       AND (s."retryAt" IS NULL OR s."retryAt" <= NOW())
       AND v.status = 'active'
       AND v."coldStorage"
       AND v."coldStorageStatus" = 'staged'
     ORDER BY s."readyAt", s."videoId"
     LIMIT $2`,
    [storageId, limit]
  )
  return {
    ids: rows.map(row => row.id),
    candidates: rows.map(row => ({ id: row.id, bytes: Number(row.encodedSize) })),
    label: 'locally staged',
  }
}

async function fetchParallelCandidates(client, storageId, inactivityDays, limit) {
  const { rows } = await client.query(
    `SELECT v.id, v."encodedSize" FROM "Video" v WHERE ${ELIGIBLE_SQL}
     ORDER BY NULLIF(v."encodedSize", 0) ASC NULLS LAST,
              COALESCE(v."lastView", v."createdAt"), v.id LIMIT $3`,
    [storageId, inactivityDays, limit]
  )
  return rows.map(row => ({ id: row.id, bytes: Number(row.encodedSize) }))
}

async function claimCandidate(client, storageId, inactivityDays, videoId) {
  const { rows } = await client.query(
    `UPDATE "Video" v
     SET "coldStorageStatus" = 'archiving',
         "coldStorageUpdatedAt" = NOW(),
         "coldStorageRetryAt" = NULL,
         "coldStorageError" = NULL
     WHERE v.id = $3 AND ${ELIGIBLE_SQL}
     RETURNING v.id, v."userId", v.status, v."encodingStatus", v."encodedPath",
               v."encodedSize", v."lastView", v."createdAt"`,
    [storageId, inactivityDays, videoId]
  )
  return rows[0] || null
}

async function claimStagedCandidate(client, storageId, videoId) {
  await client.query('BEGIN')
  try {
    const { rows } = await client.query(
      `SELECT v.id, v."userId", v.status, v."encodingStatus", v."encodedPath",
              v."encodedSize", v."lastView", v."createdAt",
              s."stagePath", s.bytes AS "stageBytes", s.files AS "stageFiles",
              s.checksum AS "stageChecksum"
       FROM cold_storage_staging s
       JOIN "Video" v ON v.id = s."videoId"
       WHERE s."videoId" = $2
         AND s."storageId" = $1
         AND s.status = 'ready'
         AND v."storageId" = $1
         AND v.status = 'active'
         AND v."coldStorage"
         AND v."coldStorageStatus" = 'staged'
       FOR UPDATE OF s, v`,
      [storageId, videoId]
    )
    if (rows.length !== 1) {
      await client.query('ROLLBACK')
      return null
    }
    await client.query(
      `UPDATE cold_storage_staging
       SET status = 'uploading', error = NULL, "updatedAt" = NOW()
       WHERE "videoId" = $1`,
      [videoId]
    )
    await client.query(
      `UPDATE "Video"
       SET "coldStorageStatus" = 'archiving', "coldStorageUpdatedAt" = NOW(),
           "coldStorageRetryAt" = NULL, "coldStorageError" = NULL
       WHERE id = $1`,
      [videoId]
    )
    await client.query('COMMIT')
    return { ...rows[0], staged: true }
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {})
    throw error
  }
}

async function resolveStagedDirectory(stagePath) {
  const root = await realpath(STAGE_ROOT)
  const expected = path.resolve(String(stagePath || ''))
  if (expected === root || !expected.startsWith(`${root}${path.sep}`)) throw new Error('staged path escapes staging root')
  const actual = await realpath(expected)
  if (actual !== expected || !actual.startsWith(`${root}${path.sep}`)) throw new Error('staged path is a symlink or escapes staging root')
  const info = await lstat(actual)
  if (!info.isDirectory() || info.isSymbolicLink()) throw new Error('staged path is not a safe directory')
  return actual
}

async function resetStagedCandidate(client, video, error = null) {
  const message = error ? String(error.message || error).slice(0, 2000) : null
  await client.query('BEGIN')
  try {
    const current = await client.query(
      `SELECT status, "restoreRequestedAt" FROM "Video" WHERE id = $1 FOR UPDATE`,
      [video.id]
    )
    const restoreRequested = current.rows[0]?.status === 'active' && Boolean(current.rows[0]?.restoreRequestedAt)
    await client.query(
      `UPDATE cold_storage_staging
       SET status = $4, error = $2, "retryAt" = CASE WHEN $2::text IS NULL OR $4 = 'restoring' THEN NULL ELSE NOW() + make_interval(mins => $3) END,
           "updatedAt" = NOW()
       WHERE "videoId" = $1`,
      [video.id, message, RETRY_MINUTES, restoreRequested ? 'restoring' : 'ready']
    )
    await client.query(
      `UPDATE "Video"
       SET "coldStorage" = true, "coldStorageStatus" = $6,
           "coldStorageId" = NULL, "coldStoragePath" = $2,
           "coldStorageChecksum" = $3, "coldStorageBytes" = $4,
           "coldStorageUpdatedAt" = NOW(), "coldStorageRetryAt" = NULL,
           "coldStorageError" = $5
       WHERE id = $1`,
      [video.id, video.stagePath, video.stageChecksum, Number(video.stageBytes || 0), message, restoreRequested ? 'restoring' : 'staged']
    )
    await client.query('COMMIT')
  } catch (resetError) {
    await client.query('ROLLBACK').catch(() => {})
    throw resetError
  }
}

async function removeCompletedStage(client, video) {
  await rm(video.stagePath, { recursive: true, force: false, maxRetries: 3, retryDelay: 1000 })
  await client.query(`DELETE FROM cold_storage_staging WHERE "videoId" = $1`, [video.id])
}

async function markFailure(client, videoId, error) {
  await client.query(
    `UPDATE "Video"
     SET "coldStorageStatus" = 'failed',
         "coldStorageUpdatedAt" = NOW(),
         "coldStorageRetryAt" = NOW() + make_interval(mins => $2),
         "coldStorageError" = $3
     WHERE id = $1 AND "coldStorageStatus" = 'archiving'`,
    [videoId, RETRY_MINUTES, String(error.message || error).slice(0, 2000)]
  )
}

async function markMissingSource(client, video, error) {
  const audit = await client.query(
    `SELECT reason, "deleteType", "createdAt"
     FROM deleted_videos_logs
     WHERE "videoId" = $1
     ORDER BY "createdAt" DESC
     LIMIT 1`,
    [video.id]
  )
  const latest = audit.rows[0]
  const auditDetail = latest
    ? `; latest deletion audit: ${latest.deleteType}/${latest.reason} at ${new Date(latest.createdAt).toISOString()}`
    : '; no deletion audit found'
  const message = `source directory missing while DB is ${video.status}/${video.encodingStatus}${auditDetail}; ${error.message}`
  await client.query(
    `UPDATE "Video"
     SET "coldStorageStatus" = 'failed',
         "coldStorageUpdatedAt" = NOW(),
         "coldStorageRetryAt" = NOW() + make_interval(mins => $2),
         "coldStorageError" = $3
     WHERE id = $1 AND "coldStorageStatus" = 'archiving'`,
    [video.id, MISSING_SOURCE_RETRY_MINUTES, message.slice(0, 2000)]
  )
  return latest
}

async function reserveBoxCapacity(client, videoId, box, bytes) {
  await client.query('BEGIN')
  try {
    const { rows } = await client.query(
      `SELECT "usedBytes", "capacityBytes", "minimumFreeBytes"
       FROM storage_boxes
       WHERE id = $1 AND "isActive" AND "acceptingUploads"
       FOR UPDATE`,
      [box.id]
    )
    if (rows.length !== 1) throw new Error(`Storage Box ${box.id} is not accepting uploads`)
    const reserved = await client.query(
      `SELECT COALESCE(sum("coldStorageBytes"), 0)::text AS bytes
       FROM "Video"
       WHERE "coldStorageId" = $1 AND "coldStorageStatus" = 'archiving' AND id <> $2`,
      [box.id, videoId]
    )
    const maximum = Number(rows[0].capacityBytes) - Number(rows[0].minimumFreeBytes)
    if (Number(rows[0].usedBytes) + Number(reserved.rows[0].bytes) + bytes > maximum) {
      await client.query('ROLLBACK')
      return false
    }
    await client.query(
      `UPDATE "Video"
       SET "coldStorageId" = $2, "coldStorageBytes" = $3, "coldStorageUpdatedAt" = NOW()
       WHERE id = $1 AND "coldStorageStatus" = 'archiving'`,
      [videoId, box.id, bytes]
    )
    await client.query('COMMIT')
    return true
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {})
    throw error
  }
}

async function acquireArchiveSlot(client, box) {
  while (!shuttingDown) {
    for (let slot = 1; slot <= box.archiveConcurrency; slot++) {
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

async function releaseArchiveSlot(client, box, slot) {
  if (slot === null) return
  await client.query(
    `SELECT pg_advisory_unlock(hashtext($1), $2)`,
    [`storage-box-connections:${box.id}`, slot]
  ).catch(() => {})
}

async function claimTerminalColdVideo(client, storageId) {
  const { rows } = await client.query(
    `WITH candidate AS (
       SELECT id FROM "Video"
       WHERE "storageId" = $1
         AND status IN ('removed', 'hard_deleted')
         AND "coldStorage"
         AND "coldStorageStatus" IN ('cold', 'failed')
         AND ("coldStorageRetryAt" IS NULL OR "coldStorageRetryAt" <= NOW())
         AND "coldStorageId" IS NOT NULL
         AND "coldStoragePath" IS NOT NULL
       ORDER BY "coldStorageUpdatedAt" NULLS FIRST, id
       LIMIT 1
       FOR UPDATE SKIP LOCKED
     )
     UPDATE "Video" v
     SET "coldStorageStatus" = 'purging', "coldStorageUpdatedAt" = NOW(), "coldStorageError" = NULL
     FROM candidate WHERE v.id = candidate.id
     RETURNING v.id, v."coldStorageId", v."coldStoragePath", v."coldStorageBytes"`,
    [storageId]
  )
  return rows[0] || null
}

async function purgeTerminalColdVideo(pool, storageId, boxesById) {
  const client = await pool.connect()
  let video = null
  let box = null
  let slot = null
  try {
    await client.query('BEGIN')
    video = await claimTerminalColdVideo(client, storageId)
    if (!video) {
      await client.query('COMMIT')
      return false
    }
    box = boxesById.get(video.coldStorageId)
    if (!box) throw new Error(`Storage Box ${video.coldStorageId} is unavailable for terminal purge`)
    slot = await acquireArchiveSlot(client, box)
    await removeRemoteArchive(box, video.coldStoragePath)
    const update = await client.query(
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
       WHERE id = $1 AND status IN ('removed', 'hard_deleted') AND "coldStorageStatus" = 'purging'
       RETURNING id`,
      [video.id]
    )
    if (update.rowCount !== 1) throw new Error('terminal video changed while its cold copy was being purged')
    await client.query(
      `UPDATE storage_boxes
       SET "usedBytes" = GREATEST(0, "usedBytes" - $2),
           "acceptingUploads" = (
             "capacityBytes" - GREATEST(0, "usedBytes" - $2) > "minimumFreeBytes"
           ),
           "updatedAt" = NOW()
       WHERE id = $1`,
      [box.id, Number(video.coldStorageBytes || 0)]
    )
    await client.query('COMMIT')
    log(`${video.id}: purged temporary cold copy after permanent deletion`)
    return true
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {})
    if (video) {
      await client.query(
        `UPDATE "Video"
         SET "coldStorageStatus" = 'failed', "coldStorageUpdatedAt" = NOW(),
             "coldStorageRetryAt" = NOW() + make_interval(mins => $2), "coldStorageError" = $3
         WHERE id = $1 AND "coldStorageStatus" = 'purging'`,
        [video.id, RETRY_MINUTES, String(error.message || error).slice(0, 2000)]
      ).catch(() => {})
    }
    log(`terminal purge ERROR${video ? ` ${video.id}` : ''}: ${error.message}`)
    return false
  } finally {
    if (box) await releaseArchiveSlot(client, box, slot)
    client.release()
  }
}

async function finalizeDatabase(client, storageId, video, box, remotePath, inspection) {
  await client.query('BEGIN')
  try {
    const { rows } = await client.query(`SELECT * FROM "Video" WHERE id = $1 FOR UPDATE`, [video.id])
    const current = rows[0]
    if (!current) throw new Error('video disappeared during archive')
    const conflicts = await client.query(
      `SELECT EXISTS (
         SELECT 1 FROM "Video" other
         WHERE other.id <> $1 AND (
           other."clonedFrom" = $1
           OR ($2::text <> '' AND other."encodedPath" = $2)
         )
       ) AS conflict`,
      [video.id, current.encodedPath || '']
    )
    const activityAt = new Date(current.lastView || current.createdAt).getTime()
    const cutoff = Date.now() - box.inactivityDays * 86_400_000
    const stillEligible = current.storageId === storageId
      && current.status === 'active'
      && current.encodingStatus === 'completed'
      && current.coldStorageStatus === 'archiving'
      && !current.clonedFrom
      && !current.hasCopies
      && !conflicts.rows[0].conflict
      && Number.isFinite(activityAt)
      && activityAt <= cutoff

    if (!stillEligible) {
      const terminal = ['removed', 'hard_deleted'].includes(current.status)
      await client.query(
        `UPDATE "Video"
         SET "coldStorage" = $6,
             "coldStorageStatus" = CASE WHEN $6 THEN 'cold' ELSE 'local' END,
             "coldStorageUpdatedAt" = NOW(),
             "coldStorageId" = $2, "coldStoragePath" = $3,
             "coldStorageChecksum" = $4, "coldStorageBytes" = $5,
             "coldStorageError" = NULL
         WHERE id = $1`,
        [video.id, box.id, remotePath, inspection.checksum, inspection.totalBytes, terminal]
      )
      await client.query('COMMIT')
      return terminal ? 'terminal' : 'retained'
    }

    await client.query(
      `UPDATE "Video"
       SET "coldStorage" = true,
           "coldStorageStatus" = 'cold',
           "coldStorageId" = $2,
           "coldStoragePath" = $3,
           "coldStorageChecksum" = $4,
           "coldStorageBytes" = $5,
           "archivedAt" = NOW(),
           "restoreRequestedAt" = NULL,
           "coldStorageUpdatedAt" = NOW(),
           "coldStorageRetryAt" = NULL,
           "coldStorageError" = NULL
       WHERE id = $1`,
      [video.id, box.id, remotePath, inspection.checksum, inspection.totalBytes]
    )
    await client.query('COMMIT')
    return 'archived'
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {})
    throw error
  }
}

async function reclaimLocalDirectory(client, sourceDir, videoId) {
  const ingestRoot = await realpath(INGEST_DIR)
  await mkdir(DELETE_ROOT, { recursive: true, mode: 0o750 })
  const deleteRoot = await realpath(DELETE_ROOT)
  const [ingestInfo, deleteInfo] = await Promise.all([stat(ingestRoot), stat(deleteRoot)])
  if (ingestInfo.dev !== deleteInfo.dev) throw new Error('cold delete directory must share the INGEST_DIR filesystem')
  if (deleteRoot === ingestRoot || deleteRoot.startsWith(`${ingestRoot}${path.sep}`)) throw new Error('cold delete directory must be outside INGEST_DIR')
  const target = path.join(deleteRoot, assertSafeComponent(videoId, 'video ID'))
  try {
    await lstat(target)
    throw new Error('cold delete target already exists')
  } catch (error) {
    if (error.code !== 'ENOENT') throw error
  }
  let moved = false
  await client.query('BEGIN')
  try {
    const { rows } = await client.query(
      `SELECT status, "coldStorage", "coldStorageStatus" FROM "Video" WHERE id = $1 FOR UPDATE`,
      [videoId]
    )
    const current = rows[0]
    if (!current
      || current.status !== 'active'
      || !current.coldStorage
      || current.coldStorageStatus !== 'cold') {
      await client.query('COMMIT')
      return false
    }
    await rename(sourceDir, target)
    moved = true
    await client.query('COMMIT')
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {})
    if (moved) await rename(target, sourceDir).catch(() => {})
    throw error
  }
  await rm(target, { recursive: true, force: false, maxRetries: 3, retryDelay: 1000 })
  return true
}

async function processCandidate(pool, storageId, box, videoId, capacity) {
  const client = await pool.connect()
  let video
  let reserved = 0
  let slot = null
  try {
    video = await claimStagedCandidate(client, storageId, videoId)
      || await claimCandidate(client, storageId, box.inactivityDays, videoId)
    if (!video) return { action: 'stale' }
    const sourceDir = video.staged
      ? await resolveStagedDirectory(video.stagePath)
      : await resolveAssetDirectory(video)
    const inspection = await inspectAndHashForArchive(sourceDir, video.id)
    if (video.staged && (
      inspection.totalBytes !== Number(video.stageBytes)
      || inspection.files !== Number(video.stageFiles)
    )) throw new Error('staged source no longer matches its verified manifest')
    if (capacity.used + capacity.reserved + inspection.totalBytes > capacity.maximum) {
      const error = new Error(`Storage Box minimum-free reserve of ${formatBytes(box.minimumFreeBytes)} would be exceeded`)
      if (video.staged) await resetStagedCandidate(client, video, error)
      else await markFailure(client, video.id, error)
      return { action: 'capacity_skip', bytes: inspection.totalBytes }
    }
    if (!await reserveBoxCapacity(client, video.id, box, inspection.totalBytes)) {
      const error = new Error('Storage Box capacity is currently reserved by other workers')
      if (video.staged) await resetStagedCandidate(client, video, error)
      else await markFailure(client, video.id, error)
      return { action: 'capacity_skip', bytes: inspection.totalBytes }
    }
    capacity.reserved += inspection.totalBytes
    reserved = inspection.totalBytes
    slot = await acquireArchiveSlot(client, box)

    const userId = assertSafeComponent(video.userId, 'user ID')
    const safeVideoId = assertSafeComponent(video.id, 'video ID')
    const finalPath = `${box.basePath}/videos/${userId}/${safeVideoId}`
    const incomingPath = `${box.basePath}/incoming/${safeVideoId}`

    if (await remoteExists(box, finalPath)) {
      await verifyRemoteDirectory(box, sourceDir, finalPath)
    } else {
      await uploadDirectory(box, sourceDir, incomingPath, video.id, inspection.totalBytes)
      await verifyRemoteDirectory(box, sourceDir, incomingPath)
      await finalizeRemote(box, incomingPath, finalPath)
      if (!await remoteExists(box, finalPath)) throw new Error('remote final path is not visible after atomic move')
    }

    const outcome = await finalizeDatabase(client, storageId, video, box, finalPath, inspection)
    capacity.reserved -= inspection.totalBytes
    reserved = 0
    if (outcome === 'retained') {
      await removeRemoteArchive(box, finalPath)
      if (video.staged) {
        await resetStagedCandidate(client, video)
        log(`${video.id}: became ineligible or shared during upload; retained in local staging`)
      } else {
        await client.query(
          `UPDATE "Video"
           SET "coldStorageId" = NULL, "coldStoragePath" = NULL,
               "coldStorageChecksum" = NULL, "coldStorageBytes" = NULL,
               "coldStorageUpdatedAt" = NOW()
           WHERE id = $1 AND NOT "coldStorage" AND "coldStorageStatus" = 'local' AND "coldStoragePath" = $2`,
          [video.id, finalPath]
        )
        log(`${video.id}: became ineligible or shared during upload; retained locally`)
      }
      return { action: 'retained', bytes: inspection.totalBytes }
    }
    capacity.used += inspection.totalBytes
    if (outcome === 'terminal') {
      if (video.staged) {
        await removeCompletedStage(client, video).catch(error => {
          log(`${video.id}: remote archive is safe but staged duplicate cleanup failed: ${error.message}`)
        })
      }
      log(`${video.id}: became permanently deleted during upload; queued remote purge`)
      return { action: 'terminal', bytes: inspection.totalBytes }
    }

    if (video.staged) {
      await removeCompletedStage(client, video).catch(error => {
        log(`${video.id}: remote archive is safe but staged duplicate cleanup failed: ${error.message}`)
      })
    } else {
      const reclaimed = await reclaimLocalDirectory(client, sourceDir, video.id)
      if (!reclaimed) {
        log(`${video.id}: state changed before local reclamation; local directory retained`)
        return { action: 'retained', bytes: inspection.totalBytes }
      }
    }
    await client.query(
      `UPDATE storage_boxes
       SET "usedBytes" = "usedBytes" + $2,
           "acceptingUploads" = (
             "capacityBytes" - ("usedBytes" + $2) > "minimumFreeBytes"
           ),
           "updatedAt" = NOW()
       WHERE id = $1`,
      [box.id, inspection.totalBytes]
    )
    log(`${video.id}: archived and ${video.staged ? 'drained local staging' : 'freed local storage'} ${formatBytes(inspection.totalBytes)} (${inspection.files} files)`)
    return { action: 'archived', bytes: inspection.totalBytes }
  } catch (error) {
    if (reserved > 0) capacity.reserved -= reserved
    if (video && !video.staged && error?.code === 'ENOENT') {
      const audit = await markMissingSource(client, video, error).catch(() => null)
      const auditText = audit ? `; audit=${audit.deleteType}/${audit.reason}` : ''
      log(`${videoId}: SKIP missing local source; DB=${video.status}/${video.encodingStatus}${auditText}; retry in 24h`)
      return { action: 'missing', error }
    }
    if (video?.staged) await resetStagedCandidate(client, video, error).catch(() => {})
    else if (video) await markFailure(client, video.id, error).catch(() => {})
    log(`${videoId}: ERROR ${error.message}`)
    return { action: 'error', error }
  } finally {
    await releaseArchiveSlot(client, box, slot)
    client.release()
  }
}

async function buildUploadLanes(client, boxes) {
  const lanes = []
  for (const box of boxes) {
    if (!box.reachable || !box.acceptingUploads || !box.usage) continue
    const reservation = await client.query(
      `SELECT COALESCE(sum("coldStorageBytes"), 0)::text AS bytes
       FROM "Video" WHERE "coldStorageId" = $1 AND "coldStorageStatus" = 'archiving'`,
      [box.id]
    )
    const globallyReserved = Number(reservation.rows[0].bytes)
    const maximum = box.usage.total - box.minimumFreeBytes
    const available = maximum - box.usage.used - globallyReserved
    if (available <= 0) continue
    lanes.push({
      box,
      available,
      capacity: {
        used: box.usage.used + globallyReserved,
        reserved: 0,
        maximum,
      },
    })
  }
  return lanes
}

function assignOneCandidatePerBox(candidates, lanes, maximum) {
  const remainingLanes = [...lanes]
  const assignments = []
  for (const candidate of candidates) {
    if (assignments.length >= maximum || remainingLanes.length === 0) break
    const bytes = Number(candidate.bytes)
    const laneIndex = remainingLanes.findIndex(lane =>
      !Number.isFinite(bytes) || bytes <= 0 || bytes <= lane.available
    )
    if (laneIndex === -1) continue
    const [lane] = remainingLanes.splice(laneIndex, 1)
    assignments.push({ ...lane, id: candidate.id })
  }
  return assignments
}

async function main() {
  const options = parseArgs(process.argv.slice(2))
  if (options.help) return usage()
  if (!DATABASE_URL) throw new Error('DATABASE_URL is not configured')

  const pool = new pg.Pool({ connectionString: DATABASE_URL, max: 12, application_name: 'cold-storage-transfer' })
  const control = await pool.connect()
  let lockHeld = false
  let lockName = null
  try {
    const storageId = await localStorageId(control)
    lockName = `cold-storage-transfer:${storageId}`
    const lock = await control.query(`SELECT pg_try_advisory_lock(hashtext($1)) AS acquired`, [lockName])
    if (!lock.rows[0].acquired) throw new Error('another cold-storage transfer worker is active')
    lockHeld = true
    let boxes = await loadStorageBoxes(control)

    if (!options.execute) return await dryRun(control, storageId, boxes[0], options)
    await recoverStaleStaging(control, storageId)
    await recoverStaleOperations(control, storageId)
    boxes = await refreshStorageBoxUsage(control, boxes)
    for (const box of boxes) {
      if (!box.usage) continue
      log(`box ${box.id}: ${box.hostname}; usage ${formatBytes(box.usage.used)}/${formatBytes(box.usage.total)} (${(box.usage.ratio * 100).toFixed(2)}%); ${formatBytes(box.remaining)} free`)
      log(`box ${box.id}: ${box.inactivityDays} inactive days; ${box.archiveConcurrency} archive + ${box.restoreConcurrency} restore connections`)
      log(`box ${box.id}: one parallel upload lane; auto-disables uploads at ${formatBytes(box.minimumFreeBytes)} free`)
    }

    let handled = 0
    while (!shuttingDown && (options.limit === null || handled < options.limit)) {
      boxes = await loadStorageBoxes(control)
      boxes = await refreshStorageBoxUsage(control, boxes)
      const boxesById = new Map(boxes.map(box => [box.id, box]))

      // Permanent policy deletions always get a chance to release remote space,
      // even while the emergency local staging queue remains busy.
      if (await purgeTerminalColdVideo(pool, storageId, boxesById)) continue

      const lanes = await buildUploadLanes(control, boxes)
      if (lanes.length === 0) {
        log(`no reachable Storage Box has more than ${formatBytes(DEFAULT_MINIMUM_FREE_BYTES)} free; rechecking automatically`)
        if (options.once) break
        await sleep(Math.min(...boxes.map(box => box.scanIntervalMs)))
        continue
      }
      const remainingLimit = options.limit === null
        ? lanes.length
        : Math.min(lanes.length, options.limit - handled)
      let stagedBatch = options.videoId
        ? { ids: [], candidates: [], label: 'empty' }
        : await fetchStagedBatch(control, storageId, remainingLimit)
      let hasStagedBatch = stagedBatch.ids.length > 0
      if (!hasStagedBatch) {
        if (!await waitForIngestCapacity(control, storageId)) break
        stagedBatch = options.videoId
          ? { ids: [], candidates: [], label: 'empty' }
          : await fetchStagedBatch(control, storageId, remainingLimit)
        hasStagedBatch = stagedBatch.ids.length > 0
      }
      // Staged uploads drain the emergency root buffer and bypass the ingest queue
      // gate. Normal /home archives still yield to production transfers.
      if (!hasStagedBatch && !await waitForIngestCapacity(control, storageId)) break
      let candidates
      let label
      if (options.videoId && handled === 0) {
        candidates = [{ id: options.videoId, bytes: 0 }]
        label = 'explicit video'
      } else if (hasStagedBatch) {
        candidates = stagedBatch.candidates
        label = stagedBatch.label
      } else {
        const inactivityDays = Math.max(...lanes.map(lane => lane.box.inactivityDays))
        candidates = await fetchParallelCandidates(control, storageId, inactivityDays, remainingLimit * 4)
        label = 'eligible local'
      }
      const assignments = assignOneCandidatePerBox(candidates, lanes, remainingLimit)
      if (assignments.length === 0) {
        log('no eligible videos found')
        if (options.once) break
        await sleep(Math.min(...boxes.map(box => box.scanIntervalMs)))
        continue
      }
      for (const assignment of assignments) {
        log(`box ${assignment.box.id}: selected ${assignment.id} (${label}); one lane for this box`)
      }
      const batchStartedAt = Date.now()
      const results = await Promise.all(assignments.map(async assignment => ({
        boxId: assignment.box.id,
        ...(await processCandidate(pool, storageId, assignment.box, assignment.id, assignment.capacity)),
      })))
      handled += results.length
      const archived = results.filter(result => result.action === 'archived')
      const freed = archived.reduce((sum, result) => sum + result.bytes, 0)
      const batchElapsedMs = Math.max(Date.now() - batchStartedAt, 1)
      const batchRate = freed > 0 ? `; effective ${formatRate(freed / (batchElapsedMs / 1000))}` : ''
      const byteAction = hasStagedBatch ? 'drained from staging' : 'freed from /home'
      log(`multi-box batch complete: ${archived.length}/${results.length} archived across ${assignments.length} box lane(s); ${formatBytes(freed)} ${byteAction} in ${formatDuration(batchElapsedMs)}${batchRate}; ${handled} handled this run`)
      if (options.once) break
    }
  } finally {
    if (lockHeld) await control.query(`SELECT pg_advisory_unlock(hashtext($1))`, [lockName]).catch(() => {})
    control.release()
    await pool.end()
  }
}

process.on('SIGINT', () => { shuttingDown = true })
process.on('SIGTERM', () => { shuttingDown = true })

main().catch(error => {
  console.error(`[cold-transfer] ERROR: ${error.message}`)
  process.exitCode = 1
})
