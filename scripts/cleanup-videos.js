import {
  appendFileSync,
  closeSync,
  existsSync,
  fsyncSync,
  lstatSync,
  mkdirSync,
  openSync,
  readFileSync,
  readdirSync,
  realpathSync,
  renameSync,
  statSync,
} from 'node:fs'
import path from 'node:path'
import { execSync } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import pg from 'pg'
import { createClient as createRedisClient } from 'redis'
import { INGEST_DIR } from '../config.js'

const DATABASE_URL = process.env.DATABASE_URL
const DEFAULT_BATCH_SIZE = 100
const DMCA_HOLD_SECONDS = 48 * 60 * 60
const ADVISORY_LOCK_KEY = 1_947_323_821
const INACTIVITY_STATUSES = new Set(['active'])
const TERMINAL_STATUSES = new Set(['offline', 'removed', 'hard_deleted'])
const TRASH_ROOT = process.env.TRASH_DIR || path.join(path.dirname(path.resolve(INGEST_DIR)), 'trash')
const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379'
const REDIS_CACHE_TTL_SECONDS = 90 * 24 * 60 * 60
const REDIS_CACHE_VERSION = 'v1'

function usage() {
  console.log(`Usage: node scripts/cleanup-videos.js [options]

Tier-aware cleanup for inactive, offline, removed, and hard-deleted videos.
Dry-run is the default. Execute mode moves unreferenced assets to recoverable
trash and updates eligible database rows to hard_deleted.

Options:
  --batch-size N   Eligible rows fetched per batch (default: ${DEFAULT_BATCH_SIZE}, max: 500)
  --limit N        Stop after inspecting N eligible candidate rows
  --verbose        Print one decision per candidate
  --nocache        Bypass the Redis confirmed-deletion cache
  --execute        Move eligible assets and update guarded database rows
  --scheduled      Run only during the configured 00:00/12:00 minute
  --help           Show this help
`)
}

function parsePositiveInt(value, name) {
  const parsed = Number.parseInt(value, 10)
  if (!Number.isSafeInteger(parsed) || parsed <= 0) throw new Error(`${name} must be a positive integer`)
  return parsed
}

function parseArgs(argv) {
  const options = { batchSize: DEFAULT_BATCH_SIZE, limit: null, verbose: false, noCache: false, execute: false, scheduled: false }
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]
    if (arg === '--help') options.help = true
    else if (arg === '--verbose') options.verbose = true
    else if (arg === '--nocache') options.noCache = true
    else if (arg === '--execute') options.execute = true
    else if (arg === '--scheduled') options.scheduled = true
    else if (arg === '--batch-size') options.batchSize = parsePositiveInt(argv[++i], '--batch-size')
    else if (arg === '--limit') options.limit = parsePositiveInt(argv[++i], '--limit')
    else throw new Error(`Unknown option: ${arg}`)
  }
  options.batchSize = Math.min(options.batchSize, 500)
  return options
}

function getLocalIp() {
  return execSync('hostname -I', { encoding: 'utf8' }).trim().split(/\s+/)[0]
}

function formatBytes(bytes) {
  if (!Number.isFinite(bytes) || bytes <= 0) return '0B'
  if (bytes < 1024) return `${bytes}B`
  if (bytes < 1024 ** 2) return `${(bytes / 1024).toFixed(1)}KB`
  if (bytes < 1024 ** 3) return `${(bytes / 1024 ** 2).toFixed(1)}MB`
  return `${(bytes / 1024 ** 3).toFixed(2)}GB`
}

function normalizeStatus(value) {
  return String(value || '').trim().toLowerCase()
}

function isoTimestamp(value) {
  const date = value instanceof Date ? value : new Date(value)
  return Number.isFinite(date.getTime()) ? date.toISOString() : String(value || '')
}

function cacheMember(video) {
  return `${video.id}:${isoTimestamp(video.updatedAt)}`
}

async function connectRedis(label) {
  const client = createRedisClient({
    url: REDIS_URL,
    socket: { connectTimeout: 1_500, reconnectStrategy: false },
  })
  let lastError = null
  client.on('error', err => { lastError = err })
  try {
    await client.connect()
    console.log(`[${label}] Redis confirmed-deletion cache connected`)
    return client
  } catch (err) {
    console.warn(`[${label}] Redis unavailable; continuing safely without cache (${lastError?.message || err.message})`)
    try { client.destroy() } catch {}
    return null
  }
}

async function excludeRedisConfirmed(redis, cacheKey, candidates) {
  if (!redis || candidates.length === 0) return { candidates, skipped: 0 }
  try {
    // Only terminal DB rows may be skipped. If a video is ever restored to
    // active/offline/removed, it is reconsidered even if updatedAt was not bumped.
    const terminal = candidates.filter(video => normalizeStatus(video.status) === 'hard_deleted')
    if (terminal.length === 0) return { candidates, skipped: 0 }
    const hits = await redis.smIsMember(cacheKey, terminal.map(cacheMember))
    const confirmed = new Set(terminal.filter((_, index) => hits[index]).map(video => video.id))
    const remaining = candidates.filter(video => !confirmed.has(video.id))
    return { candidates: remaining, skipped: confirmed.size }
  } catch (err) {
    console.warn(`[cleanup] Redis read failed; processing the full DB batch (${err.message})`)
    return { candidates, skipped: 0 }
  }
}

async function markRedisConfirmed(redis, cacheKey, videos) {
  if (!redis || videos.length === 0) return 0
  try {
    const members = [...new Set(videos.map(cacheMember))]
    await redis.sAdd(cacheKey, members)
    await redis.expire(cacheKey, REDIS_CACHE_TTL_SECONDS)
    return members.length
  } catch (err) {
    console.warn(`[cleanup] Redis write failed; DB and trash remain authoritative (${err.message})`)
    return 0
  }
}

function isDmca(video) {
  return normalizeStatus(video.reason) === 'dmca'
}

function hasValidTier(video) {
  return Boolean(video.tierName) && Number.isInteger(video.inactivityDays) && video.inactivityDays > 0
}

function isEligible(video, dmcaAges) {
  const status = normalizeStatus(video.status)
  if (INACTIVITY_STATUSES.has(status)) return hasValidTier(video) && video.inactivityEligible === true
  if (TERMINAL_STATUSES.has(status)) return !hasDmcaHold(video, dmcaAges)
  return false
}

function isProtected(video, dmcaAges) {
  return !isEligible(video, dmcaAges)
}

function hasDmcaHold(video, dmcaAges) {
  if (!isDmca(video)) return false
  const timing = dmcaAges.get(video.id)
  return !timing || !Number.isFinite(timing.ageSeconds) || timing.ageSeconds < DMCA_HOLD_SECONDS
}

function hasColdArchiveProof(video) {
  return video.coldStorage === true
    && Boolean(video.coldStorageId)
    && Boolean(video.coldStoragePath)
    && Boolean(video.coldStorageChecksum)
    && Number(video.coldStorageBytes) > 0
}

function assetDirectory(video) {
  const relative = video.encodedPath ? path.dirname(video.encodedPath) : path.join(video.userId, video.id)
  const root = path.resolve(INGEST_DIR)
  const resolved = path.resolve(root, relative)
  if (resolved === root || !resolved.startsWith(`${root}${path.sep}`)) {
    throw new Error('resolved path is outside INGEST_DIR')
  }
  return resolved
}

function inspectExactDirectory(dir) {
  try {
    const stat = lstatSync(dir)
    if (stat.isSymbolicLink()) return { exists: true, safe: false, detail: 'directory is a symlink' }
    if (!stat.isDirectory()) return { exists: true, safe: false, detail: 'path is not a directory' }
    return {
      exists: true,
      safe: true,
      hasMp4: existsSync(path.join(dir, 'video.mp4')),
      hasM3u8: existsSync(path.join(dir, 'video.m3u8')),
    }
  } catch (err) {
    if (err.code === 'ENOENT') return { exists: false, safe: true }
    return { exists: false, safe: false, detail: err.message }
  }
}

function makeRunId() {
  return `${new Date().toISOString().replace(/[:.]/g, '-')}-pid-${process.pid}`
}

function appendManifest(manifestFd, event) {
  appendFileSync(manifestFd, `${JSON.stringify({ at: new Date().toISOString(), ...event })}\n`)
  fsyncSync(manifestFd)
}

function prepareTrashRun() {
  const ingestRoot = realpathSync(INGEST_DIR)
  const trashRoot = realpathSync(TRASH_ROOT)
  if (statSync(ingestRoot).dev !== statSync(trashRoot).dev) {
    throw new Error(`Trash must be on the same filesystem as INGEST_DIR: ${trashRoot}`)
  }
  if (trashRoot === ingestRoot || trashRoot.startsWith(`${ingestRoot}${path.sep}`)) {
    throw new Error('Trash root must not be inside INGEST_DIR')
  }

  const runId = makeRunId()
  const runDir = path.join(trashRoot, runId)
  const filesDir = path.join(runDir, 'encodings')
  mkdirSync(runDir, { mode: 0o750 })
  mkdirSync(filesDir, { mode: 0o750 })
  const manifestPath = path.join(runDir, 'manifest.txt')
  const manifestFd = openSync(manifestPath, 'wx', 0o640)
  appendManifest(manifestFd, {
    event: 'run_started',
    runId,
    ingestRoot,
    trashRoot,
    databaseWrites: true,
    policy: 'tier_inactivity_v1',
  })
  return { runId, runDir, filesDir, manifestPath, manifestFd, ingestRoot, trashRoot }
}

function loadTrashMoveIndex(trashRoot) {
  const bySourcePath = new Map()
  for (const entry of readdirSync(trashRoot, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue
    const manifestPath = path.join(trashRoot, entry.name, 'manifest.txt')
    let lines
    try {
      lines = readFileSync(manifestPath, 'utf8').split('\n')
    } catch {
      continue
    }
    for (const line of lines) {
      if (!line.trim()) continue
      let event
      try {
        event = JSON.parse(line)
      } catch {
        continue
      }
      if (!['move_intent', 'moved'].includes(event.event) || !event.sourcePath || !event.trashPath) continue
      const sourcePath = path.resolve(event.sourcePath)
      const trashPath = path.resolve(event.trashPath)
      if (!trashPath.startsWith(`${trashRoot}${path.sep}`)) continue
      if (!existsSync(sourcePath) && existsSync(trashPath)) {
        bySourcePath.set(sourcePath, { sourcePath, trashPath, manifestPath })
      }
    }
  }
  return bySourcePath
}

function provenTrashRecord(sourceDir, trashMoves) {
  const record = trashMoves.get(sourceDir)
  if (!record || existsSync(sourceDir) || !existsSync(record.trashPath)) return null
  try {
    const stat = lstatSync(record.trashPath)
    return stat.isDirectory() && !stat.isSymbolicLink() ? record : null
  } catch {
    return null
  }
}

function videoAudit(video) {
  const activityAt = video.lastView || video.createdAt
  return {
    videoId: video.id,
    originalStatus: video.status,
    originalReason: video.reason || null,
    creatorTier: video.creatorTier || null,
    tierName: video.tierName || null,
    inactivityDays: video.inactivityDays ?? null,
    lastView: video.lastView || null,
    createdAt: video.createdAt || null,
    activityAt: activityAt || null,
    eligibleAt: video.eligibleAt || null,
    encodedPath: video.encodedPath || null,
  }
}

function moveDirectoryToTrash(video, sourceDir, trashRun, protectedIds) {
  let realSource
  try {
    realSource = realpathSync(sourceDir)
  } catch (err) {
    return { action: 'move_error', detail: `cannot resolve source: ${err.message}` }
  }
  if (realSource !== sourceDir || !realSource.startsWith(`${trashRun.ingestRoot}${path.sep}`)) {
    return { action: 'move_error', detail: 'source real path differs or escapes the ingest root' }
  }

  const relative = path.relative(trashRun.ingestRoot, sourceDir)
  if (!relative || relative.startsWith('..') || path.isAbsolute(relative)) {
    return { action: 'move_error', detail: 'source is outside the verified ingest root' }
  }
  const trashPath = path.join(trashRun.filesDir, relative)
  if (!trashPath.startsWith(`${trashRun.filesDir}${path.sep}`) || existsSync(trashPath)) {
    return { action: 'move_error', detail: 'unsafe or existing trash destination' }
  }

  mkdirSync(path.dirname(trashPath), { recursive: true, mode: 0o750 })
  const record = {
    ...videoAudit(video),
    estimatedBytes: Number(video.encodedSize || 0),
    sourcePath: sourceDir,
    trashPath,
    protectedVideoIds: protectedIds,
  }
  appendManifest(trashRun.manifestFd, { event: 'move_intent', ...record })
  try {
    const current = lstatSync(sourceDir)
    if (!current.isDirectory() || current.isSymbolicLink()) throw new Error('source is no longer a safe directory')
    renameSync(sourceDir, trashPath)
    appendManifest(trashRun.manifestFd, { event: 'moved', ...record })
    return { action: 'moved_to_trash', sourcePath: sourceDir, trashPath }
  } catch (err) {
    appendManifest(trashRun.manifestFd, { event: 'move_failed', error: err.message, ...record })
    return { action: 'move_error', detail: err.message }
  }
}

const VIDEO_FIELDS = `
  v.id, v."userId", v.status, v.reason, v."updatedAt", v."createdAt", v."lastView",
  v."clonedFrom", v."encodedPath", v."encodedSize", v."storageId",
  v."coldStorage", v."coldStorageId", v."coldStorageStatus", v."coldStoragePath",
  v."coldStorageChecksum", v."coldStorageBytes",
  v."transferStatus", v."encodingStatus", v.slug, v.title, u.username,
  u."creatorTier", t.name AS "tierName",
  t."inactivityDays",
  CASE
    WHEN t.id IS NOT NULL AND t."inactivityDays" > 0
    THEN COALESCE(v."lastView", v."createdAt") <= NOW() - make_interval(days => t."inactivityDays")
    ELSE false
  END AS "inactivityEligible",
  CASE
    WHEN t.id IS NOT NULL AND t."inactivityDays" > 0
    THEN COALESCE(v."lastView", v."createdAt") + make_interval(days => t."inactivityDays")
    ELSE NULL
  END AS "eligibleAt"`

async function fetchCandidates(client, storageId, afterId, limit) {
  const { rows } = await client.query(
    `SELECT ${VIDEO_FIELDS}
     FROM "Video" v
     LEFT JOIN "User" u ON u.id = v."userId"
     LEFT JOIN user_tiers t ON lower(t.name) = lower(u."creatorTier")
     WHERE v."storageId" = $1
       AND v.id > $2
       AND (
         v.status IN ('offline', 'removed', 'hard_deleted')
         OR (
           v.status = 'active'
           AND t.id IS NOT NULL
           AND t."inactivityDays" > 0
           AND COALESCE(v."lastView", v."createdAt") <= NOW() - make_interval(days => t."inactivityDays")
         )
       )
     ORDER BY v.id
     LIMIT $3`,
    [storageId, afterId, limit]
  )
  return rows
}

async function fetchRowsByPathsAndRoots(client, candidates) {
  const encodedPaths = [...new Set(candidates.map(v => v.encodedPath).filter(Boolean))]
  const rootIds = candidates.filter(v => !v.encodedPath && !v.clonedFrom).map(v => v.id)
  const related = new Map()

  if (encodedPaths.length > 0) {
    const { rows } = await client.query(
      `SELECT ${VIDEO_FIELDS}
       FROM "Video" v
       LEFT JOIN "User" u ON u.id = v."userId"
       LEFT JOIN user_tiers t ON lower(t.name) = lower(u."creatorTier")
       WHERE v."encodedPath" = ANY($1::text[])`,
      [encodedPaths]
    )
    for (const row of rows) related.set(row.id, row)
  }

  if (rootIds.length > 0) {
    const { rows } = await client.query(
      `WITH RECURSIVE tree AS (
         SELECT v.id, ARRAY[v.id] AS visited
         FROM "Video" v WHERE v.id = ANY($1::text[])
         UNION ALL
         SELECT child.id, tree.visited || child.id
         FROM tree JOIN "Video" child ON child."clonedFrom" = tree.id
         WHERE NOT child.id = ANY(tree.visited)
       )
       SELECT ${VIDEO_FIELDS}
       FROM tree JOIN "Video" v ON v.id = tree.id
       LEFT JOIN "User" u ON u.id = v."userId"
       LEFT JOIN user_tiers t ON lower(t.name) = lower(u."creatorTier")`,
      [rootIds]
    )
    for (const row of rows) related.set(row.id, row)
  }

  for (const row of candidates) related.set(row.id, row)
  return [...related.values()]
}

async function fetchDmcaAges(client, videos) {
  const ids = videos.filter(isDmca).map(v => v.id)
  const result = new Map()
  if (ids.length === 0) return result
  const { rows } = await client.query(
    `SELECT v.id,
            EXTRACT(EPOCH FROM (NOW() - COALESCE(MAX(d."createdAt"), v."updatedAt")))::bigint AS age_seconds
     FROM "Video" v
     LEFT JOIN deleted_videos_logs d
       ON d."videoId" = v.id AND lower(trim(d.reason)) = 'dmca'
     WHERE v.id = ANY($1::text[])
     GROUP BY v.id, v."updatedAt"`,
    [ids]
  )
  for (const row of rows) result.set(row.id, { ageSeconds: Number(row.age_seconds) })
  return result
}

function relatedForCandidate(candidate, related) {
  const matches = new Map()
  if (candidate.encodedPath) {
    for (const row of related) if (row.encodedPath === candidate.encodedPath) matches.set(row.id, row)
  } else {
    const children = new Map()
    for (const row of related) {
      if (!row.clonedFrom) continue
      if (!children.has(row.clonedFrom)) children.set(row.clonedFrom, [])
      children.get(row.clonedFrom).push(row)
    }
    const queue = [candidate]
    while (queue.length > 0) {
      const row = queue.shift()
      if (!row || matches.has(row.id)) continue
      matches.set(row.id, row)
      queue.push(...(children.get(row.id) || []))
    }
  }
  matches.delete(candidate.id)
  return [...matches.values()]
}

async function countInvalidTierVideos(client, storageId) {
  const { rows } = await client.query(
    `SELECT count(*)::int AS count
     FROM "Video" v
     JOIN "User" u ON u.id = v."userId"
     LEFT JOIN user_tiers t ON lower(t.name) = lower(u."creatorTier")
     WHERE v."storageId" = $1
       AND v.status = 'active'
       AND (t.id IS NULL OR t."inactivityDays" <= 0)`,
    [storageId]
  )
  return rows[0].count
}

function newSummary() {
  return {
    inspected: 0,
    by_status: {},
    by_tier: {},
    invalid_tier_protected: 0,
    clone_logical_delete: 0,
    logical_delete_shared: 0,
    dmca_hold: 0,
    retained_shared: 0,
    eligible_existing: 0,
    eligible_missing_proven: 0,
    eligible_cold_remote: 0,
    missing_unproven: 0,
    unsafe_path: 0,
    moved_to_trash: 0,
    move_error: 0,
    stale_after_recheck: 0,
    already_processed: 0,
    redis_confirmed_skipped: 0,
    redis_confirmed_added: 0,
    deleted_files_logged: 0,
    db_marked_hard_deleted: 0,
    db_update_error: 0,
    estimated_reclaimable_bytes: 0,
  }
}

function recordDecision(summary, video, decision, verbose, label) {
  summary.inspected++
  summary.by_status[video.status] = (summary.by_status[video.status] || 0) + 1
  if (INACTIVITY_STATUSES.has(normalizeStatus(video.status))) {
    const tier = video.tierName || 'invalid'
    summary.by_tier[tier] = (summary.by_tier[tier] || 0) + 1
  }
  summary[decision.action] = (summary[decision.action] || 0) + 1
  if (['eligible_existing', 'moved_to_trash'].includes(decision.action)) {
    summary.estimated_reclaimable_bytes += Number(video.encodedSize || 0)
  }
  if (verbose) console.log(`[${label}] ${decision.action.toUpperCase()} ${video.id}${decision.detail ? ` (${decision.detail})` : ''}`)
}

function dryRunDecision(video, group, dmcaAges, trashMoves) {
  if (!isEligible(video, dmcaAges)) {
    if (hasDmcaHold(video, dmcaAges)) return { action: 'dmca_hold', detail: 'DMCA grace period active' }
    return { action: 'stale_after_recheck', detail: 'no longer eligible' }
  }
  if (video.clonedFrom) {
    if (normalizeStatus(video.status) === 'hard_deleted') return { action: 'already_processed', detail: 'hard-deleted clone has no independent files' }
    return { action: 'clone_logical_delete', detail: 'shared files remain untouched' }
  }

  const protectedRows = group.filter(row => row.id !== video.id && isProtected(row, dmcaAges))
  if (protectedRows.length > 0) {
    if (normalizeStatus(video.status) === 'hard_deleted') {
      return { action: 'retained_shared', detail: `${protectedRows.length} protected shared row(s)` }
    }
    return { action: 'logical_delete_shared', detail: `${protectedRows.length} protected shared row(s); files retained` }
  }

  let dir
  try {
    dir = assetDirectory(video)
  } catch (err) {
    return { action: 'unsafe_path', detail: err.message }
  }
  const disk = inspectExactDirectory(dir)
  if (!disk.safe) return { action: 'unsafe_path', detail: disk.detail }
  if (!disk.exists) {
    if (hasColdArchiveProof(video)) {
      return { action: 'eligible_cold_remote', detail: 'verified cold-archive metadata; remote purge will follow terminal DB update' }
    }
    return provenTrashRecord(dir, trashMoves)
      ? { action: 'eligible_missing_proven', detail: 'exact trash copy proven by manifest' }
      : { action: 'missing_unproven', detail: 'source missing without trash proof' }
  }
  return { action: 'eligible_existing', detail: `would move ${dir}; mp4=${disk.hasMp4}, m3u8=${disk.hasM3u8}` }
}

async function lockAndFetchGroup(client, candidate) {
  let userIds
  let videoIds = null
  if (candidate.encodedPath) {
    const result = await client.query(`SELECT DISTINCT "userId" FROM "Video" WHERE "encodedPath" = $1`, [candidate.encodedPath])
    userIds = result.rows.map(row => row.userId)
  } else {
    const result = await client.query(
      `WITH RECURSIVE tree AS (
         SELECT id, "userId", ARRAY[id] visited FROM "Video" WHERE id = $1
         UNION ALL
         SELECT v.id, v."userId", tree.visited || v.id
         FROM tree JOIN "Video" v ON v."clonedFrom" = tree.id
         WHERE NOT v.id = ANY(tree.visited)
       ) SELECT id, "userId" FROM tree`,
      [candidate.id]
    )
    videoIds = result.rows.map(row => row.id)
    userIds = [...new Set(result.rows.map(row => row.userId))]
  }

  if (userIds.length > 0) await client.query(`SELECT id FROM "User" WHERE id = ANY($1::text[]) FOR SHARE`, [userIds])
  await client.query(
    `SELECT id FROM user_tiers
     WHERE lower(name) IN (
       SELECT DISTINCT lower("creatorTier") FROM "User" WHERE id = ANY($1::text[])
     ) FOR SHARE`,
    [userIds]
  )

  const params = candidate.encodedPath ? [candidate.encodedPath] : [videoIds]
  const where = candidate.encodedPath ? `v."encodedPath" = $1` : `v.id = ANY($1::text[])`
  const { rows } = await client.query(
    `SELECT ${VIDEO_FIELDS}
     FROM "Video" v
     LEFT JOIN "User" u ON u.id = v."userId"
     LEFT JOIN user_tiers t ON lower(t.name) = lower(u."creatorTier")
     WHERE ${where}
     FOR UPDATE OF v`,
    params
  )
  return rows
}

async function updateEligibleRows(client, rows, storageId) {
  const targets = rows.filter(row =>
    row.storageId === storageId && ['active', 'offline', 'removed'].includes(normalizeStatus(row.status))
  )
  if (targets.length === 0) return []
  const ids = targets.map(row => row.id)
  const before = new Map(targets.map(row => [row.id, { status: row.status, reason: row.reason || null }]))
  const { rows: updated } = await client.query(
    `UPDATE "Video"
     SET status = 'hard_deleted',
         reason = CASE WHEN status = 'active' THEN 'inactivity' ELSE reason END,
         "updatedAt" = NOW()
     WHERE id = ANY($1::text[])
       AND status IN ('active', 'offline', 'removed')
     RETURNING id, status, reason, "updatedAt"`,
    [ids]
  )
  return updated.map(row => ({
    videoId: row.id,
    fromStatus: before.get(row.id).status,
    fromReason: before.get(row.id).reason,
    toStatus: row.status,
    toReason: row.reason || null,
    expectedUpdatedAt: row.updatedAt,
  }))
}

function deletionLogReason(video) {
  const status = normalizeStatus(video.status)
  const reason = normalizeStatus(video.reason)
  if (INACTIVITY_STATUSES.has(status) || reason === 'inactivity') {
    return hasValidTier(video) ? `Inactivity ${video.inactivityDays} Days` : 'Inactivity Unknown Days'
  }
  if (reason === 'dmca') return 'dmca'
  if (video.reason?.trim()) return video.reason.trim()
  return `Hard delete of ${status || 'unknown'} video — moved to trash`
}

async function insertDeletionLogs(client, videos) {
  const inserted = []
  for (const video of videos) {
    const reason = deletionLogReason(video)
    const id = randomUUID()
    const { rows } = await client.query(
      `INSERT INTO deleted_videos_logs
         (id, reason, "userId", "videoId", slug, title, "deleteType", "createdAt")
       SELECT $1, $2, $3, $4, $5, $6, 'HardDelete', NOW()
       WHERE NOT EXISTS (
         SELECT 1 FROM deleted_videos_logs
         WHERE "videoId" = $4 AND reason = $2 AND "deleteType" = 'HardDelete'
       )
       RETURNING id, reason, "videoId", "createdAt"`,
      [id, reason, video.userId, video.id, video.slug || '', video.title || '']
    )
    if (rows[0]) inserted.push(rows[0])
  }
  return inserted
}

function applyStatusChanges(videos, changes) {
  const changesById = new Map(changes.map(change => [change.videoId, change]))
  return videos.map(video => {
    const change = changesById.get(video.id)
    return change ? { ...video, status: change.toStatus, reason: change.toReason, updatedAt: change.expectedUpdatedAt } : video
  })
}

async function executeCandidate(client, candidate, storageId, trashRun, trashMoves) {
  await client.query('BEGIN')
  let moved = null
  try {
    const group = await lockAndFetchGroup(client, candidate)
    const dmcaAges = await fetchDmcaAges(client, group)
    const current = group.find(row => row.id === candidate.id)
    if (!current || !isEligible(current, dmcaAges)) {
      await client.query('ROLLBACK')
      return { decision: { action: 'stale_after_recheck', detail: 'tier, activity, status, or DMCA state changed' }, processedIds: [] }
    }

    const protectedRows = group.filter(row => row.id !== current.id && isProtected(row, dmcaAges))
    const eligibleRows = group.filter(row => isEligible(row, dmcaAges))
    const updatableRows = eligibleRows.filter(row => ['active', 'offline', 'removed'].includes(normalizeStatus(row.status)))
    let decision

    if (current.clonedFrom || protectedRows.length > 0) {
      // A clone never owns the shared directory. When a clone is encountered first,
      // leave an eligible original for its own candidate pass so it can move the files.
      const logicalUpdatableRows = current.clonedFrom
        ? updatableRows.filter(row => row.clonedFrom)
        : updatableRows
      const changes = await updateEligibleRows(client, logicalUpdatableRows, storageId)
      const changedIds = new Set(changes.map(change => change.videoId))
      const rowsToLog = eligibleRows.filter(row =>
        row.storageId === storageId && (changedIds.has(row.id) || (row.id === current.id && normalizeStatus(row.status) === 'hard_deleted'))
      )
      const deletionLogs = await insertDeletionLogs(client, rowsToLog)
      await client.query('COMMIT')
      if (changes.length > 0 || deletionLogs.length > 0) {
        appendManifest(trashRun.manifestFd, {
          event: current.clonedFrom ? 'logical_clone_deleted' : 'logical_shared_deleted',
          assetPath: current.encodedPath || null,
          protectedVideoIds: protectedRows.map(row => row.id),
          statusChanges: changes,
          deletionLogs,
          policy: 'tier_inactivity_v1',
        })
      }
      decision = current.clonedFrom
        ? { action: 'clone_logical_delete', detail: 'row hard-deleted; shared files untouched' }
        : normalizeStatus(current.status) === 'hard_deleted'
          ? { action: 'retained_shared', detail: `${protectedRows.length} protected shared row(s)` }
          : { action: 'logical_delete_shared', detail: `${protectedRows.length} protected shared row(s); files retained` }
      const finalRows = applyStatusChanges(rowsToLog, changes)
      const cacheRows = current.clonedFrom ? finalRows.filter(row => row.clonedFrom) : []
      return {
        decision,
        processedIds: [...new Set([current.id, ...changes.map(change => change.videoId)])],
        cacheRows,
        dbUpdated: changes.length,
        deletionLogs: deletionLogs.length,
      }
    }

    let sourceDir
    try {
      sourceDir = assetDirectory(current)
    } catch (err) {
      await client.query('ROLLBACK')
      return { decision: { action: 'unsafe_path', detail: err.message }, processedIds: [] }
    }
    const disk = inspectExactDirectory(sourceDir)
    if (!disk.safe) {
      await client.query('ROLLBACK')
      return { decision: { action: 'unsafe_path', detail: disk.detail }, processedIds: [] }
    }

    if (disk.exists) {
      moved = moveDirectoryToTrash(current, sourceDir, trashRun, [])
      if (moved.action !== 'moved_to_trash') {
        await client.query('ROLLBACK')
        return { decision: moved, processedIds: [] }
      }
      trashMoves.set(sourceDir, { sourcePath: sourceDir, trashPath: moved.trashPath, manifestPath: trashRun.manifestPath })
    } else if (!provenTrashRecord(sourceDir, trashMoves) && !hasColdArchiveProof(current)) {
      await client.query('ROLLBACK')
      return { decision: { action: 'missing_unproven', detail: 'source missing without exact trash proof' }, processedIds: [] }
    }

    const changes = await updateEligibleRows(client, updatableRows, storageId)
    const changedIds = new Set(changes.map(change => change.videoId))
    const rowsToLog = eligibleRows.filter(row =>
      row.storageId === storageId && (changedIds.has(row.id) || normalizeStatus(row.status) === 'hard_deleted')
    )
    const deletionLogs = await insertDeletionLogs(client, rowsToLog)
    await client.query('COMMIT')
    if (changes.length > 0 || deletionLogs.length > 0) {
      appendManifest(trashRun.manifestFd, {
        event: 'db_status_updated',
        assetPath: current.encodedPath || null,
        sourcePath: sourceDir,
        statusChanges: changes,
        deletionLogs,
        policy: 'tier_inactivity_v1',
      })
    }
    decision = moved
      ? { action: 'moved_to_trash', detail: `${sourceDir} -> ${moved.trashPath}` }
      : hasColdArchiveProof(current)
        ? { action: 'eligible_cold_remote', detail: 'DB updated; cold-storage worker will purge the remote copy' }
        : { action: 'eligible_missing_proven', detail: 'DB reconciled from exact trash manifest' }
    const finalRows = applyStatusChanges(rowsToLog, changes)
    return {
      decision,
      processedIds: [...new Set(finalRows.map(row => row.id))],
      cacheRows: finalRows,
      dbUpdated: changes.length,
      deletionLogs: deletionLogs.length,
    }
  } catch (err) {
    try { await client.query('ROLLBACK') } catch {}
    if (moved?.action === 'moved_to_trash') {
      appendManifest(trashRun.manifestFd, { event: 'db_status_update_failed', sourcePath: moved.sourcePath, trashPath: moved.trashPath, error: err.message })
    }
    throw err
  }
}

async function main() {
  const options = parseArgs(process.argv.slice(2))
  if (options.help) return usage()
  if (!DATABASE_URL) throw new Error('DATABASE_URL is not configured')
  if (options.scheduled) {
    const now = new Date()
    if (![0, 12].includes(now.getHours()) || now.getMinutes() !== 0) {
      console.log(`[cleanup] scheduled startup outside 00:00/12:00 Europe/Berlin; exiting without changes`)
      return
    }
  }

  const label = options.execute ? 'trash' : 'dry-run'
  console.log(options.execute
    ? '[trash] EXECUTE MODE — tier eligibility and shared references are rechecked under lock'
    : '[dry-run] READ ONLY — no database or filesystem changes will be made')
  console.log(`[${label}] exact-path checks under: ${INGEST_DIR}; batch size: ${options.batchSize}`)

  const client = new pg.Client({
    connectionString: DATABASE_URL,
    application_name: options.execute ? 'firestream-cleanup-trash' : 'firestream-cleanup-dry-run',
  })
  await client.connect()
  let trashRun = null
  let lockHeld = false
  let redis = null
  try {
    await client.query("SET statement_timeout = '30s'")
    await client.query("SET lock_timeout = '5s'")
    if (!options.execute) {
      await client.query('SET default_transaction_read_only = on')
      const readOnly = await client.query('SHOW transaction_read_only')
      if (readOnly.rows[0]?.transaction_read_only !== 'on') throw new Error('dry-run DB session is not read-only')
    }

    const lock = await client.query(`SELECT pg_try_advisory_lock($1) AS acquired`, [ADVISORY_LOCK_KEY])
    if (!lock.rows[0].acquired) {
      console.log(`[${label}] another cleanup run is active; exiting without changes`)
      return
    }
    lockHeld = true

    const localIp = getLocalIp()
    const server = await client.query(`SELECT id, type FROM "Server" WHERE ip = $1`, [localIp])
    if (server.rows.length !== 1 || server.rows[0].type !== 'storage') throw new Error(`local IP ${localIp} is not one storage server`)
    const storageId = server.rows[0].id
    const cacheKey = `firestream:cleanup:${REDIS_CACHE_VERSION}:${storageId}:confirmed`
    if (options.noCache) {
      console.log(`[${label}] Redis confirmed-deletion cache disabled`)
    } else {
      redis = await connectRedis(label)
    }

    if (options.execute) {
      trashRun = prepareTrashRun()
      console.log(`[trash] run: ${trashRun.runId}`)
      console.log(`[trash] manifest: ${trashRun.manifestPath}`)
    }
    const trashMoves = loadTrashMoveIndex(realpathSync(TRASH_ROOT))
    const summary = newSummary()
    summary.invalid_tier_protected = await countInvalidTierVideos(client, storageId)
    const processedIds = new Set()
    let afterId = ''

    while (options.limit === null || summary.inspected < options.limit) {
      const remaining = options.limit === null ? options.batchSize : options.limit - summary.inspected
      const fetchedCandidates = await fetchCandidates(client, storageId, afterId, Math.min(options.batchSize, remaining))
      if (fetchedCandidates.length === 0) break
      afterId = fetchedCandidates.at(-1).id
      const cached = await excludeRedisConfirmed(redis, cacheKey, fetchedCandidates)
      const candidates = cached.candidates
      summary.redis_confirmed_skipped += cached.skipped
      if (candidates.length === 0) continue

      let related = []
      let dmcaAges = new Map()
      if (!options.execute) {
        related = await fetchRowsByPathsAndRoots(client, candidates)
        dmcaAges = await fetchDmcaAges(client, related)
      }

      for (const video of candidates) {
        let decision
        if (processedIds.has(video.id)) {
          decision = { action: 'already_processed', detail: 'shared group handled earlier in this run' }
        } else if (options.execute) {
          try {
            const result = await executeCandidate(client, video, storageId, trashRun, trashMoves)
            decision = result.decision
            for (const id of result.processedIds || []) processedIds.add(id)
            summary.db_marked_hard_deleted += result.dbUpdated || 0
            summary.deleted_files_logged += result.deletionLogs || 0
            summary.redis_confirmed_added += await markRedisConfirmed(redis, cacheKey, result.cacheRows || [])
          } catch (err) {
            summary.db_update_error++
            decision = { action: 'move_error', detail: err.message }
            appendManifest(trashRun.manifestFd, { event: 'candidate_failed', videoId: video.id, error: err.message })
          }
        } else {
          const group = [video, ...relatedForCandidate(video, related)]
          decision = dryRunDecision(video, group, dmcaAges, trashMoves)
        }
        recordDecision(summary, video, decision, options.verbose, label)
      }
      console.log(`[${label}] progress: ${summary.inspected} candidates inspected`)
    }

    console.log(`\n[${label}] summary`)
    console.log(JSON.stringify({ ...summary, estimated_reclaimable: formatBytes(summary.estimated_reclaimable_bytes) }, null, 2))
    if (trashRun) {
      appendManifest(trashRun.manifestFd, { event: 'run_completed', summary })
      console.log(`[trash] complete — recoverable via ${trashRun.manifestPath}`)
    } else {
      console.log('[dry-run] complete — nothing was changed')
    }
  } finally {
    if (lockHeld) {
      try { await client.query(`SELECT pg_advisory_unlock($1)`, [ADVISORY_LOCK_KEY]) } catch {}
    }
    if (trashRun?.manifestFd !== undefined) closeSync(trashRun.manifestFd)
    if (redis?.isOpen) await redis.quit().catch(() => redis.destroy())
    await client.end()
  }
}

main().catch(err => {
  console.error(`[cleanup] ERROR: ${err.message}`)
  process.exitCode = 1
})
