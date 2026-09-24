import { execFile as execFileCallback } from 'node:child_process'
import { appendFile, lstat, mkdir, readdir, realpath, rm } from 'node:fs/promises'
import path from 'node:path'
import { promisify } from 'node:util'
import pg from 'pg'
import { INGEST_DIR, MISC_DIR } from '../config.js'

const execFile = promisify(execFileCallback)
const DATABASE_URL = process.env.DATABASE_URL
const COLD_STAGE_ROOT = process.env.COLD_STAGE_ROOT || '/cold-stage'
const TRASH_ROOT = process.env.TRASH_DIR || path.join(path.dirname(path.resolve(INGEST_DIR)), 'trash')
const COLD_DELETE_ROOT = path.join(path.dirname(path.resolve(INGEST_DIR)), 'cold-delete')
const COLD_RESTORE_ROOT = path.join(path.dirname(path.resolve(INGEST_DIR)), 'cold-restore')
const AUDIT_LOG = process.env.DELETE_BANNED_AUDIT_LOG || '/var/log/firestream-dav/delete-banned.log'
const SAFE_COMPONENT = /^[A-Za-z0-9][A-Za-z0-9._-]*$/
const MANAGED_WORKERS = [
  'firestream-cleanup',
  'firestream-transfers',
  'firestream-cold-stage',
  'firestream-cold-transfer',
  'firestream-cold-restore',
]

function usage() {
  console.log(`Usage: node scripts/delete-banned.js [--username USERNAME] [--execute]

Fast, guarded purge for banned-user data on this storage server. When
--username is omitted, every currently banned account is processed.
Dry-run is the default. Execute mode temporarily pauses relevant PM2 workers,
removes the user's local, staging, trash, restore, and Storage Box directories,
then clears cold-storage state in the database. Dry runs read the actual local
and remote files so their inventory may take a little time for large accounts.

Options:
  --username NAME   Purge one exact username instead of all banned accounts
  --execute         Permanently delete the selected banned users' files
  --help            Show this help
`)
}

function parseArgs(argv) {
  const options = { username: null, execute: false, help: false }
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]
    if (arg === '--username') options.username = argv[++i]
    else if (arg === '--execute') options.execute = true
    else if (arg === '--help') options.help = true
    else throw new Error(`unknown option: ${arg}`)
  }
  if (options.username !== null && !String(options.username).trim()) throw new Error('--username cannot be empty')
  return options
}

function formatBytes(bytes) {
  const value = Number(bytes)
  if (!Number.isFinite(value) || value <= 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  const index = Math.min(Math.floor(Math.log(value) / Math.log(1000)), units.length - 1)
  return `${(value / 1000 ** index).toFixed(index > 2 ? 2 : 1)} ${units[index]}`
}

function formatCount(value) {
  return Number(value || 0).toLocaleString('en-US')
}

function emptyInventory() {
  return { files: 0, bytes: 0 }
}

function addInventory(target, source) {
  target.files += Number(source?.files || 0)
  target.bytes += Number(source?.bytes || 0)
  return target
}

function log(message) {
  console.log(`[${new Date().toISOString()}] [delete-banned] ${message}`)
}

function safeComponent(value, label) {
  const text = String(value || '')
  if (!SAFE_COMPONENT.test(text)) throw new Error(`${label} contains unsafe characters`)
  return text
}

function safeRemotePath(value, label = 'remote path') {
  const normalized = String(value || '').replace(/^\.\//, '').replace(/\/$/, '')
  if (!normalized || normalized.startsWith('/') || normalized.includes('..')) {
    throw new Error(`${label} is unsafe`)
  }
  for (const component of normalized.split('/')) safeComponent(component, label)
  return normalized
}

async function pathInfo(target) {
  try {
    const info = await lstat(target)
    return { exists: true, directory: info.isDirectory(), symlink: info.isSymbolicLink() }
  } catch (error) {
    if (error.code === 'ENOENT') return { exists: false, directory: false, symlink: false }
    throw error
  }
}

function assertChild(root, target, label) {
  const resolvedRoot = path.resolve(root)
  const resolvedTarget = path.resolve(target)
  if (resolvedTarget === resolvedRoot || !resolvedTarget.startsWith(`${resolvedRoot}${path.sep}`)) {
    throw new Error(`${label} escapes its allowed root`)
  }
  return resolvedTarget
}

async function safeRemoveTree(root, target, label) {
  const resolved = assertChild(root, target, label)
  const info = await pathInfo(resolved)
  if (!info.exists) {
    log(`${label}: already absent (${resolved})`)
    return false
  }
  if (!info.directory || info.symlink) throw new Error(`${label} is not a safe real directory: ${resolved}`)
  log(`${label}: deleting ${resolved}`)
  await rm(resolved, { recursive: true, force: false, maxRetries: 3, retryDelay: 1000 })
  if ((await pathInfo(resolved)).exists) throw new Error(`${label} still exists after deletion`)
  log(`${label}: deleted`)
  return true
}

async function listTrashUserDirectories(userId) {
  const directories = []
  let runs
  try {
    runs = await readdir(TRASH_ROOT, { withFileTypes: true })
  } catch (error) {
    if (error.code === 'ENOENT') return directories
    throw error
  }
  for (const run of runs) {
    if (!run.isDirectory() || run.isSymbolicLink()) continue
    const candidate = path.join(TRASH_ROOT, run.name, 'encodings', userId)
    if ((await pathInfo(candidate)).exists) directories.push(candidate)
  }
  return directories
}

async function listOwnedVideoDirectories(root, videoIds) {
  const directories = []
  let entries
  try {
    entries = await readdir(root, { withFileTypes: true })
  } catch (error) {
    if (error.code === 'ENOENT') return directories
    throw error
  }
  for (const entry of entries) {
    if (entry.isDirectory() && !entry.isSymbolicLink() && videoIds.has(entry.name)) {
      directories.push(path.join(root, entry.name))
    }
  }
  return directories
}

async function inspectLocalTree(target, excludedTopLevels = new Set(), retainWholeTree = false) {
  const total = emptyInventory()
  if (retainWholeTree) return total
  const rootInfo = await pathInfo(target)
  if (!rootInfo.exists) return total
  if (!rootInfo.directory || rootInfo.symlink) throw new Error(`inventory target is not a safe real directory: ${target}`)

  const pending = [{ directory: target, topLevel: true }]
  while (pending.length > 0) {
    const { directory, topLevel } = pending.pop()
    let entries
    try {
      entries = await readdir(directory, { withFileTypes: true })
    } catch (error) {
      if (error.code === 'ENOENT') continue
      throw error
    }
    for (const entry of entries) {
      if (topLevel && excludedTopLevels.has(entry.name)) continue
      const candidate = path.join(directory, entry.name)
      if (entry.isDirectory() && !entry.isSymbolicLink()) {
        pending.push({ directory: candidate, topLevel: false })
        continue
      }
      try {
        const info = await lstat(candidate)
        total.files++
        total.bytes += Number(info.size || 0)
      } catch (error) {
        if (error.code !== 'ENOENT') throw error
      }
    }
  }
  return total
}

async function localStorage(client) {
  const { stdout } = await execFile('hostname', ['-I'])
  const ip = stdout.trim().split(/\s+/)[0]
  const { rows } = await client.query(
    `SELECT id, ip, hostname FROM "Server" WHERE ip = $1 AND type = 'storage'`,
    [ip]
  )
  if (rows.length !== 1) throw new Error(`local IP ${ip} does not identify exactly one storage server`)
  return rows[0]
}

async function loadPlan(client, username) {
  const users = await client.query(
    `SELECT id, username, "isBanned", "bannedReason" FROM "User" WHERE lower(username) = lower($1)`,
    [String(username).trim()]
  )
  if (users.rows.length !== 1) throw new Error(`username ${username} did not resolve to exactly one user`)
  const user = users.rows[0]
  safeComponent(user.id, 'user ID')
  if (!user.isBanned) throw new Error(`refusing to purge ${user.username}: user is not banned`)

  const storage = await localStorage(client)
  const videos = await client.query(
    `SELECT v.id, v."storageId", v.status, v."transferStatus", v."clonedFrom", v."encodedPath",
            v."encodedSize", v."coldStorage", v."coldStorageId",
            v."coldStorageStatus", v."coldStoragePath", v."coldStorageBytes",
            s.status AS "stageStatus", s."stagePath", s.bytes AS "stageBytes"
     FROM "Video" v
     LEFT JOIN cold_storage_staging s ON s."videoId" = v.id
     WHERE v."userId" = $1
     ORDER BY v.id`,
    [user.id]
  )

  const foreignStorage = videos.rows.filter(video => video.storageId && video.storageId !== storage.id)
  if (foreignStorage.length > 0) {
    throw new Error(`${foreignStorage.length} video(s) belong to another storage server; run the purge on that server first`)
  }

  const ingestRoot = await realpath(INGEST_DIR)
  const userRoot = path.join(ingestRoot, user.id)
  const shared = await client.query(
    `SELECT target.id AS "targetId", target."encodedPath" AS "targetPath",
            other.id AS "otherId", other."userId" AS "otherUserId",
            other."clonedFrom" AS "otherClonedFrom", other."encodedPath" AS "otherPath"
     FROM "Video" target
     JOIN "Video" other ON other."userId" <> $1 AND (
       other."clonedFrom" = target.id
       OR target."clonedFrom" = other.id
       OR (target."encodedPath" <> '' AND other."encodedPath" = target."encodedPath")
     )
     WHERE target."userId" = $1 AND other.status = 'active'`,
    [user.id]
  )
  for (const video of videos.rows) {
    if (!video.encodedPath) continue
    const directory = path.resolve(ingestRoot, path.dirname(video.encodedPath))
    if (directory !== userRoot && !directory.startsWith(`${userRoot}${path.sep}`)) {
      if (!video.clonedFrom) throw new Error(`video ${video.id} has an owned encodedPath outside the banned user's directory`)
    }
  }

  const protectedDirectories = new Set()
  for (const reference of shared.rows) {
    if (!reference.targetPath) continue
    const directory = path.resolve(ingestRoot, path.dirname(reference.targetPath))
    const targetOwnsPath = directory === userRoot || directory.startsWith(`${userRoot}${path.sep}`)
    const targetOwnsClone = reference.otherClonedFrom === reference.targetId
    const sharesExactPath = reference.otherPath === reference.targetPath
    if (targetOwnsPath && (targetOwnsClone || sharesExactPath)) protectedDirectories.add(directory)
  }
  const protectedVideoIds = new Set()
  for (const video of videos.rows) {
    if (!video.encodedPath) {
      if (protectedDirectories.has(path.join(userRoot, video.id))) protectedVideoIds.add(video.id)
      continue
    }
    const directory = path.resolve(ingestRoot, path.dirname(video.encodedPath))
    if (protectedDirectories.has(directory)) protectedVideoIds.add(video.id)
  }
  const purgeVideos = videos.rows.filter(video => !protectedVideoIds.has(video.id))
  const protectedTopLevels = new Set()
  let protectWholeUserRoot = false
  for (const directory of protectedDirectories) {
    if (directory === userRoot) {
      protectWholeUserRoot = true
      continue
    }
    const relative = path.relative(userRoot, directory)
    const topLevel = relative.split(path.sep)[0]
    if (!topLevel || topLevel === '..' || path.isAbsolute(relative)) throw new Error('protected shared path escapes the user root')
    protectedTopLevels.add(topLevel)
  }

  const boxes = await client.query(
    `SELECT id, plan, hostname, username, "sshPort", "sshKeyPath", "knownHostsPath",
            "basePath", "minimumFreeBytes"
     FROM storage_boxes
     ORDER BY priority, id`
  )
  for (const box of boxes.rows) {
    safeComponent(box.username, `Storage Box ${box.id} username`)
    if (!/^[-A-Za-z0-9.]+$/.test(box.hostname)) throw new Error(`Storage Box ${box.id} hostname is unsafe`)
    if (!box.sshKeyPath || !box.knownHostsPath || /\s/.test(box.sshKeyPath) || /\s/.test(box.knownHostsPath)) {
      throw new Error(`Storage Box ${box.id} SSH paths are unavailable or unsafe`)
    }
    box.basePath = safeRemotePath(box.basePath, `Storage Box ${box.id} base path`)
  }

  const videoIds = new Set(purgeVideos.map(video => safeComponent(video.id, 'video ID')))
  const trashDirectories = await listTrashUserDirectories(user.id)
  const coldDeleteDirectories = await listOwnedVideoDirectories(COLD_DELETE_ROOT, videoIds)
  const coldRestoreDirectories = await listOwnedVideoDirectories(COLD_RESTORE_ROOT, videoIds)
  const coldByBox = new Map()
  let encodedBytes = 0
  let stagedBytes = 0
  let coldBytes = 0
  let protectedBytes = 0
  for (const video of videos.rows) {
    encodedBytes += Number(video.encodedSize || 0)
    if (protectedVideoIds.has(video.id)) protectedBytes += Number(video.encodedSize || 0)
  }
  for (const video of purgeVideos) {
    stagedBytes += Number(video.stageBytes || 0)
    if (video.coldStorageId) {
      coldBytes += Number(video.coldStorageBytes || 0)
      const entry = coldByBox.get(video.coldStorageId) || { videos: 0, bytes: 0, finalIds: [], incomingIds: [] }
      entry.videos++
      entry.bytes += Number(video.coldStorageBytes || 0)
      entry.finalIds.push(video.id)
      if (['archiving', 'failed'].includes(video.coldStorageStatus)) entry.incomingIds.push(video.id)
      coldByBox.set(video.coldStorageId, entry)
    }
  }

  return {
    user,
    storage,
    videos: videos.rows,
    purgeVideos,
    videoIds,
    protectedVideoIds,
    protectedDirectories,
    protectedTopLevels,
    protectWholeUserRoot,
    boxes: boxes.rows,
    coldByBox,
    paths: {
      ingestRoot,
      userRoot,
      miscRoot: path.resolve(MISC_DIR),
      miscUser: path.join(path.resolve(MISC_DIR), user.id),
      stageVideosRoot: path.join(path.resolve(COLD_STAGE_ROOT), 'videos'),
      stageUser: path.join(path.resolve(COLD_STAGE_ROOT), 'videos', user.id),
      trashDirectories,
      coldDeleteDirectories,
      coldRestoreDirectories,
    },
    totals: {
      videos: videos.rowCount,
      purgeVideos: purgeVideos.length,
      protectedVideos: protectedVideoIds.size,
      protectedDirectories: protectedDirectories.size,
      encodedBytes,
      protectedBytes,
      stagedVideos: purgeVideos.filter(video => video.stagePath).length,
      stagedBytes,
      coldVideos: purgeVideos.filter(video => video.coldStorageId).length,
      coldBytes,
      transferring: videos.rows.filter(video => video.transferStatus === 'transferring').length,
    },
  }
}

function rcloneSftpArgs(box) {
  return [
    '--sftp-host', box.hostname,
    '--sftp-user', box.username,
    '--sftp-port', String(box.sshPort),
    '--sftp-key-file', box.sshKeyPath,
    '--sftp-known-hosts-file', box.knownHostsPath,
    '--sftp-disable-hashcheck',
  ]
}

async function remoteExists(box, remotePath) {
  const safePath = safeRemotePath(remotePath)
  try {
    await execFile('ssh', [...sshArgs(box), 'stat', safePath], {
      timeout: 30_000,
      maxBuffer: 1024 * 1024,
    })
    return true
  } catch (error) {
    if (Number(error.code) === 1) return false
    throw error
  }
}

async function inspectRemoteTree(box, remotePath, includedTopLevels = null) {
  const safePath = safeRemotePath(remotePath)
  if (includedTopLevels !== null && includedTopLevels.length === 0) return emptyInventory()
  if (!(await remoteExists(box, safePath))) return emptyInventory()
  const filters = []
  if (includedTopLevels !== null) {
    for (const component of includedTopLevels) {
      filters.push('--include', `/${safeComponent(component, 'remote inventory component')}/**`)
    }
  }
  const { stdout } = await execFile('rclone', [
    'size', `:sftp:./${safePath}/`, '--json',
    ...filters,
    ...rcloneSftpArgs(box),
  ], { timeout: 30 * 60_000, maxBuffer: 10 * 1024 * 1024 })
  const result = JSON.parse(stdout)
  return { files: Number(result.count || 0), bytes: Number(result.bytes || 0) }
}

async function inventoryPlan(plan) {
  const excluded = plan.protectedDirectories.size > 0 ? plan.protectedTopLevels : new Set()
  const retainWhole = plan.protectedDirectories.size > 0 && plan.protectWholeUserRoot
  const local = {
    encodings: await inspectLocalTree(plan.paths.userRoot, excluded, retainWhole),
    staging: await inspectLocalTree(plan.paths.stageUser, excluded, retainWhole),
    trash: emptyInventory(),
    misc: plan.protectedDirectories.size > 0
      ? emptyInventory()
      : await inspectLocalTree(plan.paths.miscUser),
    coldDelete: emptyInventory(),
    coldRestore: emptyInventory(),
  }
  for (const directory of plan.paths.trashDirectories) {
    addInventory(local.trash, await inspectLocalTree(directory, excluded, retainWhole))
  }
  for (const directory of plan.paths.coldDeleteDirectories) {
    addInventory(local.coldDelete, await inspectLocalTree(directory))
  }
  for (const directory of plan.paths.coldRestoreDirectories) {
    addInventory(local.coldRestore, await inspectLocalTree(directory))
  }

  const boxes = []
  for (const box of plan.boxes) {
    const remote = plan.coldByBox.get(box.id) || { finalIds: [], incomingIds: [] }
    try {
      const archives = await inspectRemoteTree(
        box,
        `${box.basePath}/videos/${plan.user.id}`,
        plan.protectedDirectories.size > 0 ? remote.finalIds : null
      )
      const incoming = await inspectRemoteTree(box, `${box.basePath}/incoming`, remote.incomingIds)
      boxes.push({ box, archives, incoming, error: null })
    } catch (error) {
      boxes.push({ box, archives: emptyInventory(), incoming: emptyInventory(), error: error.message })
    }
  }

  const total = emptyInventory()
  for (const value of Object.values(local)) addInventory(total, value)
  for (const entry of boxes) {
    addInventory(total, entry.archives)
    addInventory(total, entry.incoming)
  }
  return { local, boxes, total, complete: boxes.every(entry => !entry.error) }
}

async function selectedUsernames(client, username) {
  if (username !== null) return [String(username).trim()]
  const { rows } = await client.query(
    `SELECT username FROM "User" WHERE "isBanned" ORDER BY lower(username), id`
  )
  return rows.map(row => row.username)
}

function sshArgs(box) {
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
    `${box.username}@${box.hostname}`,
  ]
}

async function runSsh(box, command, args, timeout = 120_000) {
  for (const value of args) {
    if (value === '-rf') continue
    safeRemotePath(value)
  }
  return execFile('ssh', [...sshArgs(box), command, ...args], { timeout, maxBuffer: 1024 * 1024 })
}

function parseDf(stdout, boxId) {
  const values = stdout.trim().split('\n').at(-1)?.trim().split(/\s+/)
  if (!values || values.length < 4) throw new Error(`cannot parse df response from ${boxId}`)
  const total = Number(values[1])
  const used = Number(values[2])
  const available = Number(values[3])
  if (![total, used, available].every(Number.isFinite)) throw new Error(`invalid df response from ${boxId}`)
  return { total, used, available }
}

async function remoteUsage(box) {
  const { stdout } = await execFile('ssh', [...sshArgs(box), 'df', '-B1', '.'], {
    timeout: 30_000,
    maxBuffer: 1024 * 1024,
  })
  return parseDf(stdout, box.id)
}

async function preflightBoxes(boxes) {
  const usage = new Map()
  for (const box of boxes) {
    const current = await remoteUsage(box)
    usage.set(box.id, current)
    log(`box ${box.id}: reachable; ${formatBytes(current.available)} available`)
  }
  return usage
}

async function purgeBox(box, userId, remote, hasProtectedSharedAssets) {
  const finalUserPath = safeRemotePath(`${box.basePath}/videos/${userId}`)
  if (!hasProtectedSharedAssets) {
    log(`box ${box.id}: deleting ${finalUserPath}`)
    await runSsh(box, 'rm', ['-rf', finalUserPath], 10 * 60_000)
  } else {
    for (let index = 0; index < remote.finalIds.length; index += 100) {
      const paths = remote.finalIds.slice(index, index + 100)
        .map(id => `${box.basePath}/videos/${userId}/${safeComponent(id, 'video ID')}`)
      if (paths.length > 0) await runSsh(box, 'rm', ['-rf', ...paths], 10 * 60_000)
    }
    log(`box ${box.id}: retained shared directories; deleted ${remote.finalIds.length} tracked unshared archive(s)`)
  }
  for (let index = 0; index < remote.incomingIds.length; index += 100) {
    const paths = remote.incomingIds.slice(index, index + 100).map(id => `${box.basePath}/incoming/${safeComponent(id, 'video ID')}`)
    if (paths.length > 0) await runSsh(box, 'rm', ['-rf', ...paths], 10 * 60_000)
  }
  const usage = await remoteUsage(box)
  log(`box ${box.id}: purge complete; ${formatBytes(usage.available)} available`)
  return usage
}

async function pm2OnlineWorkers() {
  const { stdout } = await execFile('pm2', ['jlist'], { maxBuffer: 10 * 1024 * 1024 })
  const processes = JSON.parse(stdout)
  const wanted = new Set(MANAGED_WORKERS)
  return processes
    .filter(process => wanted.has(process.name) && process.pm2_env?.status === 'online')
    .map(process => process.name)
}

async function stopWorkers(names) {
  if (names.length === 0) return
  log(`pausing PM2 workers: ${names.join(', ')}`)
  await execFile('pm2', ['stop', ...names], { timeout: 120_000, maxBuffer: 10 * 1024 * 1024 })
}

async function startWorkers(names) {
  if (names.length === 0) return
  log(`restarting PM2 workers: ${names.join(', ')}`)
  await execFile('pm2', ['start', ...names], { timeout: 120_000, maxBuffer: 10 * 1024 * 1024 })
}

async function recheckBanned(client, user) {
  const { rows } = await client.query(`SELECT username, "isBanned" FROM "User" WHERE id = $1`, [user.id])
  if (rows.length !== 1 || !rows[0].isBanned || rows[0].username !== user.username) {
    throw new Error('user identity or banned status changed after planning; refusing deletion')
  }
}

async function removeUserTree(plan, root, target, label) {
  if (plan.protectedDirectories.size === 0) return safeRemoveTree(root, target, label)
  const info = await pathInfo(target)
  if (!info.exists) {
    log(`${label}: already absent (${target})`)
    return false
  }
  if (!info.directory || info.symlink) throw new Error(`${label} is not a safe real directory: ${target}`)
  if (plan.protectWholeUserRoot) {
    log(`${label}: retained entirely because an active cross-user reference targets the user root`)
    return false
  }
  const entries = await readdir(target, { withFileTypes: true })
  let deleted = 0
  for (const entry of entries) {
    if (plan.protectedTopLevels.has(entry.name)) {
      log(`${label}: retained shared entry ${entry.name}`)
      continue
    }
    if (!entry.isDirectory() || entry.isSymbolicLink()) {
      throw new Error(`${label} contains an unsafe non-directory entry: ${entry.name}`)
    }
    if (await safeRemoveTree(target, path.join(target, entry.name), `${label} entry ${entry.name}`)) deleted++
  }
  log(`${label}: deleted ${deleted} unshared entries; retained ${plan.protectedTopLevels.size} shared entries`)
  return deleted > 0
}

async function removeLocalData(plan) {
  await removeUserTree(plan, plan.paths.ingestRoot, plan.paths.userRoot, 'local encodings')
  await removeUserTree(plan, plan.paths.stageVideosRoot, plan.paths.stageUser, 'cold staging')
  for (let index = 0; index < plan.paths.trashDirectories.length; index++) {
    await removeUserTree(plan, TRASH_ROOT, plan.paths.trashDirectories[index], `trash directory ${index + 1}`)
  }
  if (plan.protectedDirectories.size === 0) {
    await safeRemoveTree(plan.paths.miscRoot, plan.paths.miscUser, 'local miscellaneous assets')
  } else {
    log('local miscellaneous assets: retained because shared videos remain')
  }
  for (let index = 0; index < plan.paths.coldDeleteDirectories.length; index++) {
    await safeRemoveTree(COLD_DELETE_ROOT, plan.paths.coldDeleteDirectories[index], `cold-delete directory ${index + 1}`)
  }
  for (let index = 0; index < plan.paths.coldRestoreDirectories.length; index++) {
    await safeRemoveTree(COLD_RESTORE_ROOT, plan.paths.coldRestoreDirectories[index], `cold-restore directory ${index + 1}`)
  }
}

async function finalizeDatabase(client, plan, boxUsage) {
  await client.query('BEGIN')
  try {
    const locked = await client.query(
      `SELECT id, username, "isBanned" FROM "User" WHERE id = $1 FOR UPDATE`,
      [plan.user.id]
    )
    if (locked.rows.length !== 1 || !locked.rows[0].isBanned || locked.rows[0].username !== plan.user.username) {
      throw new Error('user identity or banned status changed before database finalization')
    }
    const purgeIds = [...plan.videoIds]
    const staged = await client.query(
      `DELETE FROM cold_storage_staging WHERE "videoId" = ANY($1::text[])`,
      [purgeIds]
    )
    const videos = await client.query(
      `UPDATE "Video"
       SET status = 'hard_deleted',
           "transferStatus" = CASE
             WHEN "transferStatus" IN ('pending', 'transferring') THEN 'error'
             ELSE "transferStatus"
           END,
           "coldStorage" = false,
           "coldStorageId" = NULL,
           "coldStorageStatus" = 'local',
           "coldStoragePath" = NULL,
           "coldStorageChecksum" = NULL,
           "coldStorageBytes" = NULL,
           "archivedAt" = NULL,
           "restoreRequestedAt" = NULL,
           "coldStorageError" = NULL,
           "coldStorageRetryAt" = NULL,
           "coldStorageUpdatedAt" = NOW(),
           "updatedAt" = NOW()
       WHERE "userId" = $1 AND id = ANY($2::text[])`,
      [plan.user.id, purgeIds]
    )
    for (const box of plan.boxes) {
      const usage = boxUsage.get(box.id)
      await client.query(
        `UPDATE storage_boxes
         SET "capacityBytes" = $2::bigint,
             "usedBytes" = $3::bigint,
             "acceptingUploads" = ($2::bigint - $3::bigint > "minimumFreeBytes"),
             "updatedAt" = NOW()
         WHERE id = $1`,
        [box.id, usage.total, usage.used]
      )
    }
    await client.query('COMMIT')
    return { videos: videos.rowCount, stagingRows: staged.rowCount }
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {})
    throw error
  }
}

async function writeAudit(event) {
  await mkdir(path.dirname(AUDIT_LOG), { recursive: true, mode: 0o750 })
  await appendFile(AUDIT_LOG, `${JSON.stringify({ at: new Date().toISOString(), ...event })}\n`, { mode: 0o640 })
}

function printPlan(plan, execute) {
  console.log(execute
    ? '[delete-banned] EXECUTE MODE — deletion is permanent'
    : '[delete-banned] DRY RUN — no files, workers, or database rows will be changed')
  console.log(`[delete-banned] user: ${plan.user.username} (${plan.user.id}); banned: yes`)
  console.log(`[delete-banned] local storage: ${plan.storage.id} (${plan.storage.ip})`)
  console.log(`[delete-banned] videos: ${plan.totals.videos}; recorded encoded bytes: ${formatBytes(plan.totals.encodedBytes)}`)
  console.log(`[delete-banned] purgeable: ${plan.totals.purgeVideos}; protected shared: ${plan.totals.protectedVideos} videos / ${plan.totals.protectedDirectories} directories / ${formatBytes(plan.totals.protectedBytes)}`)
  console.log(`[delete-banned] staged: ${plan.totals.stagedVideos} videos / ${formatBytes(plan.totals.stagedBytes)}`)
  console.log(`[delete-banned] cold: ${plan.totals.coldVideos} videos / ${formatBytes(plan.totals.coldBytes)}`)
  console.log(`[delete-banned] transferring rows: ${plan.totals.transferring}`)
  console.log(`[delete-banned] Storage Boxes checked: ${plan.boxes.length}`)
  console.log(`[delete-banned] local encoding path: ${plan.paths.userRoot}`)
  console.log(`[delete-banned] staging path: ${plan.paths.stageUser}`)
  console.log(`[delete-banned] trash directories: ${plan.paths.trashDirectories.length}`)
  if (plan.inventory) {
    const { local, boxes, total, complete } = plan.inventory
    console.log('[delete-banned] actual filesystem removal inventory:')
    console.log(`[delete-banned]   storage server encodings: ${formatCount(local.encodings.files)} files / ${formatBytes(local.encodings.bytes)}`)
    console.log(`[delete-banned]   staging directory: ${formatCount(local.staging.files)} files / ${formatBytes(local.staging.bytes)}`)
    console.log(`[delete-banned]   trash: ${formatCount(local.trash.files)} files / ${formatBytes(local.trash.bytes)}`)
    console.log(`[delete-banned]   miscellaneous: ${formatCount(local.misc.files)} files / ${formatBytes(local.misc.bytes)}`)
    console.log(`[delete-banned]   cold-delete work area: ${formatCount(local.coldDelete.files)} files / ${formatBytes(local.coldDelete.bytes)}`)
    console.log(`[delete-banned]   cold-restore work area: ${formatCount(local.coldRestore.files)} files / ${formatBytes(local.coldRestore.bytes)}`)
    for (const entry of boxes) {
      const label = `${entry.box.id} (${entry.box.plan || 'Storage Box'}, ${entry.box.hostname})`
      if (entry.error) {
        console.log(`[delete-banned]   Storage Box ${label}: INVENTORY ERROR: ${entry.error}`)
        continue
      }
      console.log(`[delete-banned]   Storage Box ${label} archives: ${formatCount(entry.archives.files)} files / ${formatBytes(entry.archives.bytes)}`)
      console.log(`[delete-banned]   Storage Box ${label} incoming: ${formatCount(entry.incoming.files)} files / ${formatBytes(entry.incoming.bytes)}`)
    }
    console.log(`[delete-banned]   TOTAL PHYSICAL REMOVAL: ${formatCount(total.files)} files / ${formatBytes(total.bytes)}${complete ? '' : ' (incomplete: one or more Storage Boxes could not be inventoried)'}`)
  }
}

function printDryRunUserSummary(plans, planningFailures) {
  console.log('[delete-banned] banned-user removal summary:')
  const grand = emptyInventory()
  let complete = true
  for (const plan of plans) {
    const actual = plan.inventory?.total || emptyInventory()
    addInventory(grand, actual)
    if (!plan.inventory?.complete) complete = false
    console.log(
      `[delete-banned]   ${plan.user.username} (${formatCount(actual.files)} physical files / ${formatBytes(actual.bytes)}): ` +
      `${formatCount(plan.totals.purgeVideos)} video rows targeted; ` +
      `${formatCount(plan.totals.protectedVideos)} shared videos protected`
    )
  }
  console.log(
    `[delete-banned] grand total: ${formatCount(grand.files)} physical files / ${formatBytes(grand.bytes)} to remove` +
    `${complete ? '' : ' (incomplete Storage Box inventory)'}; ${plans.length} users ready; ${planningFailures} skipped`
  )
}

async function purgePlan(client, plan) {
  try {
    await recheckBanned(client, plan.user)
    const boxUsage = new Map()
    for (const box of plan.boxes) {
      const remote = plan.coldByBox.get(box.id) || { finalIds: [], incomingIds: [] }
      boxUsage.set(box.id, await purgeBox(box, plan.user.id, remote, plan.protectedDirectories.size > 0))
    }

    await removeLocalData(plan)
    const database = await finalizeDatabase(client, plan, boxUsage)
    const result = {
      event: 'purge_completed',
      username: plan.user.username,
      userId: plan.user.id,
      storageId: plan.storage.id,
      videos: database.videos,
      protectedVideos: plan.totals.protectedVideos,
      protectedDirectories: plan.totals.protectedDirectories,
      stagingRows: database.stagingRows,
      recordedEncodedBytes: plan.totals.encodedBytes,
      recordedStagedBytes: plan.totals.stagedBytes,
      recordedColdBytes: plan.totals.coldBytes,
      storageBoxes: plan.boxes.map(box => box.id),
    }
    await writeAudit(result).catch(error => log(`warning: could not write audit log: ${error.message}`))
    log(`${plan.user.username}: completed; ${database.videos} video rows finalized; ${database.stagingRows} staging rows removed`)
    return { ok: true, result }
  } catch (error) {
    await writeAudit({
      event: 'purge_failed',
      username: plan.user.username,
      userId: plan.user.id,
      storageId: plan.storage.id,
      error: error.message,
    }).catch(() => {})
    log(`${plan.user.username}: FAILED: ${error.message}`)
    return { ok: false, error }
  }
}

async function executePurges(client, plans) {
  if (plans.length === 0) return { completed: 0, failed: 0 }
  let onlineWorkers = []
  let workersPaused = false
  try {
    onlineWorkers = await pm2OnlineWorkers()
    workersPaused = onlineWorkers.length > 0
    await stopWorkers(onlineWorkers)
    await preflightBoxes(plans[0].boxes)
    let completed = 0
    let failed = 0
    for (const plan of plans) {
      const outcome = await purgePlan(client, plan)
      if (outcome.ok) completed++
      else failed++
    }
    return { completed, failed }
  } finally {
    if (workersPaused) await startWorkers(onlineWorkers)
  }
}

async function main() {
  const options = parseArgs(process.argv.slice(2))
  if (options.help) return usage()
  if (!DATABASE_URL) throw new Error('DATABASE_URL is not configured')
  const client = new pg.Client({
    connectionString: DATABASE_URL,
    application_name: options.execute ? 'delete-banned-execute' : 'delete-banned-dry-run',
  })
  await client.connect()
  try {
    if (!options.execute) await client.query('SET default_transaction_read_only = on')
    const usernames = await selectedUsernames(client, options.username)
    if (usernames.length === 0) {
      console.log('[delete-banned] no banned accounts found')
      return
    }
    console.log(`[delete-banned] selected banned accounts: ${usernames.length}`)
    const plans = []
    let planningFailures = 0
    for (const username of usernames) {
      try {
        const plan = await loadPlan(client, username)
        if (!options.execute) {
          log(`${plan.user.username}: reading actual local and Storage Box file counts for dry run`)
          plan.inventory = await inventoryPlan(plan)
        }
        printPlan(plan, options.execute)
        plans.push(plan)
      } catch (error) {
        planningFailures++
        console.error(`[delete-banned] SKIP ${username}: ${error.message}`)
        if (options.username !== null) throw error
      }
    }
    if (!options.execute) {
      printDryRunUserSummary(plans, planningFailures)
      if (planningFailures > 0) process.exitCode = 1
      return
    }
    const result = await executePurges(client, plans)
    const failed = planningFailures + result.failed
    console.log(`[delete-banned] execute summary: ${result.completed} completed; ${failed} failed/skipped`)
    if (failed > 0) process.exitCode = 1
  } finally {
    await client.end()
  }
}

main().catch(error => {
  console.error(`[delete-banned] ERROR: ${error.message}`)
  process.exitCode = 1
})
