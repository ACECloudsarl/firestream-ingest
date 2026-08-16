import {
  appendFileSync,
  closeSync,
  existsSync,
  fsyncSync,
  lstatSync,
  mkdirSync,
  openSync,
  readFileSync,
  realpathSync,
  renameSync,
  statSync,
} from 'node:fs'
import path from 'node:path'
import pg from 'pg'
import { INGEST_DIR } from '../config.js'

const DATABASE_URL = process.env.DATABASE_URL
const ADVISORY_LOCK_KEY = 1_947_323_821
const TRASH_ROOT = process.env.TRASH_DIR || path.join(path.dirname(path.resolve(INGEST_DIR)), 'trash')

function usage() {
  console.log(`Usage: node scripts/restore-videos-from-trash.js --manifest PATH [--execute]

Dry-run is the default. Execute mode atomically restores directories and
guardedly restores DB statuses recorded by the selected cleanup manifest.
`)
}

function parseArgs(argv) {
  const options = { execute: false, manifest: null }
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]
    if (arg === '--help') options.help = true
    else if (arg === '--execute') options.execute = true
    else if (arg === '--manifest') options.manifest = argv[++i]
    else throw new Error(`Unknown option: ${arg}`)
  }
  return options
}

function isBelow(root, target) {
  return target !== root && target.startsWith(`${root}${path.sep}`)
}

function appendRestoreLog(fd, event) {
  appendFileSync(fd, `${JSON.stringify({ at: new Date().toISOString(), ...event })}\n`)
  fsyncSync(fd)
}

function readManifest(manifestPath) {
  const moves = new Map()
  const statusChanges = new Map()
  for (const [index, line] of readFileSync(manifestPath, 'utf8').split('\n').entries()) {
    if (!line.trim()) continue
    let event
    try {
      event = JSON.parse(line)
    } catch {
      throw new Error(`invalid JSON on manifest line ${index + 1}`)
    }
    if (['move_intent', 'moved'].includes(event.event) && event.sourcePath && event.trashPath) {
      moves.set(path.resolve(event.trashPath), event)
    }
    if (Array.isArray(event.statusChanges)) {
      for (const change of event.statusChanges) {
        if (!change.videoId || !change.fromStatus || !change.expectedUpdatedAt) continue
        statusChanges.set(change.videoId, {
          ...change,
          sourcePath: event.sourcePath ? path.resolve(event.sourcePath) : null,
          manifestEvent: event.event,
        })
      }
    }
  }
  return { moves: [...moves.values()], statusChanges: [...statusChanges.values()] }
}

function classifyPhysicalMoves(records, ingestRoot, trashRoot) {
  const summary = { recorded: records.length, restorable: 0, already_restored: 0, trash_missing: 0, source_conflict: 0, unsafe: 0 }
  const actions = []
  for (const record of [...records].reverse()) {
    const sourcePath = path.resolve(record.sourcePath)
    const trashPath = path.resolve(record.trashPath)
    if (!isBelow(ingestRoot, sourcePath) || !isBelow(trashRoot, trashPath)) {
      summary.unsafe++
      continue
    }
    const sourceExists = existsSync(sourcePath)
    const trashExists = existsSync(trashPath)
    if (sourceExists && !trashExists) {
      summary.already_restored++
      continue
    }
    if (sourceExists && trashExists) {
      summary.source_conflict++
      continue
    }
    if (!trashExists) {
      summary.trash_missing++
      continue
    }
    try {
      const stat = lstatSync(trashPath)
      if (!stat.isDirectory() || stat.isSymbolicLink()) {
        summary.unsafe++
        continue
      }
    } catch {
      summary.unsafe++
      continue
    }
    summary.restorable++
    actions.push({ ...record, sourcePath, trashPath })
  }
  return { summary, actions }
}

async function classifyDbChanges(client, changes, availableSourcePaths) {
  if (changes.length === 0) return { restorable: [], conflicts: 0, missing: 0 }
  const ids = changes.map(change => change.videoId)
  const { rows } = await client.query(
    `SELECT id, status, reason, "updatedAt" FROM "Video" WHERE id = ANY($1::text[])`,
    [ids]
  )
  const current = new Map(rows.map(row => [row.id, row]))
  const restorable = []
  let conflicts = 0
  let missing = 0
  for (const change of changes) {
    const row = current.get(change.videoId)
    if (!row) {
      missing++
      continue
    }
    const expected = new Date(change.expectedUpdatedAt).getTime()
    const actual = new Date(row.updatedAt).getTime()
    if (row.status !== 'hard_deleted' || !Number.isFinite(expected) || expected !== actual) {
      conflicts++
      continue
    }
    if (change.sourcePath && !availableSourcePaths.has(change.sourcePath)) {
      conflicts++
      continue
    }
    restorable.push(change)
  }
  return { restorable, conflicts, missing }
}

async function main() {
  const options = parseArgs(process.argv.slice(2))
  if (options.help) return usage()
  if (!options.manifest) throw new Error('--manifest is required')
  if (!DATABASE_URL) throw new Error('DATABASE_URL is not configured')

  const ingestRoot = realpathSync(INGEST_DIR)
  const trashRoot = realpathSync(TRASH_ROOT)
  const manifestPath = realpathSync(options.manifest)
  if (!isBelow(trashRoot, manifestPath) || path.basename(manifestPath) !== 'manifest.txt') {
    throw new Error('manifest must be a manifest.txt below TRASH_DIR')
  }
  if (statSync(ingestRoot).dev !== statSync(trashRoot).dev) {
    throw new Error('trash and ingest directories are not on the same filesystem')
  }

  const records = readManifest(manifestPath)
  const physical = classifyPhysicalMoves(records.moves, ingestRoot, trashRoot)
  const availableSourcePaths = new Set(records.moves
    .map(record => ({ sourcePath: path.resolve(record.sourcePath), trashPath: path.resolve(record.trashPath) }))
    .filter(record => existsSync(record.sourcePath) || existsSync(record.trashPath))
    .map(record => record.sourcePath))
  const client = new pg.Client({
    connectionString: DATABASE_URL,
    application_name: options.execute ? 'firestream-trash-restore' : 'firestream-restore-dry-run',
  })
  await client.connect()
  let logFd = null
  let lockHeld = false
  const summary = {
    physical: physical.summary,
    physical_restored: 0,
    db_recorded: records.statusChanges.length,
    db_restorable: 0,
    db_restored: 0,
    db_conflicts: 0,
    db_missing: 0,
    errors: 0,
  }

  try {
    await client.query("SET statement_timeout = '30s'")
    await client.query("SET lock_timeout = '5s'")
    if (!options.execute) await client.query('SET default_transaction_read_only = on')
    const lock = await client.query(`SELECT pg_try_advisory_lock($1) AS acquired`, [ADVISORY_LOCK_KEY])
    if (!lock.rows[0].acquired) throw new Error('cleanup or another restore is currently running')
    lockHeld = true

    const db = await classifyDbChanges(client, records.statusChanges, availableSourcePaths)
    summary.db_restorable = db.restorable.length
    summary.db_conflicts = db.conflicts
    summary.db_missing = db.missing

    console.log(`[restore] mode: ${options.execute ? 'EXECUTE' : 'DRY RUN'}`)
    console.log(`[restore] manifest: ${manifestPath}`)
    console.log(`[restore] physical ready: ${physical.actions.length}; DB rows ready: ${db.restorable.length}`)

    if (options.execute) {
      const restoreLog = path.join(path.dirname(manifestPath), 'restore-log.txt')
      logFd = openSync(restoreLog, 'a', 0o640)
      appendRestoreLog(logFd, { event: 'restore_started', manifestPath, physical: physical.actions.length, database: db.restorable.length })

      for (const action of physical.actions) {
        try {
          mkdirSync(path.dirname(action.sourcePath), { recursive: true, mode: 0o755 })
          appendRestoreLog(logFd, { event: 'restore_intent', sourcePath: action.sourcePath, trashPath: action.trashPath })
          renameSync(action.trashPath, action.sourcePath)
          appendRestoreLog(logFd, { event: 'restored', sourcePath: action.sourcePath, trashPath: action.trashPath })
          summary.physical_restored++
        } catch (err) {
          summary.errors++
          appendRestoreLog(logFd, { event: 'restore_failed', sourcePath: action.sourcePath, trashPath: action.trashPath, error: err.message })
        }
      }

      for (const change of db.restorable) {
        if (change.sourcePath && !existsSync(change.sourcePath)) {
          summary.db_conflicts++
          continue
        }
        try {
          const { rowCount } = await client.query(
            `UPDATE "Video"
             SET status = $1, reason = $2, "updatedAt" = NOW()
             WHERE id = $3
               AND status = 'hard_deleted'
               AND "updatedAt" = $4::timestamp`,
            [change.fromStatus, change.fromReason, change.videoId, change.expectedUpdatedAt]
          )
          if (rowCount === 1) {
            summary.db_restored++
            appendRestoreLog(logFd, { event: 'db_status_restored', ...change })
          } else {
            summary.db_conflicts++
            appendRestoreLog(logFd, { event: 'db_restore_conflict', ...change })
          }
        } catch (err) {
          summary.errors++
          appendRestoreLog(logFd, { event: 'db_restore_failed', error: err.message, ...change })
        }
      }
      appendRestoreLog(logFd, { event: 'restore_completed', summary })
    }

    console.log(JSON.stringify(summary, null, 2))
    if (!options.execute) console.log('[restore] dry-run complete — nothing was changed')
  } finally {
    if (lockHeld) {
      try { await client.query(`SELECT pg_advisory_unlock($1)`, [ADVISORY_LOCK_KEY]) } catch {}
    }
    if (logFd !== null) closeSync(logFd)
    await client.end()
  }
}

main().catch(err => {
  console.error(`[restore] ERROR: ${err.message}`)
  process.exitCode = 1
})
