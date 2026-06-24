import fs from 'node:fs'
import path from 'node:path'
import { execSync } from 'node:child_process'
import pg from 'pg'
import { INGEST_DIR } from '../config.js'

const DATABASE_URL = process.env.DATABASE_URL || 'postgresql://firestream:SiMoX1234%40%40@204.168.236.110:5432/openvibe'

// Only process videos assigned to this storage server (or null — legacy)
function getLocalStorageId() {
  const ip = execSync('hostname -I', { encoding: 'utf8' }).trim().split(/\s+/)[0]
  return ip
}

// Recursively sum file sizes in a directory
function getDirSize(dirPath) {
  let total = 0
  function walk(d) {
    let entries
    try {
      entries = fs.readdirSync(d, { withFileTypes: true })
    } catch {
      return // directory doesn't exist or is inaccessible — skip
    }
    for (const entry of entries) {
      const full = path.join(d, entry.name)
      if (entry.isDirectory()) {
        walk(full)
      } else if (entry.isFile()) {
        try {
          total += fs.statSync(full).size
        } catch {
          // file disappeared — ignore
        }
      }
    }
  }
  walk(dirPath)
  return total
}

function formatBytes(bytes) {
  if (bytes < 1024) return `${bytes}B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)}KB`
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)}MB`
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)}GB`
}

async function main() {
  const localIp = getLocalStorageId()
  console.log(`[backfill] local IP: ${localIp}`)
  console.log(`[backfill] ingest dir: ${INGEST_DIR}`)

  const client = new pg.Client({ connectionString: DATABASE_URL })
  await client.connect()

  // Resolve this server's storageId from the Server table
  const serverRow = await client.query(
    'SELECT id FROM "Server" WHERE ip = $1',
    [localIp]
  )
  if (serverRow.rows.length === 0) {
    console.log(`[backfill] ERROR: no Server row for IP ${localIp}`)
    await client.end()
    process.exit(1)
  }
  const storageId = serverRow.rows[0].id
  console.log(`[backfill] storageId: ${storageId}`)

  // Find all completed videos on this server with encodedSize = 0
  const result = await client.query(
    `SELECT id, "userId"
     FROM "Video"
     WHERE "encodedSize" = 0
       AND "encodingStatus" = 'completed'
       AND ("storageId" = $1 OR "storageId" IS NULL)`,
    [storageId]
  )

  const videos = result.rows
  console.log(`[backfill] found ${videos.length} videos to process`)

  let updated = 0
  let skippedNoDir = 0
  let skippedEmpty = 0
  let errors = 0

  for (let i = 0; i < videos.length; i++) {
    const { id: videoId, userId } = videos[i]
    const videoDir = path.join(INGEST_DIR, userId, videoId)

    // Check if directory exists
    let dirExists = false
    try {
      dirExists = fs.statSync(videoDir).isDirectory()
    } catch {
      dirExists = false
    }

    if (!dirExists) {
      skippedNoDir++
      if (skippedNoDir <= 5) {
        console.log(`[backfill] SKIP ${videoId}: directory not found (${videoDir})`)
      }
      continue
    }

    const encodedSize = getDirSize(videoDir)

    if (encodedSize === 0) {
      skippedEmpty++
      if (skippedEmpty <= 5) {
        console.log(`[backfill] SKIP ${videoId}: directory is empty`)
      }
      continue
    }

    try {
      await client.query(
        'UPDATE "Video" SET "encodedSize" = $1, "updatedAt" = NOW() WHERE id = $2',
        [encodedSize, videoId]
      )
      updated++
      if (updated % 50 === 0 || updated <= 5) {
        console.log(`[backfill] ${updated}/${videos.length} ${videoId}: ${formatBytes(encodedSize)}`)
      }
    } catch (err) {
      errors++
      console.log(`[backfill] ERROR updating ${videoId}: ${err.message}`)
    }
  }

  console.log(`\n[backfill] DONE`)
  console.log(`  total found:    ${videos.length}`)
  console.log(`  updated:        ${updated}`)
  console.log(`  skipped (no dir): ${skippedNoDir}`)
  console.log(`  skipped (empty):  ${skippedEmpty}`)
  console.log(`  errors:         ${errors}`)

  await client.end()
}

main().catch(err => {
  console.error('[backfill] fatal error:', err.message)
  process.exit(1)
})
