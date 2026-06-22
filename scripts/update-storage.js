import { execSync } from 'node:child_process'
import pg from 'pg'
import { INGEST_DIR } from '../config.js'

const DATABASE_URL = process.env.DATABASE_URL || 'postgresql://firestream:SiMoX1234%40%40@204.168.236.110:5432/openvibe'

function getLocalIp() {
  const raw = execSync("hostname -I", { encoding: "utf8" }).trim()
  return raw.split(/\s+/)[0] // first IP
}

function getDiskUsage(mountPath) {
  // df -B1 gives bytes
  const out = execSync(`df -B1 "${mountPath}"`, { encoding: "utf8" })
  const lines = out.trim().split("\n")
  const parts = lines[1].split(/\s+/)
  const total = parseInt(parts[1], 10)
  const used = parseInt(parts[2], 10)
  const free = parseInt(parts[3], 10)
  return { free, used, total }
}

async function main() {
  const ip = getLocalIp()
  console.log(`[update-storage] this server IP: ${ip}`)
  console.log(`[update-storage] ingest dir: ${INGEST_DIR}`)

  const storage = getDiskUsage(INGEST_DIR)
  console.log(`[update-storage] disk: total=${(storage.total / 1e9).toFixed(1)}GB used=${(storage.used / 1e9).toFixed(1)}GB free=${(storage.free / 1e9).toFixed(1)}GB`)

  const client = new pg.Client({ connectionString: DATABASE_URL })
  await client.connect()

  // Find server by IP
  const row = await client.query('SELECT id FROM "Server" WHERE ip = $1', [ip])
  if (row.rows.length === 0) {
    console.log(`[update-storage] no Server row found for IP ${ip}`)
    await client.end()
    process.exit(1)
  }

  const serverId = row.rows[0].id
  console.log(`[update-storage] updating Server ${serverId}`)

  await client.query(
    'UPDATE "Server" SET storage = $1, "updatedAt" = NOW() WHERE id = $2',
    [JSON.stringify(storage), serverId]
  )

  console.log("[update-storage] done")
  await client.end()
}

main().catch(err => {
  console.error("[update-storage] error:", err.message)
  process.exit(1)
})
