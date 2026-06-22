import fs from 'node:fs'
import path from 'node:path'
import { execSync } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import pg from 'pg'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const IPS_FILE = path.join(__dirname, '..', 'allowed_ips.json')
const DATABASE_URL = process.env.DATABASE_URL || 'postgresql://firestream:SiMoX1234%40%40@204.168.236.110:5432/openvibe'

async function main() {
  console.log('[update-ips] querying Server table for active encoder IPs...')

  const client = new pg.Client({ connectionString: DATABASE_URL })
  await client.connect()

  const rows = await client.query(
    'SELECT ip FROM "Server" WHERE ip IS NOT NULL AND ip != \'\' AND "isActive" = true'
  )
  await client.end()

  const ips = [...new Set(rows.rows.map(r => r.ip))]

  if (ips.length === 0) {
    console.log('[update-ips] WARNING: no active server IPs found')
  }

  fs.writeFileSync(IPS_FILE, JSON.stringify(ips, null, 2) + '\n')
  console.log(`[update-ips] wrote ${ips.length} IPs to ${IPS_FILE}: ${ips.join(', ')}`)

  // SIGHUP the ingest process to reload without restart
  try {
    const pid = execSync("pgrep -f 'node.*ingest.js'", { encoding: 'utf8' }).trim().split('\n')[0]
    if (pid) {
      process.kill(parseInt(pid, 10), 'SIGHUP')
      console.log(`[update-ips] sent SIGHUP to ingest (pid ${pid})`)
    }
  } catch {
    console.log('[update-ips] ingest process not running — IPs written, will load on next start')
  }
}

main().catch(err => {
  console.error('[update-ips] error:', err.message)
  process.exit(1)
})
