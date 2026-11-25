// Keep limiter destinations aligned with the archive worker's active boxes.
import pg from 'pg'
const originalLog = console.log
console.log = console.error
await import('../config.js')
console.log = originalLog
if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL is not configured')
const client = new pg.Client({
  connectionString: process.env.DATABASE_URL,
  connectionTimeoutMillis: 10000,
  statement_timeout: 10000,
})
try {
  await client.connect()
  const { rows } = await client.query('SELECT DISTINCT hostname FROM storage_boxes WHERE "isActive" ORDER BY hostname')
  if (!rows.length) throw new Error('no active Storage Boxes')
  for (const { hostname } of rows) {
    if (!/^[A-Za-z0-9.-]+$/.test(hostname)) throw new Error('invalid Storage Box hostname')
  }
  console.log(rows.map(row => row.hostname).join('\n'))
} finally {
  await client.end()
}
