import { readFileSync, existsSync } from 'fs'
import { fileURLToPath } from 'url'
import { dirname, join } from 'path'

const __dirname = dirname(fileURLToPath(import.meta.url))

// Load .env manually (no deps)
function loadEnv() {
  const envPath = join(__dirname, '.env')
  if (!existsSync(envPath)) return
  for (const line of readFileSync(envPath, 'utf8').split('\n')) {
    const trimmed = line.trim()
    if (!trimmed || trimmed.startsWith('#')) continue
    const eq = trimmed.indexOf('=')
    if (eq === -1) continue
    const key = trimmed.slice(0, eq).trim()
    const val = trimmed.slice(eq + 1).trim()
    if (!process.env[key]) process.env[key] = val
  }
}

loadEnv()

export const INGEST_DIR = process.env.INGEST_DIR || '/home/server/encodings'
export const MISC_DIR = process.env.MISC_DIR || '/home/server/misc'
export const PORT = parseInt(process.env.PORT || '2999', 10)

// Allowed IPs — loaded from allowed_ips.json, reloadable via SIGHUP
const IPS_PATH = join(__dirname, 'allowed_ips.json')

let _allowedIps = new Set()

export function loadAllowedIps() {
  if (!existsSync(IPS_PATH)) {
    console.warn('[config] allowed_ips.json not found — no IPs whitelisted')
    _allowedIps = new Set()
    return
  }
  try {
    const raw = JSON.parse(readFileSync(IPS_PATH, 'utf8'))
    const ips = Array.isArray(raw) ? raw : []
    _allowedIps = new Set(ips)
    console.log(`[config] loaded ${_allowedIps.size} allowed IPs`)
  } catch (err) {
    console.error('[config] failed to parse allowed_ips.json:', err.message)
  }
}

export function isAllowed(ip) {
  return _allowedIps.has(ip)
}

// Initial load
loadAllowedIps()

// Reload on SIGHUP
process.on('SIGHUP', () => {
  console.log('[config] SIGHUP received — reloading allowed IPs')
  loadAllowedIps()
})
