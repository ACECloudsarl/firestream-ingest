import { execSync } from 'node:child_process'
import { readFileSync } from 'node:fs'
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

function readProcStat() {
  const raw = readFileSync('/proc/stat', 'utf8')
  const line = raw.split('\n')[0] // first line = aggregate CPU
  const parts = line.split(/\s+/).slice(1) // drop 'cpu', keep numbers
  const vals = parts.map(Number)
  // user nice system idle iowait irq softirq steal guest guest_nice
  const idle = vals[3] + (vals[4] || 0) // idle + iowait
  const total = vals.reduce((a, b) => a + b, 0)
  return { idle, total }
}

function getCpuUsage() {
  // Two samples 500ms apart
  const a = readProcStat()
  execSync('sleep 0.5')
  const b = readProcStat()
  const idleDelta = b.idle - a.idle
  const totalDelta = b.total - a.total
  if (totalDelta === 0) return 0
  return Math.round((1 - idleDelta / totalDelta) * 100)
}

function getRamUsage() {
  const raw = readFileSync('/proc/meminfo', 'utf8')
  const get = (key) => {
    const m = raw.match(new RegExp(`^${key}:\\s+(\\d+)`, 'm'))
    return m ? parseInt(m[1], 10) : 0
  }
  const total = get('MemTotal')
  const available = get('MemAvailable')
  if (total === 0) return 0
  return Math.round(((total - available) / total) * 100)
}

function getBlockDevice(mountPath) {
  const raw = execSync(`df "${mountPath}"`, { encoding: 'utf8' })
  const lines = raw.trim().split('\n')
  const dev = lines[1].split(/\s+/)[0] // e.g. /dev/sda1 or /dev/md2
  // Strip /dev/ prefix to get the name used in /proc/diskstats
  return dev.replace('/dev/', '')
}

function readDiskStats(device) {
  const raw = readFileSync('/proc/diskstats', 'utf8')
  for (const line of raw.split('\n')) {
    const parts = line.trim().split(/\s+/)
    if (parts[2] === device) {
      // fields: major minor name reads rmerged rsectors rms writes wmerged wsectors wms inprog ioms iowms
      const sectorsRead = parseInt(parts[5], 10) || 0
      const sectorsWritten = parseInt(parts[9], 10) || 0
      const msReading = parseInt(parts[6], 10) || 0
      const msWriting = parseInt(parts[10], 10) || 0
      const iosInProgress = parseInt(parts[11], 10) || 0
      const msDoingIO = parseInt(parts[12], 10) || 0
      return { sectorsRead, sectorsWritten, msReading, msWriting, iosInProgress, msDoingIO }
    }
  }
  return null
}

function getDiskIO(mountPath) {
  const device = getBlockDevice(mountPath)
  // Try exact match first, then fall back to stripping partition number (sda1 → sda)
  let a = readDiskStats(device)
  if (!a) {
    const base = device.replace(/\d+$/, '')
    if (base !== device) a = readDiskStats(base)
  }
  if (!a) return { readKBps: 0, writeKBps: 0, iowait: 0 }

  execSync('sleep 0.5')

  let b = readDiskStats(device)
  if (!b) {
    const base = device.replace(/\d+$/, '')
    if (base !== device) b = readDiskStats(base)
  }
  if (!b) return { readKBps: 0, writeKBps: 0, iowait: 0 }

  const secs = 0.5
  const readKBps = Math.round(((b.sectorsRead - a.sectorsRead) * 512) / (1024 * secs))
  const writeKBps = Math.round(((b.sectorsWritten - a.sectorsWritten) * 512) / (1024 * secs))
  const iowait = Math.round(((b.msDoingIO - a.msDoingIO) / secs) * 100 / 1000) // % of time doing IO
  return { readKBps, writeKBps, iowait }
}

function getPrimaryInterface() {
  // Use default route to find the primary external interface
  try {
    const raw = execSync('ip route show default 0.0.0.0/0', { encoding: 'utf8' }).trim()
    const m = raw.match(/dev\s+(\S+)/)
    if (m) return m[1]
  } catch { /* fall through */ }
  // Fallback: first non-lo interface from /proc/net/dev
  try {
    const netDev = readFileSync('/proc/net/dev', 'utf8')
    for (const line of netDev.split('\n')) {
      const parts = line.trim().split(/\s+/)
      const name = (parts[0] || '').replace(':', '')
      if (name && name !== 'lo' && !name.startsWith('Inter')) return name
    }
  } catch { /* fall through */ }
  return null
}

function readNetDev(iface) {
  const raw = readFileSync('/proc/net/dev', 'utf8')
  for (const line of raw.split('\n')) {
    const parts = line.trim().split(/\s+/)
    if (parts[0] === iface + ':') {
      // fields: face rxBytes rxPkts rxErrs rxDrop ... txBytes txPkts txErrs ...
      const rxBytes = parseInt(parts[1], 10) || 0
      const txBytes = parseInt(parts[9], 10) || 0
      return { rxBytes, txBytes }
    }
  }
  return null
}

// Read current cumulative counters once (no sleep) — used for the delta method
function getCurrentNetCounters() {
  const iface = getPrimaryInterface()
  if (!iface) return null
  return readNetDev(iface)
}

// 500ms snapshot — used only as fallback on first run or after counter reset
function getNetworkSpeedSnapshot() {
  const iface = getPrimaryInterface()
  if (!iface) return { inSpeed: 0, outSpeed: 0 }

  const a = readNetDev(iface)
  if (!a) return { inSpeed: 0, outSpeed: 0 }

  execSync('sleep 0.5')

  const b = readNetDev(iface)
  if (!b) return { inSpeed: 0, outSpeed: 0 }

  const secs = 0.5
  const rxDelta = b.rxBytes - a.rxBytes
  const txDelta = b.txBytes - a.txBytes
  // Clamp negative deltas (counter reset / interface restart) to 0
  const inSpeed = Math.max(0, Math.round(rxDelta / secs))
  const outSpeed = Math.max(0, Math.round(txDelta / secs))
  return { inSpeed, outSpeed }
}

// Compute network speed from cumulative counter delta since last DB write.
// This captures 100% of bytes transferred — not just a 500ms window.
function computeNetworkFromDelta(currentRx, currentTx, prevRx, prevTx, elapsedSec) {
  if (elapsedSec <= 0) return null
  // Counter reset or interface restart detected — can't use delta
  if (currentRx < prevRx || currentTx < prevTx) return null
  const inSpeed = Math.round((currentRx - prevRx) / elapsedSec)
  const outSpeed = Math.round((currentTx - prevTx) / elapsedSec)
  return { inSpeed, outSpeed }
}

async function main() {
  const ip = getLocalIp()
  console.log(`[update-storage] this server IP: ${ip}`)
  console.log(`[update-storage] ingest dir: ${INGEST_DIR}`)

  const storage = getDiskUsage(INGEST_DIR)
  console.log(`[update-storage] disk: total=${(storage.total / 1e9).toFixed(1)}GB used=${(storage.used / 1e9).toFixed(1)}GB free=${(storage.free / 1e9).toFixed(1)}GB`)

  const cpu = getCpuUsage()
  const ram = getRamUsage()
  const diskIO = getDiskIO(INGEST_DIR)

  // Read current /proc/net/dev counters once (no sleep)
  const netCounters = getCurrentNetCounters()

  const client = new pg.Client({ connectionString: DATABASE_URL })
  await client.connect()

  // Find server by IP AND fetch previous network counters for delta calc
  const row = await client.query(
    'SELECT id, "sysUsage" FROM "Server" WHERE ip = $1',
    [ip]
  )
  if (row.rows.length === 0) {
    console.log(`[update-storage] no Server row found for IP ${ip}`)
    await client.end()
    process.exit(1)
  }

  const serverId = row.rows[0].id
  const prevSysUsage = row.rows[0].sysUsage || {}
  const prevNetwork = prevSysUsage.network || {}
  const prevRx = prevNetwork.lastRxBytes
  const prevTx = prevNetwork.lastTxBytes
  const prevTs = prevNetwork.lastTimestamp  // epoch ms, timezone-safe

  // Compute network speed: prefer cumulative delta (100% accurate), fall back to 500ms snapshot
  let network = { inSpeed: 0, outSpeed: 0, lastRxBytes: 0, lastTxBytes: 0, lastTimestamp: Date.now() }

  if (netCounters) {
    // Always seed the counters for next run
    network.lastRxBytes = netCounters.rxBytes
    network.lastTxBytes = netCounters.txBytes

    if (prevRx != null && prevTx != null && prevTs != null) {
      const elapsedSec = (Date.now() - prevTs) / 1000
      const delta = computeNetworkFromDelta(
        netCounters.rxBytes, netCounters.txBytes,
        prevRx, prevTx, elapsedSec
      )
      if (delta) {
        network.inSpeed = delta.inSpeed
        network.outSpeed = delta.outSpeed
        console.log(`[update-storage] net delta: elapsed=${elapsedSec.toFixed(0)}s deltaRx=${(netCounters.rxBytes - prevRx)}B deltaTx=${(netCounters.txBytes - prevTx)}B`)
      } else {
        // Counter reset detected — fall back to snapshot, counters already seeded above
        console.log(`[update-storage] net counter reset detected, using fallback snapshot`)
        const snap = getNetworkSpeedSnapshot()
        network.inSpeed = snap.inSpeed
        network.outSpeed = snap.outSpeed
      }
    } else {
      // First run — no previous counters, use snapshot to get an initial reading
      console.log(`[update-storage] net first run, using snapshot and seeding counters`)
      const snap = getNetworkSpeedSnapshot()
      network.inSpeed = snap.inSpeed
      network.outSpeed = snap.outSpeed
    }
  }

  const sysUsage = { cpu, ram, diskIO, network }
  console.log(`[update-storage] sysUsage: cpu=${cpu}% ram=${ram}% diskIO=${diskIO.readKBps}KB/s read / ${diskIO.writeKBps}KB/s write / ${diskIO.iowait}% iowait | net: in=${(network.inSpeed / 1e6).toFixed(2)}MB/s out=${(network.outSpeed / 1e6).toFixed(2)}MB/s`)

  console.log(`[update-storage] updating Server ${serverId}`)

  await client.query(
    'UPDATE "Server" SET storage = $1, "sysUsage" = $2, "updatedAt" = NOW() WHERE id = $3',
    [JSON.stringify(storage), JSON.stringify(sysUsage), serverId]
  )

  console.log("[update-storage] done")
  await client.end()
}

main().catch(err => {
  console.error("[update-storage] error:", err.message)
  process.exit(1)
})
