import http from 'node:http'
import fs from 'node:fs'
import path from 'node:path'
import { spawn } from 'node:child_process'
import pg from 'pg'
import busboy from 'busboy'
import { INGEST_DIR, MISC_DIR } from '../config.js'

const PORT = parseInt(process.env.POSTER_UPLOAD_PORT || '3001', 10)
const DATABASE_URL = process.env.DATABASE_URL
const MAX_POSTER_SIZE = 10 * 1024 * 1024 // 10 MB

function log(msg) {
	const ts = new Date().toISOString()
	console.log(`[${ts}] ${msg}`)
}

// ─── DB pool ─────────────────────────────────────────────────────────────────

let pool = null

function getPool() {
	if (!pool) {
		if (!DATABASE_URL) {
			throw new Error('DATABASE_URL is not set')
		}
		pool = new pg.Pool({
			connectionString: DATABASE_URL,
			max: 5,
			idleTimeoutMillis: 30000,
		})
	}
	return pool
}

// ─── Multipart parser ────────────────────────────────────────────────────────

function parseMultipart(req) {
	return new Promise((resolve, reject) => {
		const bb = busboy({
			headers: req.headers,
			limits: { fileSize: MAX_POSTER_SIZE, files: 1 },
		})
		const fields = {}
		const files = {}

		bb.on('field', (name, val) => {
			fields[name] = val
		})

		bb.on('file', (name, stream, info) => {
			const { filename, mimeType } = info
			const chunks = []
			stream.on('data', chunk => chunks.push(chunk))
			stream.on('end', () => {
				files[name] = {
					buffer: Buffer.concat(chunks),
					filename,
					mimeType,
				}
			})
		})

		bb.on('filesLimit', () => {
			reject(new Error('Too many files'))
		})

		bb.on('close', () => {
			resolve({ fields, files })
		})

		bb.on('error', (err) => {
			reject(err)
		})

		req.pipe(bb)
	})
}

// ─── ffmpeg / ffprobe helpers ────────────────────────────────────────────────

function spawnAsync(cmd, args) {
	return new Promise((resolve, reject) => {
		const child = spawn(cmd, args, { stdio: ['ignore', 'pipe', 'pipe'] })
		let stderr = ''
		child.stderr.on('data', d => { stderr += d.toString() })
		child.on('close', code => {
			if (code === 0) resolve()
			else reject(new Error(`${cmd} exited ${code}: ${stderr.slice(-300).trim()}`))
		})
		child.on('error', reject)
	})
}

function getDuration(inputPath) {
	return new Promise((resolve) => {
		const child = spawn('ffprobe', [
			'-v', 'quiet', '-print_format', 'json', '-show_format', inputPath,
		], { stdio: ['ignore', 'pipe', 'ignore'] })
		let stdout = ''
		child.stdout.on('data', d => { stdout += d.toString() })
		child.on('close', code => {
			if (code !== 0) return resolve(0)
			try { resolve(parseFloat(JSON.parse(stdout).format?.duration) || 0) }
			catch { resolve(0) }
		})
		child.on('error', () => resolve(0))
	})
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function formatBytes(bytes) {
	if (bytes < 1024) return `${bytes}B`
	if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)}KB`
	return `${(bytes / (1024 * 1024)).toFixed(1)}MB`
}

const CORS_HEADERS = {
	'Access-Control-Allow-Origin': '*',
	'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
	'Access-Control-Allow-Headers': 'Content-Type',
}

function json(res, status, body) {
	res.writeHead(status, { ...CORS_HEADERS, 'Content-Type': 'application/json' })
	res.end(JSON.stringify(body))
}

// ─── Handler ─────────────────────────────────────────────────────────────────

async function handlePosterUpload(req, res) {
	const ip = req.socket.remoteAddress?.replace('::ffff:', '') || 'unknown'

	// Parse video_id from URL: /upload_poster/{video_id}
	const urlPath = req.url.split('?')[0]
	const parts = urlPath.split('/').filter(Boolean)

	if (parts.length !== 2 || parts[0] !== 'upload_poster') {
		return json(res, 400, { error: 'Invalid path' })
	}

	const videoId = parts[1]

	// Validate videoId
	if (videoId.includes('..') || videoId.includes('/') || videoId.includes('\\')) {
		return json(res, 400, { error: 'Invalid videoId' })
	}

	// Parse multipart form
	let fields, files
	try {
		const result = await parseMultipart(req)
		fields = result.fields
		files = result.files
	} catch (err) {
		log(`ERROR parse multipart: ${err.message}`)
		return json(res, 400, { error: 'Failed to parse form data' })
	}

	// ── Auth ────────────────────────────────────────────────
	let userId = (fields.userId || '').trim()
	const key = (fields.key || '').trim()

	if (key) {
		try {
			const db = getPool()
			const result = await db.query(
				'SELECT id FROM "User" WHERE "apiKey" = $1 LIMIT 1',
				[key],
			)
			if (result.rows.length === 0) {
				return json(res, 401, { error: 'Invalid API key' })
			}
			userId = result.rows[0].id
		} catch (err) {
			log(`ERROR db auth lookup: ${err.message}`)
			return json(res, 500, { error: 'Auth lookup failed' })
		}
	}

	if (!userId) {
		return json(res, 400, { error: 'userId or key is required' })
	}

	// Validate userId
	if (userId.includes('..') || userId.includes('/') || userId.includes('\\')) {
		return json(res, 400, { error: 'Invalid userId' })
	}

	// ── Get uploaded file ───────────────────────────────────
	const uploadedFile = files.file
	if (!uploadedFile || !uploadedFile.buffer || uploadedFile.buffer.length === 0) {
		return json(res, 400, { error: 'No file uploaded or file is empty' })
	}

	// ── Ownership check ─────────────────────────────────────
	try {
		const db = getPool()
		const row = await db.query(
			'SELECT id, "userId" FROM "Video" WHERE id = $1',
			[videoId],
		)
		if (row.rows.length === 0) {
			return json(res, 404, { error: 'Video not found' })
		}
		if (row.rows[0].userId !== userId) {
			return json(res, 403, { error: 'Not your video' })
		}
	} catch (err) {
		log(`ERROR db video lookup: ${err.message}`)
		return json(res, 500, { error: 'Database error' })
	}

	// ── Save to disk ────────────────────────────────────────
	const posterDir = path.join(MISC_DIR, userId, 'posters')
	const posterFilename = `${videoId}-poster.jpg`
	const posterPath = path.join(posterDir, posterFilename)

	try {
		fs.mkdirSync(posterDir, { recursive: true })
		fs.writeFileSync(posterPath, uploadedFile.buffer)
	} catch (err) {
		log(`ERROR write poster ${posterPath}: ${err.message}`)
		return json(res, 500, { error: 'Failed to save file' })
	}

	// ── Update DB ───────────────────────────────────────────
	try {
		const db = getPool()
		await db.query(
			'UPDATE "Video" SET poster = $1, "updatedAt" = NOW() WHERE id = $2',
			[posterFilename, videoId],
		)
	} catch (err) {
		log(`ERROR db update poster for ${videoId}: ${err.message}`)
		// File saved but DB update failed — still return success with warning
		return json(res, 200, {
			videoId,
			poster: posterFilename,
			warning: 'File saved but database update failed',
		})
	}

	log(`POSTER ${videoId}: uploaded by ${userId} (${formatBytes(uploadedFile.buffer.length)}) from ${ip}`)

	return json(res, 200, {
		videoId,
		poster: posterFilename,
	})
}

// ─── Regenerate poster handler ───────────────────────────────────────────────

async function handleRegeneratePoster(req, res) {
	const ip = req.socket.remoteAddress?.replace('::ffff:', '') || 'unknown'

	// Parse video_id and query params from URL
	const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`)
	const parts = url.pathname.split('/').filter(Boolean)

	if (parts.length !== 2 || parts[0] !== 'regenerate_poster') {
		return json(res, 400, { error: 'Invalid path' })
	}

	const videoId = parts[1]

	// Validate videoId
	if (videoId.includes('..') || videoId.includes('/') || videoId.includes('\\')) {
		return json(res, 400, { error: 'Invalid videoId' })
	}

	// ── Auth ────────────────────────────────────────────────
	let userId = (url.searchParams.get('userId') || '').trim()
	const key = (url.searchParams.get('key') || '').trim()

	if (key) {
		try {
			const db = getPool()
			const result = await db.query(
				'SELECT id FROM "User" WHERE "apiKey" = $1 LIMIT 1',
				[key],
			)
			if (result.rows.length === 0) {
				return json(res, 401, { error: 'Invalid API key' })
			}
			userId = result.rows[0].id
		} catch (err) {
			log(`ERROR db auth lookup: ${err.message}`)
			return json(res, 500, { error: 'Auth lookup failed' })
		}
	}

	if (!userId) {
		return json(res, 400, { error: 'userId or key is required' })
	}

	// Validate userId
	if (userId.includes('..') || userId.includes('/') || userId.includes('\\')) {
		return json(res, 400, { error: 'Invalid userId' })
	}

	// ── Ownership check ─────────────────────────────────────
	try {
		const db = getPool()
		const row = await db.query(
			'SELECT id, "userId" FROM "Video" WHERE id = $1',
			[videoId],
		)
		if (row.rows.length === 0) {
			return json(res, 404, { error: 'Video not found' })
		}
		if (row.rows[0].userId !== userId) {
			return json(res, 403, { error: 'Not your video' })
		}
	} catch (err) {
		log(`ERROR db video lookup: ${err.message}`)
		return json(res, 500, { error: 'Database error' })
	}

	// ── Find encoded MP4 ────────────────────────────────────
	const videoDir = path.join(INGEST_DIR, userId, videoId)
	const videoPath = path.join(videoDir, 'video.mp4')

	try {
		fs.accessSync(videoPath, fs.constants.R_OK)
	} catch {
		log(`REGENERATE_POSTER ${videoId}: encoded MP4 not found at ${videoPath}`)
		return json(res, 404, { error: 'Encoded video not found' })
	}

	// ── Get user's grid size preference ─────────────────────
	let gridSize = 3
	try {
		const db = getPool()
		const userRow = await db.query(
			'SELECT settings FROM "User" WHERE id = $1',
			[userId],
		)
		if (userRow.rows.length > 0 && userRow.rows[0].settings) {
			const settings = typeof userRow.rows[0].settings === 'string'
				? JSON.parse(userRow.rows[0].settings)
				: userRow.rows[0].settings
			if (settings.thumbnailGridLayout && Number.isInteger(settings.thumbnailGridLayout)) {
				gridSize = settings.thumbnailGridLayout
			}
		}
	} catch (err) {
		log(`WARN could not read user settings: ${err.message}`)
		// Non-fatal — use default gridSize
	}

	// ── Get video duration ──────────────────────────────────
	const duration = await getDuration(videoPath)
	if (duration <= 0) {
		return json(res, 500, { error: 'Could not determine video duration' })
	}

	// ── Build ffmpeg command ────────────────────────────────
	const numFrames = gridSize * gridSize
	const startSec = duration * 0.05
	const endSec = duration * 0.95
	const usable = endSec - startSec
	const fpsValue = numFrames / usable
	const cellW = Math.floor(1280 / gridSize)

	const posterDir = path.join(MISC_DIR, userId, 'posters')
	const posterFilename = `${videoId}-poster.jpg`
	const posterPath = path.join(posterDir, posterFilename)

	const ffmpegArgs = [
		'-y',
		'-ss', startSec.toString(),
		'-i', videoPath,
		'-vf', `fps=${fpsValue}:round=up,scale=${cellW}:-1,tile=${gridSize}x${gridSize}:margin=3:padding=3:color=black`,
		'-frames:v', '1',
		posterPath,
	]

	log(`REGENERATE_POSTER ${videoId}: grid=${gridSize}x${gridSize} frames=${numFrames} duration=${duration.toFixed(1)}s`)

	try {
		fs.mkdirSync(posterDir, { recursive: true })
		await spawnAsync('ffmpeg', ffmpegArgs)
	} catch (err) {
		log(`ERROR ffmpeg poster ${videoId}: ${err.message}`)
		// Clean up partial output on failure
		try { fs.unlinkSync(posterPath) } catch {}
		return json(res, 500, { error: `Poster generation failed: ${err.message}` })
	}

	// ── Update DB ───────────────────────────────────────────
	try {
		const db = getPool()
		await db.query(
			'UPDATE "Video" SET poster = $1, "updatedAt" = NOW() WHERE id = $2',
			[posterFilename, videoId],
		)
	} catch (err) {
		log(`ERROR db update poster for ${videoId}: ${err.message}`)
		return json(res, 200, {
			videoId,
			poster: posterFilename,
			warning: 'Poster generated but database update failed',
		})
	}

	log(`REGENERATE_POSTER ${videoId}: done`)

	return json(res, 200, {
		videoId,
		poster: posterFilename,
	})
}

// ─── Server ──────────────────────────────────────────────────────────────────

const server = http.createServer((req, res) => {
	// CORS preflight
	if (req.method === 'OPTIONS') {
		res.writeHead(204, CORS_HEADERS)
		return res.end()
	}

	// POST /upload_poster/{video_id}
	if (req.method === 'POST' && req.url.startsWith('/upload_poster/')) {
		return handlePosterUpload(req, res).catch(err => {
			log(`UNHANDLED ${err.message}`)
			json(res, 500, { error: 'Internal server error' })
		})
	}

	// GET /regenerate_poster/{video_id}
	if (req.method === 'GET' && req.url.startsWith('/regenerate_poster/')) {
		return handleRegeneratePoster(req, res).catch(err => {
			log(`UNHANDLED ${err.message}`)
			json(res, 500, { error: 'Internal server error' })
		})
	}

	json(res, 405, { error: 'Method not allowed' })
})

server.listen(PORT, () => {
	log(`poster-upload-server listening on :${PORT}`)
	log(`misc dir: ${MISC_DIR}`)
})
