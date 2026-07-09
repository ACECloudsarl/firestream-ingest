import http from 'node:http'
import fs from 'node:fs'
import path from 'node:path'
import crypto from 'node:crypto'
import { Readable } from 'node:stream'
import pg from 'pg'
import busboy from 'busboy'
import srtToVtt from 'srt-to-vtt'
import { MISC_DIR } from '../config.js'

const PORT = parseInt(process.env.SUBTITLE_PORT || '3002', 10)
const DATABASE_URL = process.env.DATABASE_URL
const MAX_SUBTITLE_SIZE = 1 * 1024 * 1024 // 1 MB
const ALLOWED_EXTENSIONS = new Set(['.vtt', '.srt'])

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
			limits: { fileSize: MAX_SUBTITLE_SIZE, files: 1 },
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

// ─── Convert SRT → WebVTT ──────────────────────────────────────────────────

function convertToVtt(buffer, originalFilename) {
	const ext = path.extname(originalFilename).toLowerCase()
	if (ext === '.vtt') {
		// Already WebVTT — pass through unchanged
		return Promise.resolve(buffer)
	}
	// Convert SRT to WebVTT via streaming pipeline
	return new Promise((resolve, reject) => {
		const chunks = []
		const readable = Readable.from(buffer)
		const converter = srtToVtt()
		converter.on('data', chunk => chunks.push(chunk))
		converter.on('end', () => {
			let vtt = Buffer.concat(chunks).toString('utf-8')
			// Fix header: srt-to-vtt emits "WEBVTT FILE" — use standard header
			vtt = vtt.replace(/^WEBVTT\s+FILE[ \t]*\r?\n\r?\n/, 'WEBVTT\r\n\r\n')
			// Strip SRT cue sequence numbers (bare integers between blank line and timestamp)
			vtt = vtt.replace(/(\r?\n\r?\n)\d+(\r?\n\d{2}:\d{2}:\d{2}\.\d{3}\s*-->)/g, '$1$2')
			resolve(Buffer.from(vtt, 'utf-8'))
		})
		converter.on('error', reject)
		readable.pipe(converter)
	})
}

// ─── Upload handler ──────────────────────────────────────────────────────────

async function handleSubtitleUpload(req, res) {
	const ip = req.socket.remoteAddress?.replace('::ffff:', '') || 'unknown'

	// Parse video_id from URL: /upload_subtitle/{video_id}
	const urlPath = req.url.split('?')[0]
	const parts = urlPath.split('/').filter(Boolean)

	if (parts.length !== 2 || parts[0] !== 'upload_subtitle') {
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

	// Validate extension
	const originalExt = path.extname(uploadedFile.filename).toLowerCase()
	if (!ALLOWED_EXTENSIONS.has(originalExt)) {
		return json(res, 400, { error: 'Invalid file type. Only .vtt and .srt files are allowed' })
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

	// ── Convert SRT to WebVTT if needed ──────────────────────
	let vttBuffer
	try {
		vttBuffer = await convertToVtt(uploadedFile.buffer, uploadedFile.filename)
	} catch (err) {
		log(`ERROR converting subtitle: ${err.message}`)
		return json(res, 400, { error: `Subtitle conversion failed: ${err.message}` })
	}

	// ── Save to disk ────────────────────────────────────────
	const subtitleDir = path.join(MISC_DIR, userId, 'subtitles')
	const subtitleId = crypto.randomUUID()
	const subtitleFilename = `${subtitleId}.vtt`
	const subtitlePath = path.join(subtitleDir, subtitleFilename)

	try {
		fs.mkdirSync(subtitleDir, { recursive: true })
		fs.writeFileSync(subtitlePath, vttBuffer)
	} catch (err) {
		log(`ERROR write subtitle ${subtitlePath}: ${err.message}`)
		return json(res, 500, { error: 'Failed to save file' })
	}

	log(`SUBTITLE ${videoId}: uploaded by ${userId} (${formatBytes(vttBuffer.length)}, ${originalExt}) from ${ip}`)

	return json(res, 200, {
		filename: subtitleFilename,
	})
}

// ─── Serve handler ───────────────────────────────────────────────────────────

function handleSubtitleServe(req, res) {
	const urlPath = req.url.split('?')[0]
	const parts = urlPath.split('/').filter(Boolean)

	// GET /subtitles/{userId}/{file}
	if (parts.length < 3 || parts[0] !== 'subtitles') {
		return json(res, 400, { error: 'Invalid path' })
	}

	const userId = parts[1]
	const file = parts[2]

	// Path traversal protection
	if (userId.includes('..') || userId.includes('/') || userId.includes('\\')) {
		return json(res, 400, { error: 'Invalid userId' })
	}
	if (file.includes('..') || file.includes('/') || file.includes('\\')) {
		return json(res, 400, { error: 'Invalid filename' })
	}

	// Only serve .vtt files
	if (!file.toLowerCase().endsWith('.vtt')) {
		return json(res, 400, { error: 'Only .vtt files are served' })
	}

	const filePath = path.join(MISC_DIR, userId, 'subtitles', file)

	try {
		fs.accessSync(filePath, fs.constants.R_OK)
	} catch {
		return json(res, 404, { error: 'Subtitle file not found' })
	}

	const stat = fs.statSync(filePath)
	res.writeHead(200, {
		'Content-Type': 'text/vtt',
		'Content-Length': stat.size,
		'Access-Control-Allow-Origin': '*',
		'Cache-Control': 'public, max-age=3600',
	})

	const stream = fs.createReadStream(filePath)
	stream.pipe(res)
	stream.on('error', () => {
		res.end()
	})
}

// ─── Server ──────────────────────────────────────────────────────────────────

const server = http.createServer((req, res) => {
	// CORS preflight
	if (req.method === 'OPTIONS') {
		res.writeHead(204, CORS_HEADERS)
		return res.end()
	}

	// POST /upload_subtitle/{video_id}
	if (req.method === 'POST' && req.url.startsWith('/upload_subtitle/')) {
		return handleSubtitleUpload(req, res).catch(err => {
			log(`UNHANDLED ${err.message}`)
			json(res, 500, { error: 'Internal server error' })
		})
	}

	// GET /subtitles/{userId}/{file}
	if (req.method === 'GET' && req.url.startsWith('/subtitles/')) {
		return handleSubtitleServe(req, res)
	}

	json(res, 405, { error: 'Method not allowed' })
})

server.listen(PORT, () => {
	log(`subtitle-server listening on :${PORT}`)
	log(`misc dir: ${MISC_DIR}`)
})
