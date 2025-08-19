const express = require("express");
const path = require("path");
const fs = require("fs");
const fsp = fs.promises;
const crypto = require("crypto");
const { md5Base64, crc32cBase64 } = require("./crypto");

const PORT = process.env.PORT || 8000;
const ROOT = path.resolve(process.env.GCS_DIR || path.join(process.cwd(), "gcs-data"));

const app = express();
app.use(express.json({ limit: "200mb" }));

// Simple access log (helpful for debugging)
app.use((req, res, next) => {
	res.on("finish", () => {
		console.log(req.method, decodeURI(req.url), res.statusCode, res.statusMessage);
	});
	next();
});

// ---------------------------- utils ------------------------------------
const ok = (res, data) => res.status(200).json(data);
const conflict = (res, msg) => res.status(409).json({ error: { code: 409, message: msg } });
const notFound = (res, msg) => res.status(404).json({ error: { code: 404, message: msg } });
const badReq = (res, msg) => res.status(400).json({ error: { code: 400, message: msg } });
const ensureDir = async (dir) => fsp.mkdir(dir, { recursive: true });

function safeJoin(base, target) {
	const full = path.join(base, target);
	const rel = path.relative(base, full);
	if (rel.startsWith("..") || path.isAbsolute(rel)) throw new Error("Path traversal detected");
	return full;
}

async function objectMeta(bucket, objectRel, contentTypeOverride) {
	const bucketDir = path.join(ROOT, bucket);
	const filePath = safeJoin(bucketDir, objectRel);
	const stat = await fsp.stat(filePath);
	const sizeNum = stat.size;
	const buf = await fsp.readFile(filePath);
	const md5b64 = md5Base64(buf);
	const crcB64 = crc32cBase64(buf);
	const ct = contentTypeOverride || (objectRel.endsWith(".csv") ? "text/csv" : "application/octet-stream");
	const encoded = encodeURIComponent(objectRel).replace(/%2F/g, "%2F");
	const base = `http://127.0.0.1:${PORT}`;
	const json = {
		kind: "storage#object",
		id: `${bucket}/${objectRel}`,
		bucket,
		name: objectRel,
		size: String(sizeNum),
		contentType: ct,
		storageClass: "STANDARD",
		md5Hash: md5b64,
		crc32c: crcB64,
		etag: md5b64,
		selfLink: `${base}/storage/v1/b/${bucket}/o/${encoded}`,
		mediaLink: `${base}/download/storage/v1/b/${bucket}/o/${encoded}?alt=media`,
		timeCreated: new Date(stat.mtimeMs).toISOString(),
		updated: new Date(stat.mtimeMs).toISOString(),
		metageneration: "1",
		generation: String(stat.mtimeMs | 0),
	};
	const headers = {
		"ETag": `"${md5b64}"`,
		"x-goog-hash": `crc32c=${crcB64},md5=${md5b64}`,
		"X-Goog-Generation": String(stat.mtimeMs | 0),
		"X-Goog-Stored-Content-Encoding": "identity",
		"Last-Modified": new Date(stat.mtimeMs).toUTCString(),
		"Content-Type": ct,
		"Content-Length": String(sizeNum),
	};
	return { json, headers };
}

// ------------------------ multipart parsing -----------------------------
// Minimal parser for multipart/related with two parts (metadata JSON + media)
function parseMultipartRelated(rawBuf, contentTypeHeader) {
	// e.g. Content-Type: multipart/related; boundary=abc123; type=application/json
	const m = /boundary=(?:(?:"([^"]+)")|([^;]+))/i.exec(contentTypeHeader || "");
	if (!m) throw new Error("multipart boundary not found");
	const boundary = m[1] || m[2];
	const dashBoundary = Buffer.from(`--${boundary}`);
	const CRLF = Buffer.from("\r\n");

	// Split into parts by boundary markers
	const parts = [];
	let start = 0;
	while (true) {
		const idx = rawBuf.indexOf(dashBoundary, start);
		if (idx === -1) break;
		const next = rawBuf.indexOf(dashBoundary, idx + dashBoundary.length);
		if (next === -1) break;
		const part = rawBuf.subarray(idx + dashBoundary.length + CRLF.length, next - CRLF.length);
		parts.push(part);
		start = next;
	}
	if (parts.length === 0) throw new Error("no multipart parts found");

	function splitHeadersBody(partBuf) {
		const sep = Buffer.from("\r\n\r\n");
		const sepIdx = partBuf.indexOf(sep);
		if (sepIdx === -1) return { headers: {}, body: partBuf };
		const headersRaw = partBuf.subarray(0, sepIdx).toString("utf8");
		const body = partBuf.subarray(sepIdx + sep.length);
		const headers = {};
		headersRaw.split(/\r?\n/).forEach((line) => {
			const k = line.split(":")[0]?.trim();
			const v = line.slice(line.indexOf(":") + 1).trim();
			if (k) headers[k.toLowerCase()] = v;
		});
		return { headers, body };
	}

	// Prefer non-JSON part as media; if only JSON exists, no media
	let media = null;
	let meta = null;
	for (const p of parts) {
		const { headers, body } = splitHeadersBody(p);
		const ctype = headers["content-type"] || "";
		if (/application\/json/i.test(ctype)) meta = { headers, body };
		else media = { headers, body };
	}
	if (!media && meta) {
		// some clients may send JSON-only (unlikely); treat body as content
		media = meta;
	}
	if (!media) throw new Error("multipart missing media part");

	// Try to read contentType from JSON metadata if present
	let contentType;
	if (meta) {
		try {
			const j = JSON.parse(meta.body.toString("utf8"));
			if (typeof j.contentType === "string") contentType = j.contentType;
		} catch { }
	}

	const mediaType = (media.headers["content-type"]) || contentType || "application/octet-stream";
	return { body: media.body, contentType: mediaType };
}

// ---------------------------- routes -----------------------------------
// Create bucket
app.post(/^\/storage\/v1\/b$/, async (req, res) => {
	const { name } = req.body || {};
	if (!name) return badReq(res, "Missing bucket name");
	const bucketDir = path.join(ROOT, name);
	await ensureDir(ROOT);
	if (fs.existsSync(bucketDir)) return conflict(res, `Bucket ${name} already exists`);
	await ensureDir(bucketDir);
	return ok(res, { name, kind: "storage#bucket" });
});

// Upload (simple: media or multipart)
app.post(/^\/upload\/storage\/v1\/b\/([^/]+)\/o$/, express.raw({ type: "*/*", limit: "500mb" }), async (req, res) => {
	const bucket = req.params[0];
	const objectNameRaw = req.query.name;
	if (!objectNameRaw) return badReq(res, "Missing object name");
	const objectName = decodeURIComponent(String(objectNameRaw));

	const bucketDir = path.join(ROOT, bucket);
	// auto-create bucket if missing (dev convenience)
	if (!fs.existsSync(bucketDir)) await ensureDir(bucketDir);

	const ctype = req.headers["content-type"] || "";
	let bytes = req.body || Buffer.alloc(0);
	let uploadedContentType = undefined;

	if (/^multipart\/related/i.test(ctype)) {
		({ body: bytes, contentType: uploadedContentType } = parseMultipartRelated(bytes, ctype));
	}

	const filePath = safeJoin(bucketDir, objectName);
	await ensureDir(path.dirname(filePath));
	await fsp.writeFile(filePath, bytes);

	const { json, headers } = await objectMeta(bucket, objectName, uploadedContentType || req.header("Content-Type"));
	for (const [k, v] of Object.entries(headers)) res.setHeader(k, v);
	return ok(res, json);
});

// Metadata GET (and alt=media on /storage like real GCS)
app.get(/^\/storage\/v1\/b\/([^/]+)\/o\/(.+)$/, async (req, res) => {
	const bucket = req.params[0];
	const objectRel = decodeURIComponent(req.params[1]);
	const bucketDir = path.join(ROOT, bucket);
	const filePath = safeJoin(bucketDir, objectRel);
	if (!fs.existsSync(filePath)) return notFound(res, "Object not found");
	if ((req.query.alt || "") === "media") {
		const { headers } = await objectMeta(bucket, objectRel);
		for (const [k, v] of Object.entries(headers)) res.setHeader(k, v);
		return fs.createReadStream(filePath).pipe(res);
	}
	const { json, headers } = await objectMeta(bucket, objectRel);
	for (const [k, v] of Object.entries(headers)) res.setHeader(k, v);
	return ok(res, json);
});

// Media GET (download endpoint)
app.get(/^\/download\/storage\/v1\/b\/([^/]+)\/o\/(.+)$/, async (req, res) => {
	const bucket = req.params[0];
	const objectRel = decodeURIComponent(req.params[1]);
	const bucketDir = path.join(ROOT, bucket);
	const filePath = safeJoin(bucketDir, objectRel);
	if (!fs.existsSync(filePath)) return notFound(res, "Object not found");
	const { headers } = await objectMeta(bucket, objectRel);
	for (const [k, v] of Object.entries(headers)) res.setHeader(k, v);
	fs.createReadStream(filePath).pipe(res);
});

// Delete
app.delete(/^\/storage\/v1\/b\/([^/]+)\/o\/(.+)$/, async (req, res) => {
	const bucket = req.params[0];
	const objectRel = decodeURIComponent(req.params[1]);
	const bucketDir = path.join(ROOT, bucket);
	const filePath = safeJoin(bucketDir, objectRel);
	if (!fs.existsSync(filePath)) return notFound(res, "Object not found");
	await fsp.unlink(filePath);
	return res.status(204).end();
});

app.use((err, _req, res, _next) => {
	console.error(err)
	return res.status(500).json({ error: { code: 500, message: err.message } });
})

// ---------------------------- boot -------------------------------------
app.listen(PORT, async () => {
	await ensureDir(ROOT);
	console.log(`Mini GCS emulator running on http://127.0.0.1:${PORT}`);
	console.log(`Data dir: ${ROOT}`);
});

