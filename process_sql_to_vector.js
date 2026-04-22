// process_sql_to_vector.js
// Saves vectors to a local SQLite file using better-sqlite3
// No ChromaDB server needed — pure local file

const fs = require("fs");
const readline = require("readline");
const axios = require("axios");
const crypto = require("crypto");
const path = require("path");
const pLimit = require("p-limit").default;
const Database = require("better-sqlite3");

// ─── CONFIG ───────────────────────────────────────────────────────────────────
const FILE_PATH       = process.env.SQL_FILE    || "./airtelnoc.sql";
const EMBED_URL       = process.env.EMBED_URL   || "http://localhost:8000/embed";
const DB_PATH         = process.env.DB_PATH     || "./vectors.db";
const CHECKPOINT_FILE = "./checkpoint.json";

const BATCH_SIZE    = 50;
const CONCURRENCY   = 3;
const EMBED_TIMEOUT = 30000;
// ─────────────────────────────────────────────────────────────────────────────

const limit = pLimit(CONCURRENCY);

const SKIP_COLS = new Set([
  "id", "year", "is_deleted", "project_id", "circle_id",
  "created_by", "updated_by",
]);

let columnNames = [];
let inValues    = false;
let db;
let insertStmt;

// ── DB setup ──────────────────────────────────────────────────────────────────
function initDB() {
  db = new Database(DB_PATH);
  db.exec(`
    CREATE TABLE IF NOT EXISTS vectors (
      id TEXT PRIMARY KEY,
      document TEXT NOT NULL,
      embedding BLOB NOT NULL
    )
  `);
  insertStmt = db.prepare(`
    INSERT OR REPLACE INTO vectors (id, document, embedding)
    VALUES (@id, @document, @embedding)
  `);
  console.log(`SQLite vector DB ready: ${DB_PATH}`);
}

// ── Checkpoint ────────────────────────────────────────────────────────────────
function loadCheckpoint() {
  try { return JSON.parse(fs.readFileSync(CHECKPOINT_FILE, "utf8")); }
  catch { return { processedLines: 0, storedChunks: 0 }; }
}
function saveCheckpoint(data) {
  fs.writeFileSync(CHECKPOINT_FILE, JSON.stringify(data, null, 2));
}

// ── SQL parsing ───────────────────────────────────────────────────────────────
function parseColumns(line) {
  const match = line.match(/INSERT INTO `\w+`\s*\((.+?)\)\s*VALUES/i);
  if (!match) return [];
  return match[1].split(",").map(c => c.trim().replace(/`/g, ""));
}

function parseValueRow(line) {
  const trimmed = line.trim().replace(/[,;]+$/, "").replace(/^\(/, "").replace(/\)$/, "");
  const values = [];
  let current = "";
  let inQuote = false;

  for (let i = 0; i < trimmed.length; i++) {
    const ch = trimmed[i];
    if (ch === "'" && trimmed[i - 1] !== "\\") {
      inQuote = !inQuote;
      current += ch;
    } else if ((ch === "," || ch === "\t") && !inQuote) {
      if (ch === ",") { values.push(current.trim()); current = ""; }
    } else {
      current += ch;
    }
  }
  if (current.trim()) values.push(current.trim());
  return values.map(v => v.replace(/^'(.*)'$/s, "$1").trim());
}

function rowToText(values) {
  if (columnNames.length === 0) {
    return values
      .filter(v => v && v !== "NULL" && !/^\d{1,4}$/.test(v) && v !== "0000-00-00 00:00:00")
      .join(" | ");
  }
  const parts = [];
  for (let i = 0; i < columnNames.length && i < values.length; i++) {
    const col = columnNames[i];
    const val = values[i];
    if (SKIP_COLS.has(col)) continue;
    if (!val || val === "NULL" || val === "0" || val === "0000-00-00 00:00:00") continue;
    parts.push(`${col}: ${val}`);
  }
  return parts.join(" | ");
}

// ── Embed + store ─────────────────────────────────────────────────────────────
function hashId(text) {
  return crypto.createHash("md5").update(text).digest("hex");
}

function float32ToBuffer(arr) {
  const buf = Buffer.alloc(arr.length * 4);
  arr.forEach((v, i) => buf.writeFloatLE(v, i * 4));
  return buf;
}

async function embedAndStore(texts) {
  const res = await axios.post(EMBED_URL, { texts }, { timeout: EMBED_TIMEOUT });
  const embeddings = res.data.embeddings;

  const insertMany = db.transaction((rows) => {
    for (const row of rows) insertStmt.run(row);
  });

  const rows = texts.map((doc, i) => ({
    id: hashId(doc),
    document: doc,
    embedding: float32ToBuffer(embeddings[i]),
  }));

  insertMany(rows);
  return texts.length;
}

// ── Progress ──────────────────────────────────────────────────────────────────
function makeProgress(filePath) {
  const totalBytes = fs.statSync(filePath).size;
  let bytesRead = 0;
  let rowsStored = 0;
  const startTime = Date.now();
  return {
    update(lineBytes, newRows = 0) {
      bytesRead += lineBytes;
      rowsStored += newRows;
      const pct     = ((bytesRead / totalBytes) * 100).toFixed(1);
      const elapsed = (Date.now() - startTime) / 1000;
      const rate    = (bytesRead / elapsed / 1e6).toFixed(2);
      const eta     = elapsed > 1
        ? ((totalBytes - bytesRead) / (bytesRead / elapsed)).toFixed(0)
        : "?";
      process.stdout.write(
        `\r[${pct}%] ${(bytesRead/1e6).toFixed(1)}/${(totalBytes/1e6).toFixed(1)} MB` +
        ` | ${rowsStored} rows | ${rate} MB/s | ETA ${eta}s   `
      );
    },
  };
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  initDB();

  const checkpoint = loadCheckpoint();
  const skipLines  = checkpoint.processedLines;
  let totalStored  = checkpoint.storedChunks;
  if (skipLines > 0) {
    console.log(`Resuming from line ${skipLines} (${totalStored} rows already stored)`);
  }

  const stream   = fs.createReadStream(FILE_PATH);
  const rl       = readline.createInterface({ input: stream, crlfDelay: Infinity });
  const progress = makeProgress(FILE_PATH);

  let buffer     = [];
  let lineNum    = 0;
  let parsedRows = 0;
  const pending  = [];

  async function flush(buf) {
    try {
      const stored = await embedAndStore(buf);
      totalStored += stored;
      progress.update(0, stored);
    } catch (err) {
      console.error(`\nBatch failed: ${err.message}`);
    }
  }

  for await (const line of rl) {
    lineNum++;
    progress.update(Buffer.byteLength(line, "utf8") + 1);
    if (lineNum <= skipLines) continue;

    const trimmed = line.trim();

    // INSERT header with VALUES on same line
    if (/INSERT INTO/i.test(trimmed) && /VALUES/i.test(trimmed)) {
      columnNames = parseColumns(trimmed);
      inValues = true;
      // Check if values start on same line after VALUES
      const afterValues = trimmed.replace(/.*VALUES\s*/i, "").trim();
      if (afterValues.startsWith("(")) {
        for (const rowText of afterValues.split(/\),\s*\(/).map((r, i, a) => {
          return (i === 0 ? "" : "(") + r + (i === a.length - 1 ? "" : ")");
        })) {
          // handled below via inValues
        }
      }
      continue;
    }

    // VALUES keyword alone on a line
    if (/^VALUES\s*$/i.test(trimmed)) {
      inValues = true;
      continue;
    }

    // End of block
    if (inValues && (trimmed === "" || /^INSERT/i.test(trimmed) || /^--/.test(trimmed) || /^\/\*/.test(trimmed))) {
      inValues = false;
    }

    // Value row
    if (inValues && trimmed.startsWith("(")) {
      const values = parseValueRow(trimmed);
      const text   = rowToText(values);
      if (text.length > 15) {
        buffer.push(text);
        parsedRows++;
        if (buffer.length >= BATCH_SIZE) {
          const batch = buffer.splice(0, BATCH_SIZE);
          pending.push(limit(() => flush(batch)));
        }
      }
    }

    if (lineNum % 5000 === 0) {
      await Promise.all(pending.splice(0));
      saveCheckpoint({ processedLines: lineNum, storedChunks: totalStored });
    }
  }

  if (buffer.length > 0) pending.push(limit(() => flush(buffer)));
  await Promise.all(pending);
  saveCheckpoint({ processedLines: lineNum, storedChunks: totalStored });

  const count = db.prepare("SELECT COUNT(*) as c FROM vectors").get();
  db.close();

  console.log(`\n\nDONE`);
  console.log(`  SQL rows parsed : ${parsedRows}`);
  console.log(`  Vectors stored  : ${count.c}`);
  console.log(`  DB file         : ${path.resolve(DB_PATH)}`);

  if (totalStored > 0) {
    try { fs.unlinkSync(CHECKPOINT_FILE); } catch {}
  }
}

main().catch(err => {
  console.error("\nFatal:", err.message);
  process.exit(1);
});