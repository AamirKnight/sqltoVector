// process_sql_to_vector.js
// Handles SQL format where each row is on its own line:
//   INSERT INTO `table` (`col1`, `col2`, ...) VALUES
//   (val1, val2, ...),
//   (val1, val2, ...);

const fs = require("fs");
const readline = require("readline");
const axios = require("axios");
const crypto = require("crypto");
const { ChromaClient } = require("chromadb");
const pLimit = require("p-limit").default;

// ─── CONFIG ───────────────────────────────────────────────────────────────────
const FILE_PATH = process.env.SQL_FILE || "./airtelnoc.sql";
const EMBED_URL  = process.env.EMBED_URL  || "http://localhost:8000/embed";
const COLLECTION_NAME = process.env.COLLECTION || "sql_data";
const CHECKPOINT_FILE = "./checkpoint.json";

const BATCH_SIZE      = 50;
const CONCURRENCY     = 3;
const EMBED_TIMEOUT   = 30000;
// ─────────────────────────────────────────────────────────────────────────────

// Column names we care about (text/meaningful fields from monthly_template)
const SKIP_COLS = new Set([
  "id", "year", "is_deleted", "project_id", "circle_id",
  "created_by", "updated_by",
]);

const client = new ChromaClient({ host: "localhost", port: 8001 });
const limit  = pLimit(CONCURRENCY);
let collection;
let columnNames = []; // extracted from INSERT header
let inValues    = false;

// ── Checkpoint ────────────────────────────────────────────────────────────────
function loadCheckpoint() {
  try { return JSON.parse(fs.readFileSync(CHECKPOINT_FILE, "utf8")); }
  catch { return { processedLines: 0, storedChunks: 0 }; }
}
function saveCheckpoint(data) {
  fs.writeFileSync(CHECKPOINT_FILE, JSON.stringify(data, null, 2));
}

// ── Extract column names from INSERT header ───────────────────────────────────
// e.g. INSERT INTO `monthly_template` (`id`, `dates`, ...) VALUES
function parseColumns(line) {
  const match = line.match(/INSERT INTO `\w+` \((.+?)\)\s+VALUES/i);
  if (!match) return [];
  return match[1].split(",").map(c => c.trim().replace(/`/g, ""));
}

// ── Parse a value row line ────────────────────────────────────────────────────
// e.g. (1,\t'2024-07-01',\t2024,\t'Q2', ...)
function parseValueRow(line) {
  const trimmed = line.trim().replace(/[,;)]+$/, "").replace(/^\(/, "");
  
  // Split by tab+comma or just comma, respecting single-quoted strings
  const values = [];
  let current = "";
  let inQuote = false;
  
  for (let i = 0; i < trimmed.length; i++) {
    const ch = trimmed[i];
    if (ch === "'" && trimmed[i-1] !== "\\") {
      inQuote = !inQuote;
      current += ch;
    } else if (ch === "," && !inQuote) {
      values.push(current.trim());
      current = "";
    } else {
      current += ch;
    }
  }
  if (current.trim()) values.push(current.trim());
  
  return values.map(v => v.replace(/^'(.*)'$/, "$1").trim()); // strip quotes
}

// ── Build a readable text from a row ─────────────────────────────────────────
function rowToText(values) {
  if (columnNames.length === 0) {
    // No column names yet — just join non-numeric values
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

async function embedAndStore(texts) {
  const res = await axios.post(EMBED_URL, { texts }, { timeout: EMBED_TIMEOUT });
  const embeddings = res.data.embeddings;
  const ids = texts.map(hashId);
  await collection.upsert({ ids, documents: texts, embeddings });
  return texts.length;
}

// ── Progress ──────────────────────────────────────────────────────────────────
function makeProgress(filePath) {
  const totalBytes = fs.statSync(filePath).size;
  let bytesRead = 0;
  let chunksStored = 0;
  const startTime = Date.now();
  return {
    update(lineBytes, newChunks = 0) {
      bytesRead += lineBytes;
      chunksStored += newChunks;
      const pct     = ((bytesRead / totalBytes) * 100).toFixed(1);
      const elapsed = (Date.now() - startTime) / 1000;
      const rate    = (bytesRead / elapsed / 1e6).toFixed(2);
      const eta     = elapsed > 1
        ? ((totalBytes - bytesRead) / (bytesRead / elapsed)).toFixed(0)
        : "?";
      process.stdout.write(
        `\r[${pct}%] ${(bytesRead/1e6).toFixed(1)}/${(totalBytes/1e6).toFixed(1)} MB` +
        ` | ${chunksStored} rows stored | ${rate} MB/s | ETA ${eta}s   `
      );
    },
  };
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  console.log("Connecting to ChromaDB...");
  collection = await client.getOrCreateCollection({
    name: COLLECTION_NAME,
    embeddingFunction: { generate: async (texts) => {
      const res = await axios.post(EMBED_URL, { texts }, { timeout: EMBED_TIMEOUT });
      return res.data.embeddings;
    }},
  });
  console.log(`Collection '${COLLECTION_NAME}' ready.\n`);

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

    // Detect INSERT header → extract column names
    if (trimmed.toUpperCase().includes("INSERT INTO") && trimmed.toUpperCase().includes("VALUES")) {
      columnNames = parseColumns(trimmed);
      inValues = true;
      continue;
    }

    // Detect start of VALUES block (VALUES on its own line)
    if (trimmed.toUpperCase() === "VALUES" || trimmed.toUpperCase().startsWith("VALUES")) {
      inValues = true;
      continue;
    }

    // End of INSERT block
    if (inValues && (trimmed === "" || trimmed.toUpperCase().startsWith("INSERT") || trimmed.startsWith("--") || trimmed.startsWith("/"))) {
      inValues = false;
    }

    // Parse value rows: lines starting with (
    if (inValues && trimmed.startsWith("(")) {
      const values  = parseValueRow(trimmed);
      const text    = rowToText(values);
      if (text.length > 20) {
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

  console.log(`\n\nDONE`);
  console.log(`  Rows parsed : ${parsedRows}`);
  console.log(`  Chunks stored: ${totalStored}`);
  console.log(`  Collection  : ${COLLECTION_NAME}`);

  if (totalStored > 0) {
    try { fs.unlinkSync(CHECKPOINT_FILE); } catch {}
  }
}

main().catch(err => {
  console.error("\nFatal:", err.message);
  process.exit(1);
});