const fs = require("fs");
const readline = require("readline");
const axios = require("axios");
const { ChromaClient } = require("chromadb");

const FILE_PATH = "./dump.sql";
const EMBED_URL = "http://localhost:8000/embed";

const BATCH_SIZE = 50;

// Init Chroma (persistent)
const client = new ChromaClient({
  path: "./chroma_db"
});

let collection;

async function initDB() {
  collection = await client.getOrCreateCollection({
    name: "sql_data"
  });
}

// Extract values
function extractValues(line) {
  const match = line.match(/\((.*)\)/);
  if (!match) return null;

  return match[1]
    .replace(/['"]/g, "")
    .split(",")
    .join(" ");
}

// Chunk
function chunkText(text, size = 200) {
  const words = text.split(" ");
  let chunks = [];

  for (let i = 0; i < words.length; i += size) {
    chunks.push(words.slice(i, i + size).join(" "));
  }

  return chunks;
}

// Embed
async function embedBatch(texts) {
  const res = await axios.post(EMBED_URL, { texts });
  return res.data.embeddings;
}

// Process batch
async function processBatch(texts) {
  const embeddings = await embedBatch(texts);

  const ids = texts.map((_, i) => Date.now() + "_" + i);

  await collection.add({
    ids,
    documents: texts,
    embeddings
  });

  console.log("Stored:", texts.length);
}

// Main
async function processFile() {
  await initDB();

  const stream = fs.createReadStream(FILE_PATH);
  const rl = readline.createInterface({ input: stream });

  let buffer = [];

  for await (const line of rl) {
    if (!line.startsWith("INSERT")) continue;

    const text = extractValues(line);
    if (!text) continue;

    const chunks = chunkText(text);

    for (const chunk of chunks) {
      buffer.push(chunk);

      if (buffer.length >= BATCH_SIZE) {
        await processBatch(buffer);
        buffer = [];
      }
    }
  }

  if (buffer.length) {
    await processBatch(buffer);
  }

  console.log("✅ DONE — Vector DB created");
}

processFile();