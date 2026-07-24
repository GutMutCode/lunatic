#!/usr/bin/env node

// Deterministic physical SLOC accounting for embedded-v2 candidates.
// Usage: node count-sloc.mjs path/to/sloc-manifest.json
//        node count-sloc.mjs --self-test

import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import process from "node:process";

const SCHEMA_VERSION = 1;
const SUPPORTED_SUFFIXES = new Set([".rs", ".toml", ".wat", ".wit", ".json", ".yaml", ".yml"]);
const IGNORED_DIRECTORIES = new Set([".git", "target"]);
const COUNTED_ROLES = new Set(["production"]);
const VALID_ROLES = new Set(["production", "test", "generated", "lockfile"]);

function fail(message) {
  throw new Error(message);
}

function sha256(filePath) {
  return crypto.createHash("sha256").update(fs.readFileSync(filePath)).digest("hex");
}

function styleFor(filePath) {
  const suffix = path.extname(filePath);
  if (suffix === ".rs") return "rust";
  if (suffix === ".wat" || suffix === ".wit") return "wasm-text";
  if (suffix === ".toml" || suffix === ".yaml" || suffix === ".yml") return "hash";
  if (suffix === ".json") return "json";
  fail(`unsupported source type: ${filePath}`);
}

function stripComments(line, style, state) {
  if (style === "json") return line;

  let lineComment;
  let blockOpen = null;
  let blockClose = null;
  if (style === "rust") {
    lineComment = "//";
    blockOpen = "/*";
    blockClose = "*/";
  } else if (style === "wasm-text") {
    lineComment = ";;";
    blockOpen = "(;";
    blockClose = ";)";
  } else if (style === "hash") {
    lineComment = "#";
  } else {
    fail(`unknown comment style: ${style}`);
  }

  let output = "";
  let index = 0;
  let quote = null;
  let escaped = false;
  while (index < line.length) {
    if (state.blockDepth > 0) {
      if (blockOpen && line.startsWith(blockOpen, index)) {
        state.blockDepth += 1;
        index += blockOpen.length;
      } else if (blockClose && line.startsWith(blockClose, index)) {
        state.blockDepth -= 1;
        index += blockClose.length;
      } else {
        index += 1;
      }
      continue;
    }

    const character = line[index];
    if (quote !== null) {
      output += character;
      if (escaped) escaped = false;
      else if (character === "\\") escaped = true;
      else if (character === quote) quote = null;
      index += 1;
      continue;
    }

    if (character === '"' || (style === "hash" && character === "'")) {
      quote = character;
      output += character;
      index += 1;
      continue;
    }
    if (line.startsWith(lineComment, index)) break;
    if (blockOpen && line.startsWith(blockOpen, index)) {
      state.blockDepth += 1;
      index += blockOpen.length;
      continue;
    }
    output += character;
    index += 1;
  }
  return output;
}

function countFile(filePath) {
  const style = styleFor(filePath);
  const state = { blockDepth: 0 };
  let count = 0;
  for (const line of fs.readFileSync(filePath, "utf8").split(/\r?\n/)) {
    if (stripComments(line, style, state).trim()) count += 1;
  }
  if (state.blockDepth !== 0) fail(`unterminated block comment: ${filePath}`);
  return count;
}

function walk(root, current = root, found = new Set()) {
  for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
    if (IGNORED_DIRECTORIES.has(entry.name)) continue;
    const fullPath = path.join(current, entry.name);
    if (entry.isDirectory()) {
      walk(root, fullPath, found);
    } else if (entry.isFile()) {
      const suffix = path.extname(entry.name);
      if (SUPPORTED_SUFFIXES.has(suffix) || entry.name === "Cargo.lock") {
        found.add(path.relative(root, fullPath).split(path.sep).join("/"));
      }
    }
  }
  return found;
}

function analyze(manifestPathValue) {
  const manifestPath = fs.realpathSync(manifestPathValue);
  const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
  if (manifest.schema_version !== SCHEMA_VERSION) fail(`schema_version must be ${SCHEMA_VERSION}`);
  if (typeof manifest.candidate !== "string" || !manifest.candidate) fail("candidate must be a non-empty string");
  if (typeof manifest.root !== "string" || !manifest.root) fail("root must be a non-empty string");
  if (!Array.isArray(manifest.entries)) fail("entries must be an array");

  const root = fs.realpathSync(path.resolve(path.dirname(manifestPath), manifest.root));
  if (!fs.statSync(root).isDirectory()) fail(`candidate root is not a directory: ${root}`);
  const attributed = new Set();
  const rows = [];
  const categoryTotals = {};
  const roleTotals = Object.fromEntries([...VALID_ROLES].sort().map((role) => [role, 0]));

  for (const entry of manifest.entries) {
    if (typeof entry !== "object" || entry === null || Array.isArray(entry)) fail("each entry must be an object");
    if (typeof entry.path !== "string" || !entry.path) fail("entry.path must be a non-empty string");
    const relative = entry.path.replaceAll("\\", "/");
    if (attributed.has(relative)) fail(`duplicate entry: ${relative}`);
    attributed.add(relative);
    if (!VALID_ROLES.has(entry.role)) fail(`invalid role for ${relative}: ${entry.role}`);
    if (typeof entry.category !== "string" || !entry.category) fail(`entry.category must be non-empty for ${relative}`);
    if ((entry.role === "generated" || entry.role === "lockfile") && !entry.reason) {
      fail(`${entry.role} entry requires a reason: ${relative}`);
    }

    const source = path.resolve(root, relative);
    const relativeCheck = path.relative(root, source);
    if (relativeCheck.startsWith("..") || path.isAbsolute(relativeCheck)) fail(`entry escapes candidate root: ${relative}`);
    if (!fs.existsSync(source) || !fs.statSync(source).isFile()) fail(`attributed file does not exist: ${relative}`);

    let sloc = 0;
    if (entry.role !== "generated" && entry.role !== "lockfile") sloc = countFile(source);
    roleTotals[entry.role] += sloc;
    if (COUNTED_ROLES.has(entry.role)) {
      categoryTotals[entry.category] = (categoryTotals[entry.category] ?? 0) + sloc;
    }
    rows.push({ path: relative, role: entry.role, category: entry.category, sloc, sha256: sha256(source) });
  }

  const discovered = walk(root);
  const missing = [...discovered].filter((item) => !attributed.has(item)).sort();
  const extra = [...attributed].filter((item) => !discovered.has(item)).sort();
  if (missing.length) fail(`unattributed supported files: ${JSON.stringify(missing)}`);
  if (extra.length) fail(`entries are not supported source/config files: ${JSON.stringify(extra)}`);

  rows.sort((left, right) => left.path.localeCompare(right.path));
  return {
    schema_version: SCHEMA_VERSION,
    candidate: manifest.candidate,
    manifest: manifestPath.split(path.sep).join("/"),
    manifest_sha256: sha256(manifestPath),
    counted_roles: [...COUNTED_ROLES].sort(),
    production_sloc: roleTotals.production,
    role_totals: roleTotals,
    category_totals: Object.fromEntries(Object.entries(categoryTotals).sort()),
    files: rows,
  };
}

function selfTest() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "embedded-v2-sloc-"));
  try {
    fs.mkdirSync(path.join(root, "src"));
    fs.writeFileSync(
      path.join(root, "src", "main.rs"),
      '// comment\nfn main() { /* inline */ }\n/* block\ncomment */\nlet x = "//";\n',
    );
    fs.writeFileSync(path.join(root, "Cargo.toml"), '# comment\n[package]\nname = "fixture#name"\n');
    const manifest = {
      schema_version: 1,
      candidate: "self-test",
      root: ".",
      entries: [
        { path: "Cargo.toml", role: "production", category: "configuration" },
        { path: "src/main.rs", role: "production", category: "host" },
      ],
    };
    const manifestPath = path.join(root, "sloc-manifest.json");
    fs.writeFileSync(manifestPath, JSON.stringify(manifest));
    const result = analyze(manifestPath);
    if (result.production_sloc !== 5) fail(`expected 5 SLOC, got ${result.production_sloc}`);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
}

try {
  if (process.argv.length === 3 && process.argv[2] === "--self-test") {
    selfTest();
    console.log("self-test: ok");
  } else if (process.argv.length === 3) {
    console.log(JSON.stringify(analyze(process.argv[2]), null, 2));
  } else {
    fail("usage: node count-sloc.mjs <manifest.json> | --self-test");
  }
} catch (error) {
  console.error(`count-sloc: ${error.message}`);
  process.exitCode = 2;
}
