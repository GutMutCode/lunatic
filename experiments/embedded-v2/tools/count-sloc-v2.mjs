#!/usr/bin/env node

// Frozen candidate SLOC accounting tool for embedded-v2.
// Usage: node count-sloc-v2.mjs <manifest.json> | --self-test

import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import process from "node:process";

const EXTENSIONS = new Set([".rs", ".toml", ".wat", ".wit", ".json", ".yaml", ".yml"]);
const ROLES = new Set(["production", "test", "generated", "lockfile"]);

function requireValue(condition, message) {
  if (!condition) throw new Error(message);
}

function hash(file) {
  return crypto.createHash("sha256").update(fs.readFileSync(file)).digest("hex");
}

function syntax(file) {
  const extension = path.extname(file);
  if (extension === ".rs") return { line: "//", open: "/*", close: "*/", single: false };
  if (extension === ".wat" || extension === ".wit") return { line: ";;", open: "(;", close: ";)", single: false };
  if (extension === ".toml" || extension === ".yaml" || extension === ".yml") {
    return { line: "#", open: null, close: null, single: true };
  }
  if (extension === ".json") return null;
  throw new Error(`unsupported source type: ${file}`);
}

function stripComments(line, rules, state) {
  if (rules === null) return line;
  let output = "";
  let quote = null;
  let escaped = false;
  for (let index = 0; index < line.length;) {
    if (state.depth > 0) {
      if (rules.open && line.startsWith(rules.open, index)) {
        state.depth += 1;
        index += rules.open.length;
      } else if (rules.close && line.startsWith(rules.close, index)) {
        state.depth -= 1;
        index += rules.close.length;
      } else index += 1;
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
    if (character === '"' || (rules.single && character === "'")) {
      quote = character;
      output += character;
      index += 1;
      continue;
    }
    if (line.startsWith(rules.line, index)) break;
    if (rules.open && line.startsWith(rules.open, index)) {
      state.depth += 1;
      index += rules.open.length;
      continue;
    }
    output += character;
    index += 1;
  }
  return output;
}

function count(file) {
  const rules = syntax(file);
  const state = { depth: 0 };
  let result = 0;
  for (const line of fs.readFileSync(file, "utf8").split(/\r?\n/)) {
    if (stripComments(line, rules, state).trim()) result += 1;
  }
  requireValue(state.depth === 0, `unterminated block comment: ${file}`);
  return result;
}

function discover(root, current = root, result = new Set()) {
  for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
    if (entry.name === ".git" || entry.name === "target") continue;
    const file = path.join(current, entry.name);
    if (entry.isDirectory()) discover(root, file, result);
    else if (entry.isFile() && (EXTENSIONS.has(path.extname(file)) || entry.name === "Cargo.lock")) {
      result.add(path.relative(root, file).split(path.sep).join("/"));
    }
  }
  return result;
}

function analyze(manifestName) {
  const manifestFile = fs.realpathSync(manifestName);
  const manifest = JSON.parse(fs.readFileSync(manifestFile, "utf8"));
  requireValue(manifest.schema_version === 1, "schema_version must be 1");
  requireValue(typeof manifest.candidate === "string" && manifest.candidate.length > 0, "candidate must be non-empty");
  requireValue(typeof manifest.root === "string" && manifest.root.length > 0, "root must be non-empty");
  requireValue(Array.isArray(manifest.entries), "entries must be an array");
  const root = fs.realpathSync(path.resolve(path.dirname(manifestFile), manifest.root));
  requireValue(fs.statSync(root).isDirectory(), `candidate root is not a directory: ${root}`);

  const attributed = new Set();
  const files = [];
  const roleTotals = { generated: 0, lockfile: 0, production: 0, test: 0 };
  const categoryTotals = {};
  for (const entry of manifest.entries) {
    requireValue(entry && typeof entry === "object" && !Array.isArray(entry), "each entry must be an object");
    requireValue(typeof entry.path === "string" && entry.path.length > 0, "entry.path must be non-empty");
    const relative = entry.path.replaceAll("\\", "/");
    requireValue(!attributed.has(relative), `duplicate entry: ${relative}`);
    attributed.add(relative);
    requireValue(ROLES.has(entry.role), `invalid role for ${relative}: ${entry.role}`);
    requireValue(typeof entry.category === "string" && entry.category.length > 0, `category missing: ${relative}`);
    if (entry.role === "generated" || entry.role === "lockfile") {
      requireValue(typeof entry.reason === "string" && entry.reason.length > 0, `${entry.role} reason missing: ${relative}`);
    }
    const file = path.resolve(root, relative);
    const boundary = path.relative(root, file);
    requireValue(!boundary.startsWith("..") && !path.isAbsolute(boundary), `entry escapes root: ${relative}`);
    requireValue(fs.existsSync(file) && fs.statSync(file).isFile(), `file does not exist: ${relative}`);
    const sloc = entry.role === "generated" || entry.role === "lockfile" ? 0 : count(file);
    roleTotals[entry.role] += sloc;
    if (entry.role === "production") categoryTotals[entry.category] = (categoryTotals[entry.category] ?? 0) + sloc;
    files.push({ path: relative, role: entry.role, category: entry.category, sloc, sha256: hash(file) });
  }
  const actual = discover(root);
  const missing = [...actual].filter((file) => !attributed.has(file)).sort();
  const extra = [...attributed].filter((file) => !actual.has(file)).sort();
  requireValue(missing.length === 0, `unattributed supported files: ${JSON.stringify(missing)}`);
  requireValue(extra.length === 0, `unsupported or missing entries: ${JSON.stringify(extra)}`);
  files.sort((left, right) => left.path.localeCompare(right.path));
  return {
    schema_version: 1,
    candidate: manifest.candidate,
    manifest: manifestFile.split(path.sep).join("/"),
    manifest_sha256: hash(manifestFile),
    production_sloc: roleTotals.production,
    role_totals: roleTotals,
    category_totals: Object.fromEntries(Object.entries(categoryTotals).sort()),
    files,
  };
}

function selfTest() {
  const temporary = fs.mkdtempSync(path.join(os.tmpdir(), "embedded-v2-sloc-"));
  try {
    const candidate = path.join(temporary, "candidate");
    fs.mkdirSync(path.join(candidate, "src"), { recursive: true });
    fs.writeFileSync(path.join(candidate, "src", "main.rs"), '// c\nfn main() { /* c */ }\n/* c\nc */\nlet x = "//";\n');
    fs.writeFileSync(path.join(candidate, "Cargo.toml"), '# c\n[package]\nname = "x#y"\n');
    const manifest = {
      schema_version: 1,
      candidate: "self-test",
      root: "candidate",
      entries: [
        { path: "Cargo.toml", role: "production", category: "configuration" },
        { path: "src/main.rs", role: "production", category: "host" }
      ]
    };
    const manifestFile = path.join(temporary, "manifest.json");
    fs.writeFileSync(manifestFile, JSON.stringify(manifest));
    requireValue(analyze(manifestFile).production_sloc === 5, "self-test expected 5 SLOC");
  } finally {
    fs.rmSync(temporary, { recursive: true, force: true });
  }
}

try {
  if (process.argv.length === 3 && process.argv[2] === "--self-test") {
    selfTest();
    console.log("self-test: ok");
  } else {
    requireValue(process.argv.length === 3, "usage: node count-sloc-v2.mjs <manifest.json> | --self-test");
    console.log(JSON.stringify(analyze(process.argv[2]), null, 2));
  }
} catch (error) {
  console.error(`count-sloc-v2: ${error.message}`);
  process.exitCode = 2;
}
