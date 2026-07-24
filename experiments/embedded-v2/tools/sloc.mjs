#!/usr/bin/env node

// Frozen embedded-v2 SLOC tool. Node.js 24.4.1, no third-party packages.
// `production` is decision-bearing; all supported files must be attributed.

import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import process from "node:process";

const extensions = new Set([".rs", ".toml", ".wat", ".wit", ".json", ".yaml", ".yml"]);
const roles = new Set(["production", "test", "generated", "lockfile"]);
const check = (value, message) => { if (!value) throw new Error(message); };
const digest = (file) => crypto.createHash("sha256").update(fs.readFileSync(file)).digest("hex");

function commentRules(file) {
  switch (path.extname(file)) {
    case ".rs": return ["//", "/*", "*/", false];
    case ".wat": case ".wit": return [";;", "(;", ";)", false];
    case ".toml": case ".yaml": case ".yml": return ["#", null, null, true];
    case ".json": return null;
    default: throw new Error(`unsupported source type: ${file}`);
  }
}

function uncomment(line, rules, state) {
  if (rules === null) return line;
  const [lineMark, open, close, singleQuotes] = rules;
  let result = "", quote = null, escaped = false, index = 0;
  while (index < line.length) {
    if (state.depth > 0) {
      if (open && line.startsWith(open, index)) { state.depth += 1; index += open.length; }
      else if (close && line.startsWith(close, index)) { state.depth -= 1; index += close.length; }
      else index += 1;
      continue;
    }
    const character = line[index];
    if (quote !== null) {
      result += character;
      if (escaped) escaped = false;
      else if (character === "\\") escaped = true;
      else if (character === quote) quote = null;
      index += 1;
    } else if (character === '"' || (singleQuotes && character === "'")) {
      quote = character; result += character; index += 1;
    } else if (line.startsWith(lineMark, index)) break;
    else if (open && line.startsWith(open, index)) { state.depth += 1; index += open.length; }
    else { result += character; index += 1; }
  }
  return result;
}

function sloc(file) {
  const state = { depth: 0 }, rules = commentRules(file);
  const count = fs.readFileSync(file, "utf8").split(/\r?\n/)
    .filter((line) => uncomment(line, rules, state).trim().length > 0).length;
  check(state.depth === 0, `unterminated block comment: ${file}`);
  return count;
}

function findSources(root, current = root, found = new Set()) {
  for (const item of fs.readdirSync(current, { withFileTypes: true })) {
    if (item.name === ".git" || item.name === "target") continue;
    const absolute = path.join(current, item.name);
    if (item.isDirectory()) findSources(root, absolute, found);
    else if (item.isFile() && (extensions.has(path.extname(absolute)) || item.name === "Cargo.lock")) {
      found.add(path.relative(root, absolute).split(path.sep).join("/"));
    }
  }
  return found;
}

function analyze(manifestArgument) {
  const manifestFile = fs.realpathSync(manifestArgument);
  const manifest = JSON.parse(fs.readFileSync(manifestFile, "utf8"));
  check(manifest.schema_version === 1, "schema_version must be 1");
  check(typeof manifest.candidate === "string" && manifest.candidate, "candidate must be non-empty");
  check(typeof manifest.root === "string" && manifest.root, "root must be non-empty");
  check(Array.isArray(manifest.entries), "entries must be an array");
  const root = fs.realpathSync(path.resolve(path.dirname(manifestFile), manifest.root));
  check(fs.statSync(root).isDirectory(), `candidate root is not a directory: ${root}`);
  const seen = new Set(), files = [], roleTotals = { generated: 0, lockfile: 0, production: 0, test: 0 };
  const categoryTotals = {};
  for (const entry of manifest.entries) {
    check(entry && typeof entry === "object" && !Array.isArray(entry), "entry must be an object");
    check(typeof entry.path === "string" && entry.path, "entry.path must be non-empty");
    const relative = entry.path.replaceAll("\\", "/");
    check(!seen.has(relative), `duplicate entry: ${relative}`); seen.add(relative);
    check(roles.has(entry.role), `invalid role: ${relative}`);
    check(typeof entry.category === "string" && entry.category, `category missing: ${relative}`);
    if (entry.role === "generated" || entry.role === "lockfile") check(typeof entry.reason === "string" && entry.reason, `reason missing: ${relative}`);
    const file = path.resolve(root, relative), boundary = path.relative(root, file);
    check(!boundary.startsWith("..") && !path.isAbsolute(boundary), `entry escapes root: ${relative}`);
    check(fs.existsSync(file) && fs.statSync(file).isFile(), `file missing: ${relative}`);
    const lines = entry.role === "generated" || entry.role === "lockfile" ? 0 : sloc(file);
    roleTotals[entry.role] += lines;
    if (entry.role === "production") categoryTotals[entry.category] = (categoryTotals[entry.category] ?? 0) + lines;
    files.push({ path: relative, role: entry.role, category: entry.category, sloc: lines, sha256: digest(file) });
  }
  const actual = findSources(root);
  const missing = [...actual].filter((file) => !seen.has(file)).sort();
  const extra = [...seen].filter((file) => !actual.has(file)).sort();
  check(missing.length === 0, `unattributed files: ${JSON.stringify(missing)}`);
  check(extra.length === 0, `unsupported entries: ${JSON.stringify(extra)}`);
  files.sort((a, b) => a.path.localeCompare(b.path));
  return { schema_version: 1, candidate: manifest.candidate, manifest_sha256: digest(manifestFile), production_sloc: roleTotals.production, role_totals: roleTotals, category_totals: Object.fromEntries(Object.entries(categoryTotals).sort()), files };
}

function selfTest() {
  const temporary = fs.mkdtempSync(path.join(os.tmpdir(), "embedded-v2-sloc-"));
  try {
    const candidate = path.join(temporary, "candidate");
    fs.mkdirSync(path.join(candidate, "src"), { recursive: true });
    fs.writeFileSync(path.join(candidate, "src", "main.rs"), '// comment\nfn main() { /* inline */ }\n/* block\ncomment */\nlet x = "//";\n');
    fs.writeFileSync(path.join(candidate, "Cargo.toml"), '# comment\n[package]\nname = "fixture#name"\n');
    const manifest = { schema_version: 1, candidate: "self-test", root: "candidate", entries: [
      { path: "Cargo.toml", role: "production", category: "configuration" },
      { path: "src/main.rs", role: "production", category: "host" }
    ] };
    const file = path.join(temporary, "manifest.json"); fs.writeFileSync(file, JSON.stringify(manifest));
    check(analyze(file).production_sloc === 4, "self-test expected 4 SLOC");
  } finally { fs.rmSync(temporary, { recursive: true, force: true }); }
}

try {
  check(process.argv.length === 3, "usage: node sloc.mjs <manifest.json> | --self-test");
  if (process.argv[2] === "--self-test") { selfTest(); console.log("self-test: ok"); }
  else console.log(JSON.stringify(analyze(process.argv[2]), null, 2));
} catch (error) { console.error(`sloc: ${error.message}`); process.exitCode = 2; }
