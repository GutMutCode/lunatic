#!/usr/bin/env node

// Canonical frozen embedded-v3 SLOC/accounting tool. Node.js 24, no third-party packages.
// Every regular file in a candidate tree is attributed. Only the candidate's
// root-level target/ directory is excluded.

import childProcess from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const TOOL_DIRECTORY = path.dirname(fileURLToPath(import.meta.url));
const EXPERIMENT_ROOT = path.resolve(TOOL_DIRECTORY, "..");
const CANDIDATES_ROOT = path.resolve(EXPERIMENT_ROOT, "candidates");
const ORACLE_ROOT = path.resolve(EXPERIMENT_ROOT, "oracle");
const REPOSITORY_ROOT = path.resolve(TOOL_DIRECTORY, "../../..");
const REPOSITORY_SRC = path.join(REPOSITORY_ROOT, "src");
const REPOSITORY_CRATES = path.join(REPOSITORY_ROOT, "crates");
const CANDIDATES = new Set(["lunatic", "direct-wasmtime", "extism"]);
const ROLES = new Set(["production", "test", "generated", "lockfile"]);
const ENTRY_PROPERTIES = new Set([
  "path",
  "role",
  "category",
  "reason",
  "generator",
  "source",
]);
const TOP_LEVEL_PROPERTIES = new Set(["schema_version", "candidate", "entries"]);
const UTF8_DECODER = new TextDecoder("utf-8", { fatal: true });

function check(condition, message) {
  if (!condition) throw new Error(message);
}

function nonEmptyString(value) {
  return typeof value === "string" && value.trim().length > 0;
}

function normalizedForComparison(value) {
  const normalized = path.normalize(value).replace(/[\\/]+$/, "");
  return process.platform === "win32" ? normalized.toLowerCase() : normalized;
}

function samePath(left, right) {
  return normalizedForComparison(left) === normalizedForComparison(right);
}

function isWithin(parent, child) {
  const relative = path.relative(parent, child);
  return (
    relative === "" ||
    (!path.isAbsolute(relative) && relative !== ".." && !relative.startsWith(`..${path.sep}`))
  );
}

function sha256(bufferOrFile) {
  const contents = Buffer.isBuffer(bufferOrFile)
    ? bufferOrFile
    : fs.readFileSync(bufferOrFile);
  return crypto.createHash("sha256").update(contents).digest("hex");
}

function decodeUtf8(contents, relativePath) {
  check(!contents.includes(0), `binary/NUL content is forbidden for counted text: ${relativePath}`);
  try {
    return UTF8_DECODER.decode(contents);
  } catch (error) {
    throw new Error(`production/test file is not valid UTF-8: ${relativePath} (${error.message})`);
  }
}

function nonBlankPhysicalLines(contents, relativePath) {
  return decodeUtf8(contents, relativePath)
    .split(/\r\n|\n|\r/)
    .filter((line) => line.trim().length > 0).length;
}

function normalizedRelativePath(value) {
  check(nonEmptyString(value), "entry.path must be a non-empty string");
  check(!value.includes("\\"), `entry.path must use '/' separators: ${value}`);
  check(!value.includes("\0"), `entry.path contains NUL: ${value}`);
  check(!path.posix.isAbsolute(value), `entry.path must be relative: ${value}`);
  check(path.posix.normalize(value) === value, `entry.path is not normalized: ${value}`);
  check(value !== "." && value !== ".." && !value.startsWith("../"), `entry.path escapes candidate root: ${value}`);
  check(!value.endsWith("/"), `entry.path names a directory: ${value}`);
  return value;
}

function assertNoReparseRedirect(absolute, expectedRealPath, relativePath) {
  const information = fs.lstatSync(absolute);
  check(!information.isSymbolicLink(), `symlink/reparse point is forbidden: ${relativePath}`);
  const actualRealPath = fs.realpathSync.native(absolute);
  check(
    samePath(actualRealPath, expectedRealPath),
    `reparse-point redirect is forbidden: ${relativePath} -> ${actualRealPath}`,
  );
  return information;
}

function enumerateCandidate(candidateRoot) {
  check(fs.existsSync(candidateRoot), `candidate root does not exist: ${candidateRoot}`);
  const rootInformation = fs.lstatSync(candidateRoot);
  check(!rootInformation.isSymbolicLink(), `candidate root is a symlink/reparse point: ${candidateRoot}`);
  check(rootInformation.isDirectory(), `candidate root is not a directory: ${candidateRoot}`);
  const rootRealPath = fs.realpathSync.native(candidateRoot);
  const files = [];
  let excludedRootTarget = false;

  function visit(directory, relativeDirectory) {
    const children = fs.readdirSync(directory).sort((left, right) => left.localeCompare(right, "en"));
    for (const name of children) {
      const relativePath = relativeDirectory ? `${relativeDirectory}/${name}` : name;
      const absolute = path.join(directory, name);
      const expectedRealPath = path.join(rootRealPath, ...relativePath.split("/"));
      const information = assertNoReparseRedirect(absolute, expectedRealPath, relativePath);

      if (relativePath === "target" && information.isDirectory()) {
        excludedRootTarget = true;
        continue;
      }
      if (information.isDirectory()) {
        visit(absolute, relativePath);
      } else if (information.isFile()) {
        files.push(relativePath);
      } else {
        throw new Error(`special/non-regular filesystem entry is forbidden: ${relativePath}`);
      }
    }
  }

  visit(candidateRoot, "");
  return { files, excludedRootTarget, rootRealPath };
}

function isTestPath(relativePath) {
  const pieces = relativePath.split("/");
  const basename = pieces.at(-1);
  return pieces.slice(0, -1).includes("tests") || /^[^/]+_test\..+$/.test(basename);
}

function validateManifestShape(manifest, expectedCandidate) {
  check(manifest && typeof manifest === "object" && !Array.isArray(manifest), "manifest must be an object");
  for (const key of Object.keys(manifest)) {
    check(TOP_LEVEL_PROPERTIES.has(key), `unknown manifest property: ${key}`);
  }
  check(manifest.schema_version === 3, "schema_version must be 3");
  check(manifest.candidate === expectedCandidate, `manifest candidate must be ${expectedCandidate}`);
  check(Array.isArray(manifest.entries), "entries must be an array");
}

function parseAndValidateEntries(manifest, candidateRoot, enumeratedFiles) {
  const seen = new Set();
  const entries = new Map();

  for (const [index, entry] of manifest.entries.entries()) {
    check(entry && typeof entry === "object" && !Array.isArray(entry), `entries[${index}] must be an object`);
    for (const key of Object.keys(entry)) {
      check(ENTRY_PROPERTIES.has(key), `unknown entry property at entries[${index}]: ${key}`);
    }
    const relativePath = normalizedRelativePath(entry.path);
    check(!seen.has(relativePath), `duplicate manifest attribution: ${relativePath}`);
    seen.add(relativePath);
    check(ROLES.has(entry.role), `invalid role for ${relativePath}: ${entry.role}`);
    check(nonEmptyString(entry.category), `category is required for ${relativePath}`);

    if (entry.role === "generated" || entry.role === "lockfile") {
      check(nonEmptyString(entry.reason), `reason is required for ${entry.role} file ${relativePath}`);
      check(nonEmptyString(entry.generator), `generator is required for ${entry.role} file ${relativePath}`);
      check(nonEmptyString(entry.source), `source is required for ${entry.role} file ${relativePath}`);
    }
    if (entry.role === "test") {
      check(isTestPath(relativePath), `test role is allowed only under tests/ or for *_test.*: ${relativePath}`);
    }
    if (entry.role === "lockfile") {
      check(path.posix.basename(relativePath) === "Cargo.lock", `only Cargo.lock may use the lockfile role: ${relativePath}`);
    }
    if (path.posix.basename(relativePath) === "Cargo.lock") {
      check(entry.role === "lockfile", `Cargo.lock must use the lockfile role: ${relativePath}`);
    }

    const resolved = path.resolve(candidateRoot, ...relativePath.split("/"));
    check(isWithin(candidateRoot, resolved), `entry escapes candidate root: ${relativePath}`);
    entries.set(relativePath, entry);
  }

  check(seen.has("sloc-manifest.json"), "sloc-manifest.json must attribute itself");
  const actual = new Set(enumeratedFiles);
  const missing = enumeratedFiles.filter((relativePath) => !seen.has(relativePath));
  const extra = [...seen].filter((relativePath) => !actual.has(relativePath)).sort();
  check(missing.length === 0, `unattributed regular files: ${JSON.stringify(missing)}`);
  check(extra.length === 0, `manifest entries do not name enumerated regular files: ${JSON.stringify(extra)}`);
  return entries;
}

function classifyExternalPath(candidate, candidateRoot, resolvedPath) {
  if (isWithin(candidateRoot, resolvedPath)) return "candidate-internal";
  if (isWithin(ORACLE_ROOT, resolvedPath)) {
    throw new Error(`oracle path dependency is forbidden: ${resolvedPath}`);
  }

  if (candidate === "direct-wasmtime") {
    throw new Error(`direct-wasmtime path dependency escapes candidate root: ${resolvedPath}`);
  }
  if (candidate === "extism") {
    throw new Error(`extism path dependency escapes candidate root: ${resolvedPath}`);
  }

  if (samePath(resolvedPath, REPOSITORY_ROOT) || isWithin(REPOSITORY_SRC, resolvedPath)) {
    return "external-existing-runtime:repo-src";
  }
  if (isWithin(REPOSITORY_CRATES, resolvedPath)) {
    return "external-existing-runtime:repo-crates";
  }
  throw new Error(`lunatic path dependency is not an allowed repo src/crates runtime path: ${resolvedPath}`);
}

function cratesIoSource(source) {
  return (
    typeof source === "string" &&
    source.startsWith("registry+") &&
    (source.includes("crates.io-index") || source.includes("index.crates.io"))
  );
}

function validateCargoMetadata(candidate, candidateRoot, enumeratedFiles) {
  const cargoManifests = enumeratedFiles.filter((relativePath) => path.posix.basename(relativePath) === "Cargo.toml");
  if (cargoManifests.length === 0) {
    return { status: "not-applicable", reason: "candidate contains no Cargo.toml" };
  }
  check(cargoManifests.includes("Cargo.toml"), "a candidate containing Cargo.toml files must have one at its root");

  const manifestPath = path.join(candidateRoot, "Cargo.toml");
  const commandArguments = ["metadata", "--format-version", "1", "--locked", "--manifest-path", manifestPath];
  const result = childProcess.spawnSync("cargo", commandArguments, {
    cwd: candidateRoot,
    encoding: "utf8",
    maxBuffer: 64 * 1024 * 1024,
    windowsHide: true,
  });
  if (result.error) throw new Error(`cargo metadata --locked could not run: ${result.error.message}`);
  check(
    result.status === 0,
    `cargo metadata --locked failed (exit ${result.status}): ${(result.stderr || result.stdout).trim()}`,
  );

  let metadata;
  try {
    metadata = JSON.parse(result.stdout);
  } catch (error) {
    throw new Error(`cargo metadata returned invalid JSON: ${error.message}`);
  }
  check(Array.isArray(metadata.packages), "cargo metadata JSON has no packages array");

  const externalPaths = new Map();
  const candidatePackages = [];
  const recordPath = (resolvedPath, classification, owner) => {
    if (classification === "candidate-internal") return;
    const key = `${classification}\0${normalizedForComparison(resolvedPath)}`;
    const existing = externalPaths.get(key) ?? { path: resolvedPath, classification, owners: new Set() };
    existing.owners.add(owner);
    externalPaths.set(key, existing);
  };

  for (const pkg of metadata.packages) {
    check(nonEmptyString(pkg.manifest_path), `cargo package ${pkg.name ?? "<unknown>"} lacks manifest_path`);
    const manifestDirectory = fs.realpathSync.native(path.dirname(pkg.manifest_path));
    if (pkg.source === null) {
      const classification = classifyExternalPath(candidate, candidateRoot, manifestDirectory);
      recordPath(manifestDirectory, classification, `package:${pkg.name}@${pkg.version}`);
    }
    if (isWithin(candidateRoot, manifestDirectory)) candidatePackages.push(pkg);

    for (const dependency of pkg.dependencies ?? []) {
      if (dependency.path === null || dependency.path === undefined) continue;
      check(fs.existsSync(dependency.path), `cargo metadata path dependency does not exist: ${dependency.path}`);
      const dependencyPath = fs.realpathSync.native(dependency.path);
      const classification = classifyExternalPath(candidate, candidateRoot, dependencyPath);
      recordPath(
        dependencyPath,
        classification,
        `dependency:${pkg.name}@${pkg.version}->${dependency.name}`,
      );
    }
  }

  let extismVersion = null;
  if (candidate === "extism") {
    const declarations = candidatePackages.flatMap((pkg) =>
      (pkg.dependencies ?? [])
        .filter((dependency) => dependency.name === "extism")
        .map((dependency) => ({ pkg, dependency })),
    );
    check(declarations.length > 0, "extism candidate must declare the extism crate");
    for (const { pkg, dependency } of declarations) {
      check(
        cratesIoSource(dependency.source),
        `extism dependency of ${pkg.name} must come from crates.io, got ${dependency.source}`,
      );
      check(
        dependency.req === "=1.30.0",
        `extism dependency of ${pkg.name} must be pinned as '=1.30.0', got ${dependency.req}`,
      );
    }
    const resolvedExtism = metadata.packages.filter((pkg) => pkg.name === "extism");
    check(resolvedExtism.length > 0, "cargo metadata did not resolve extism");
    for (const pkg of resolvedExtism) {
      check(pkg.version === "1.30.0", `resolved extism version must be 1.30.0, got ${pkg.version}`);
      check(cratesIoSource(pkg.source), `resolved extism package must come from crates.io, got ${pkg.source}`);
    }
    extismVersion = "1.30.0";
  }

  return {
    status: "verified",
    command: `cargo ${commandArguments.slice(0, 4).join(" ")} --manifest-path <candidate>/Cargo.toml`,
    package_count: metadata.packages.length,
    candidate_package_count: candidatePackages.length,
    extism_version: extismVersion,
    external_path_dependencies: [...externalPaths.values()]
      .map((item) => ({ ...item, owners: [...item.owners].sort() }))
      .sort((left, right) => left.path.localeCompare(right.path, "en")),
  };
}

function parseManifest(manifestPath) {
  const contents = fs.readFileSync(manifestPath);
  let text;
  try {
    text = UTF8_DECODER.decode(contents);
  } catch (error) {
    throw new Error(`manifest is not valid UTF-8: ${error.message}`);
  }
  try {
    return { manifest: JSON.parse(text), contents };
  } catch (error) {
    throw new Error(`manifest is not valid JSON: ${error.message}`);
  }
}

function analyzeCandidate(candidate, candidateRoot) {
  check(CANDIDATES.has(candidate), `unsupported candidate: ${candidate}`);
  const expectedRoot = path.resolve(candidateRoot);
  const manifestPath = path.join(expectedRoot, "sloc-manifest.json");
  const enumeration = enumerateCandidate(expectedRoot);
  check(enumeration.files.includes("sloc-manifest.json"), "candidate root has no sloc-manifest.json");
  const { manifest, contents: manifestContents } = parseManifest(manifestPath);
  validateManifestShape(manifest, candidate);
  const entries = parseAndValidateEntries(manifest, expectedRoot, enumeration.files);

  const roleTotals = Object.fromEntries(
    [...ROLES].sort().map((role) => [role, { files: 0, bytes: 0, sloc: 0 }]),
  );
  const categoryTotals = {};
  const files = [];
  for (const relativePath of enumeration.files) {
    const entry = entries.get(relativePath);
    const absolute = path.join(expectedRoot, ...relativePath.split("/"));
    const contents = fs.readFileSync(absolute);
    const lines = entry.role === "production" || entry.role === "test"
      ? nonBlankPhysicalLines(contents, relativePath)
      : 0;
    const roleTotal = roleTotals[entry.role];
    roleTotal.files += 1;
    roleTotal.bytes += contents.length;
    roleTotal.sloc += lines;
    const category = categoryTotals[entry.category] ?? { production_sloc: 0, test_sloc: 0, files: 0 };
    category.files += 1;
    if (entry.role === "production") category.production_sloc += lines;
    if (entry.role === "test") category.test_sloc += lines;
    categoryTotals[entry.category] = category;
    files.push({
      path: relativePath,
      role: entry.role,
      category: entry.category,
      bytes: contents.length,
      sloc: lines,
      sha256: sha256(contents),
    });
  }

  const cargoMetadata = validateCargoMetadata(candidate, expectedRoot, enumeration.files);
  return {
    schema_version: 3,
    candidate,
    manifest_sha256: sha256(manifestContents),
    production_sloc: roleTotals.production.sloc,
    test_sloc: roleTotals.test.sloc,
    enumeration: {
      regular_files: enumeration.files.length,
      excluded_root_target: enumeration.excludedRootTarget,
      exclusion: "only <candidate>/target/ when it is a real directory",
    },
    role_totals: roleTotals,
    category_totals: Object.fromEntries(
      Object.entries(categoryTotals).sort(([left], [right]) => left.localeCompare(right, "en")),
    ),
    cargo_metadata: cargoMetadata,
    files,
  };
}

function candidateFromCliManifest(argument) {
  const suppliedPath = path.resolve(process.cwd(), argument);
  for (const candidate of [...CANDIDATES].sort()) {
    const allowedPath = path.join(CANDIDATES_ROOT, candidate, "sloc-manifest.json");
    if (samePath(suppliedPath, allowedPath)) {
      return { candidate, candidateRoot: path.dirname(allowedPath) };
    }
  }
  throw new Error(
    `manifest must be exactly ../candidates/{lunatic,direct-wasmtime,extism}/sloc-manifest.json relative to this tool`,
  );
}

function copyFixture(destination) {
  const source = path.join(TOOL_DIRECTORY, "test-fixtures", "normal");
  fs.cpSync(source, destination, { recursive: true, errorOnExist: true });
}

function expectFailure(label, operation, pattern) {
  let caught = null;
  try {
    operation();
  } catch (error) {
    caught = error;
  }
  check(caught !== null, `${label}: expected failure but analysis succeeded`);
  check(pattern.test(caught.message), `${label}: unexpected error: ${caught.message}`);
}

function selfTest() {
  const temporary = fs.mkdtempSync(path.join(os.tmpdir(), "embedded-v3-sloc-"));
  const results = {
    normal_fixture: "pending",
    missing_attribution: "pending",
    symlink_reparse: "pending",
    extism_external_path: "pending",
  };
  try {
    const normalRoot = path.join(temporary, "normal", "direct-wasmtime");
    fs.mkdirSync(path.dirname(normalRoot), { recursive: true });
    copyFixture(normalRoot);
    const normal = analyzeCandidate("direct-wasmtime", normalRoot);
    check(normal.production_sloc === 6, `normal fixture expected 6 production SLOC, got ${normal.production_sloc}`);
    check(normal.test_sloc === 3, `normal fixture expected 3 test SLOC, got ${normal.test_sloc}`);
    check(normal.cargo_metadata.status === "verified", "normal fixture cargo metadata was not verified");
    results.normal_fixture = "ok";

    const missingRoot = path.join(temporary, "missing", "direct-wasmtime");
    fs.mkdirSync(path.dirname(missingRoot), { recursive: true });
    copyFixture(missingRoot);
    fs.writeFileSync(path.join(missingRoot, "unattributed.dat"), "must be attributed\n");
    expectFailure(
      "missing attribution",
      () => analyzeCandidate("direct-wasmtime", missingRoot),
      /unattributed regular files/,
    );
    results.missing_attribution = "ok";

    const symlinkRoot = path.join(temporary, "symlink", "direct-wasmtime");
    fs.mkdirSync(path.dirname(symlinkRoot), { recursive: true });
    copyFixture(symlinkRoot);
    const outside = path.join(temporary, "outside.txt");
    fs.writeFileSync(outside, "outside\n");
    try {
      fs.symlinkSync(outside, path.join(symlinkRoot, "redirect.txt"), "file");
      expectFailure(
        "symlink/reparse",
        () => analyzeCandidate("direct-wasmtime", symlinkRoot),
        /symlink\/reparse|reparse-point redirect/,
      );
      results.symlink_reparse = "ok";
    } catch (error) {
      if (["EPERM", "EACCES", "ENOSYS", "UNKNOWN"].includes(error.code)) {
        results.symlink_reparse = `skipped: platform denied symlink creation (${error.code})`;
      } else {
        throw error;
      }
    }
    const extismCase = path.join(temporary, "extism-path");
    const extismRoot = path.join(extismCase, "extism");
    const outsideCrate = path.join(extismCase, "outside-crate");
    fs.mkdirSync(path.dirname(extismRoot), { recursive: true });
    copyFixture(extismRoot);
    fs.mkdirSync(path.join(outsideCrate, "src"), { recursive: true });
    fs.writeFileSync(
      path.join(outsideCrate, "Cargo.toml"),
      '[package]\nname = "outside-crate"\nversion = "0.0.0"\nedition = "2021"\n',
    );
    fs.writeFileSync(path.join(outsideCrate, "src", "lib.rs"), "pub fn outside() {}\n");
    fs.writeFileSync(
      path.join(extismRoot, "Cargo.toml"),
      '[package]\nname = "embedded-v3-sloc-fixture"\nversion = "0.0.0"\nedition = "2021"\n\n[dependencies]\noutside-crate = { path = "../outside-crate" }\n',
    );
    fs.writeFileSync(
      path.join(extismRoot, "Cargo.lock"),
      '# This file is automatically @generated by Cargo.\n# It is not intended for manual editing.\nversion = 4\n\n[[package]]\nname = "embedded-v3-sloc-fixture"\nversion = "0.0.0"\ndependencies = [\n "outside-crate",\n]\n\n[[package]]\nname = "outside-crate"\nversion = "0.0.0"\n',
    );
    const extismManifestPath = path.join(extismRoot, "sloc-manifest.json");
    const extismManifest = JSON.parse(fs.readFileSync(extismManifestPath, "utf8"));
    extismManifest.candidate = "extism";
    fs.writeFileSync(extismManifestPath, `${JSON.stringify(extismManifest, null, 2)}\n`);
    expectFailure(
      "extism external path",
      () => analyzeCandidate("extism", extismRoot),
      /extism path dependency escapes candidate root/,
    );
    results.extism_external_path = "ok";

    return results;
  } finally {
    fs.rmSync(temporary, { recursive: true, force: true });
  }
}

try {
  check(process.argv.length === 3, "usage: node sloc.mjs <fixed candidate sloc-manifest.json> | --self-test");
  if (process.argv[2] === "--self-test") {
    console.log(JSON.stringify({ self_test: "ok", ...selfTest() }, null, 2));
  } else {
    const { candidate, candidateRoot } = candidateFromCliManifest(process.argv[2]);
    console.log(JSON.stringify(analyzeCandidate(candidate, candidateRoot), null, 2));
  }
} catch (error) {
  console.error(`sloc: ${error.message}`);
  process.exitCode = 2;
}
