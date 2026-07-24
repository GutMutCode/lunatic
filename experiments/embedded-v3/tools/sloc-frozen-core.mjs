// Frozen embedded-v3 SLOC policy core. Node.js 24, no third-party packages.

import childProcess from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const TOOL_DIRECTORY = path.dirname(fileURLToPath(import.meta.url));
export const EXPERIMENT_ROOT = path.resolve(TOOL_DIRECTORY, "..");
const EXPERIMENT_REAL_ROOT = fs.realpathSync.native(EXPERIMENT_ROOT);
const CANDIDATES_ROOT = path.resolve(EXPERIMENT_ROOT, "candidates");
const ORACLE_ROOT = fs.realpathSync.native(path.resolve(EXPERIMENT_ROOT, "oracle"));
const GUESTS_ROOT = fs.realpathSync.native(path.resolve(EXPERIMENT_ROOT, "guests", "core"));
const REPOSITORY_ROOT = fs.realpathSync.native(path.resolve(TOOL_DIRECTORY, "../../.."));
const REPOSITORY_SRC = fs.realpathSync.native(path.join(REPOSITORY_ROOT, "src"));
const REPOSITORY_CRATES = fs.realpathSync.native(path.join(REPOSITORY_ROOT, "crates"));
const CANDIDATES = new Set(["lunatic", "direct-wasmtime", "extism"]);
const ROLES = new Set(["production", "test", "generated", "lockfile", "accounting"]);
const GENERATED_KINDS = new Set(["canonical-copy", "binary-guest"]);
const ENTRY_PROPERTIES = new Set([
  "path",
  "role",
  "category",
  "reason",
  "generator",
  "source",
  "generated_kind",
  "canonical_id",
  "sha256",
]);
const TOP_LEVEL_PROPERTIES = new Set(["schema_version", "candidate", "entries"]);
const BASE_ENTRY_PROPERTIES = new Set(["path", "role", "category"]);
const PROVENANCE_PROPERTIES = new Set(["reason", "generator", "source"]);
const GENERATED_PROPERTIES = new Set([
  ...BASE_ENTRY_PROPERTIES,
  ...PROVENANCE_PROPERTIES,
  "generated_kind",
  "sha256",
]);
const SHA256_PATTERN = /^[0-9a-f]{64}$/;
const TEST_PATH_PATTERN = /^tests\/(?:[A-Za-z0-9_.-]+\/)*[A-Za-z0-9_.-]+\.rs$/;
const BINARY_GUEST_PATH_PATTERN = /^guest-artifacts\/(?:[A-Za-z0-9_.-]+\/)*[A-Za-z0-9_.-]+\.wasm$/;
const INCLUDE_ESCAPE_PATTERN = /(?:#\s*\[\s*path\s*=|\binclude(?:_str|_bytes)?\s*!\s*\()/m;
const MAX_COUNTED_LINE_BYTES = 120;
const EXPECTED_RUSTFMT_VERSION = "rustfmt 1.9.0-stable (59807616e1 2026-04-14)";
const PINNED_LUNATIC_PRODUCT_COMMIT = "f5ba0831ef757e2a134fbeafe622c0d0280a55dd";
const LUNATIC_PRODUCT_PATHS = ["Cargo.toml", "Cargo.lock", "src", "crates"];
const CRATES_IO_SOURCE = "registry+https://github.com/rust-lang/crates.io-index";
export const WASM_CORE_HEADER = Buffer.from([0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]);
const UTF8_DECODER = new TextDecoder("utf-8", { fatal: true });

function registryDependency(version, features) {
  return Object.freeze({
    source: CRATES_IO_SOURCE,
    version,
    requirement: `=${version}`,
    kind: "normal",
    default_features: false,
    features: Object.freeze([...features].sort()),
  });
}

function productPathDependency(product_path, version) {
  return Object.freeze({
    product_path,
    version,
    requirement: `=${version}`,
    kind: "normal",
    default_features: false,
    features: Object.freeze([]),
  });
}

const COMMON_REGISTRY_DEPENDENCIES = Object.freeze({
  anyhow: registryDependency("1.0.100", ["std"]),
  serde: registryDependency("1.0.229", ["derive", "std"]),
  serde_json: registryDependency("1.0.151", ["std"]),
  sha2: registryDependency("0.10.9", ["std"]),
  tokio: registryDependency("1.53.1", ["macros", "rt-multi-thread", "sync", "time"]),
});

function registryPolicy(additional) {
  return Object.freeze({ ...COMMON_REGISTRY_DEPENDENCIES, ...additional });
}

export const FROZEN_DIRECT_DEPENDENCY_POLICY = Object.freeze({
  lunatic: Object.freeze({
    required_registry: Object.freeze([]),
    required_product_paths: Object.freeze(["lunatic-process"]),
    registry: registryPolicy({
      // ProcessState::register exposes wasmtime::Linker in its public signature.
      // Source review must reject using this declaration to bypass Lunatic.
      wasmtime: registryDependency("46.0.1", []),
    }),
    product_paths: Object.freeze({
      "lunatic-process": productPathDependency("crates/lunatic-process", "0.14.0"),
    }),
  }),
  "direct-wasmtime": Object.freeze({
    required_registry: Object.freeze(["wasmtime"]),
    required_product_paths: Object.freeze([]),
    registry: registryPolicy({
      wasmtime: registryDependency("46.0.1", ["cranelift", "runtime"]),
    }),
    product_paths: Object.freeze({}),
  }),
  extism: Object.freeze({
    required_registry: Object.freeze(["extism"]),
    required_product_paths: Object.freeze([]),
    registry: registryPolicy({
      extism: registryDependency("1.30.0", ["wasmtime-default-features"]),
    }),
    product_paths: Object.freeze({}),
  }),
});

export const ACCOUNTING_METADATA = Object.freeze({
  path: "sloc-manifest.json",
  category: "accounting-metadata",
});
export const LOCKFILE_METADATA = Object.freeze({
  path: "Cargo.lock",
  category: "dependency-lock",
  reason: "Cargo dependency resolution; verified by cargo metadata --locked",
  generator: "cargo",
  source: "Cargo.toml",
});
export const BINARY_GUEST_METADATA = Object.freeze({
  category: "guest-artifact",
  reason: "Compiled WebAssembly guest artifact; source remains separately attributed",
});
export const CANONICAL_EXCLUSIONS = new Map([
  [
    "embedded-v3-candidate-wire-v3",
    Object.freeze({
      destination: "src/protocol.rs",
      source: "protocol/candidate-wire-frozen.rs",
      sha256: "55866a0037dd1c1c7edff270f873e1172bbab9c946196a9815086aa54df57682",
      category: "common-protocol",
      reason: "Frozen runtime-neutral candidate wire bytes shared by every candidate",
      generator: "exact-copy",
    }),
  ],
]);

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

export function sha256(bufferOrFile) {
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
  const lines = decodeUtf8(contents, relativePath).split(/\r\n|\n|\r/);
  for (const [index, line] of lines.entries()) {
    check(
      Buffer.byteLength(line, "utf8") <= MAX_COUNTED_LINE_BYTES,
      `counted text line exceeds frozen ${MAX_COUNTED_LINE_BYTES}-byte anti-minification limit: ${relativePath}:${index + 1}`,
    );
  }
  return lines.filter((line) => line.trim().length > 0).length;
}

function normalizedRelativePath(value, label = "entry.path") {
  check(nonEmptyString(value), `${label} must be a non-empty string`);
  check(!value.includes("\\"), `${label} must use '/' separators: ${value}`);
  check(!/[\u0000-\u001f:*?"<>|]/u.test(value), `${label} contains a non-portable character: ${value}`);
  check(!path.posix.isAbsolute(value), `${label} must be relative: ${value}`);
  check(path.posix.normalize(value) === value, `${label} is not normalized: ${value}`);
  check(value !== "." && value !== ".." && !value.startsWith("../"), `${label} escapes its root: ${value}`);
  check(!value.endsWith("/"), `${label} names a directory: ${value}`);
  for (const component of value.split("/")) {
    check(!component.endsWith(".") && !component.endsWith(" "), `${label} has a non-portable component: ${value}`);
  }
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

function assertExternalRegularFile(root, rootRealPath, relativePath, label) {
  const normalized = normalizedRelativePath(relativePath, label);
  const absolute = path.join(root, ...normalized.split("/"));
  check(fs.existsSync(absolute), `${label} does not exist: ${normalized}`);
  const expectedRealPath = path.join(rootRealPath, ...normalized.split("/"));
  const information = assertNoReparseRedirect(absolute, expectedRealPath, `${label}:${normalized}`);
  check(information.isFile(), `${label} is not a regular file: ${normalized}`);
  return { absolute, contents: fs.readFileSync(absolute) };
}

function enumerateCandidate(candidateRoot) {
  check(fs.existsSync(candidateRoot), `candidate root does not exist: ${candidateRoot}`);
  const rootInformation = fs.lstatSync(candidateRoot);
  check(!rootInformation.isSymbolicLink(), `candidate root is a symlink/reparse point: ${candidateRoot}`);
  check(rootInformation.isDirectory(), `candidate root is not a directory: ${candidateRoot}`);
  const rootRealPath = fs.realpathSync.native(candidateRoot);
  const files = [];

  function visit(directory, relativeDirectory) {
    const children = fs.readdirSync(directory).sort((left, right) => left.localeCompare(right, "en"));
    for (const name of children) {
      const relativePath = relativeDirectory ? `${relativeDirectory}/${name}` : name;
      const absolute = path.join(directory, name);
      const expectedRealPath = path.join(rootRealPath, ...relativePath.split("/"));
      const information = assertNoReparseRedirect(absolute, expectedRealPath, relativePath);
      if (relativePath === "target" && information.isDirectory()) {
        throw new Error("candidate root target/ is forbidden during accounting; remove build outputs first");
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
  return { files, rootRealPath };
}

function isForbiddenDocumentationPath(relativePath) {
  const pieces = relativePath.split("/");
  const basename = pieces.at(-1);
  return (
    pieces[0] === "docs" ||
    pieces[0] === "doc" ||
    /^README(?:\..+)?$/i.test(basename) ||
    /^(?:LICENSE|LICENCE|COPYING|NOTICE)(?:\..+)?$/i.test(basename)
  );
}

function assertAllowedEntryProperties(entry, allowed, relativePath) {
  for (const key of Object.keys(entry)) {
    check(allowed.has(key), `${entry.role} entry ${relativePath} may not contain property: ${key}`);
  }
}

function assertExactFields(entry, expected, relativePath) {
  for (const [key, value] of Object.entries(expected)) {
    check(entry[key] === value, `${relativePath} must set ${key} to ${JSON.stringify(value)}`);
  }
}

function canonicalManifestFields(canonical) {
  return {
    category: canonical.category,
    reason: canonical.reason,
    generator: canonical.generator,
    source: canonical.source,
    sha256: canonical.sha256,
  };
}

function validateManifestShape(manifest, expectedCandidate) {
  check(manifest && typeof manifest === "object" && !Array.isArray(manifest), "manifest must be an object");
  for (const key of Object.keys(manifest)) {
    check(TOP_LEVEL_PROPERTIES.has(key), `unknown manifest property: ${key}`);
  }
  check(manifest.schema_version === 3, "schema_version must be 3");
  check(manifest.candidate === expectedCandidate, `manifest candidate must be ${expectedCandidate}`);
  check(Array.isArray(manifest.entries) && manifest.entries.length > 0, "entries must be a non-empty array");
}

function parseAndValidateEntries(manifest, enumeratedFiles) {
  const seen = new Set();
  const entries = new Map();
  const canonicalIds = new Set();

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
    check(
      !isForbiddenDocumentationPath(relativePath),
      `documentation is forbidden inside a measured candidate root; move it outside: ${relativePath}`,
    );

    if (entry.role === "production") {
      assertAllowedEntryProperties(entry, BASE_ENTRY_PROPERTIES, relativePath);
    } else if (entry.role === "test") {
      assertAllowedEntryProperties(entry, BASE_ENTRY_PROPERTIES, relativePath);
      check(TEST_PATH_PATTERN.test(relativePath), `test role is allowed only for Rust source under top-level tests/: ${relativePath}`);
    } else if (entry.role === "accounting") {
      assertAllowedEntryProperties(entry, BASE_ENTRY_PROPERTIES, relativePath);
      assertExactFields(entry, ACCOUNTING_METADATA, relativePath);
    } else if (entry.role === "lockfile") {
      assertAllowedEntryProperties(entry, new Set([...BASE_ENTRY_PROPERTIES, ...PROVENANCE_PROPERTIES]), relativePath);
      assertExactFields(entry, LOCKFILE_METADATA, relativePath);
    } else if (entry.role === "generated") {
      check(GENERATED_KINDS.has(entry.generated_kind), `invalid generated_kind for ${relativePath}: ${entry.generated_kind}`);
      check(SHA256_PATTERN.test(entry.sha256), `generated sha256 must be lowercase hexadecimal for ${relativePath}`);
      check(nonEmptyString(entry.generator), `generator is required for generated file ${relativePath}`);
      check(nonEmptyString(entry.source), `source is required for generated file ${relativePath}`);
      check(nonEmptyString(entry.reason), `reason is required for generated file ${relativePath}`);
      if (entry.generated_kind === "canonical-copy") {
        assertAllowedEntryProperties(entry, new Set([...GENERATED_PROPERTIES, "canonical_id"]), relativePath);
        check(nonEmptyString(entry.canonical_id), `canonical_id is required for ${relativePath}`);
        const canonical = CANONICAL_EXCLUSIONS.get(entry.canonical_id);
        check(canonical !== undefined, `canonical_id is not frozen/allowlisted for ${relativePath}: ${entry.canonical_id}`);
        check(!canonicalIds.has(entry.canonical_id), `canonical_id may appear only once per candidate: ${entry.canonical_id}`);
        canonicalIds.add(entry.canonical_id);
        check(relativePath === canonical.destination, `canonical copy must be at ${canonical.destination}: ${relativePath}`);
        assertExactFields(entry, canonicalManifestFields(canonical), relativePath);
      } else {
        assertAllowedEntryProperties(entry, GENERATED_PROPERTIES, relativePath);
        assertExactFields(entry, BINARY_GUEST_METADATA, relativePath);
        check(BINARY_GUEST_PATH_PATTERN.test(relativePath), `binary guest must be a .wasm under guest-artifacts/: ${relativePath}`);
      }
    }

    if (relativePath === "sloc-manifest.json") {
      check(entry.role === "accounting", "sloc-manifest.json must use the accounting role");
    } else {
      check(entry.role !== "accounting", `accounting role is reserved for sloc-manifest.json: ${relativePath}`);
    }
    if (path.posix.basename(relativePath) === "Cargo.lock") {
      check(entry.role === "lockfile", `Cargo.lock must use the lockfile role: ${relativePath}`);
      check(relativePath === "Cargo.lock", `only the root Cargo.lock is allowed: ${relativePath}`);
    } else {
      check(entry.role !== "lockfile", `lockfile role is reserved for root Cargo.lock: ${relativePath}`);
    }
    if (
      path.posix.basename(relativePath) === "Cargo.toml" ||
      path.posix.basename(relativePath) === "build.rs" ||
      [".wat", ".wit", ".toml", ".json", ".jsonl", ".ndjson", ".yaml", ".yml"].includes(
        path.posix.extname(relativePath).toLowerCase(),
      )
    ) {
      if (relativePath !== "sloc-manifest.json") {
        check(entry.role === "production", `build/runtime/config/WAT input must use production role: ${relativePath}`);
      }
    }
    entries.set(relativePath, entry);
  }

  for (const id of CANONICAL_EXCLUSIONS.keys()) {
    check(canonicalIds.has(id), `candidate must include the frozen canonical copy: ${id}`);
  }
  check(seen.has("sloc-manifest.json"), "sloc-manifest.json must attribute itself");
  const actual = new Set(enumeratedFiles);
  const missing = enumeratedFiles.filter((relativePath) => !seen.has(relativePath));
  const extra = [...seen].filter((relativePath) => !actual.has(relativePath)).sort();
  check(missing.length === 0, `unattributed regular files: ${JSON.stringify(missing)}`);
  check(extra.length === 0, `manifest entries do not name enumerated regular files: ${JSON.stringify(extra)}`);
  return entries;
}

function verifyCanonicalRegistry() {
  const verified = new Map();
  for (const [id, canonical] of CANONICAL_EXCLUSIONS) {
    const source = assertExternalRegularFile(
      EXPERIMENT_ROOT,
      EXPERIMENT_REAL_ROOT,
      canonical.source,
      `canonical source for ${id}`,
    );
    check(!source.contents.includes(0x0d), `canonical source for ${id} must contain LF-only bytes`);
    const sourceSha256 = sha256(source.contents);
    check(
      sourceSha256 === canonical.sha256,
      `frozen canonical source hash drift for ${id}: expected ${canonical.sha256}, got ${sourceSha256}`,
    );
    verified.set(id, { ...canonical, contents: source.contents });
  }
  return verified;
}

function validateBinaryGuestSource(entry, relativePath, entries) {
  if (entry.source.startsWith("candidate:")) {
    const sourcePath = normalizedRelativePath(entry.source.slice("candidate:".length), `source for ${relativePath}`);
    check(sourcePath !== relativePath, `binary guest may not cite itself as source: ${relativePath}`);
    const sourceEntry = entries.get(sourcePath);
    check(sourceEntry !== undefined, `binary guest source is not attributed: ${sourcePath}`);
    check(
      sourceEntry.role === "production" || sourceEntry.role === "test",
      `binary guest source must be counted production/test source: ${sourcePath}`,
    );
    check(
      [".rs", ".wat", ".wit", ".c", ".cc", ".cpp", ".h"].includes(path.posix.extname(sourcePath).toLowerCase()),
      `binary guest candidate source has an unsupported extension: ${sourcePath}`,
    );
    return { kind: "candidate-attributed-source", path: sourcePath };
  }
  if (entry.source.startsWith("experiment:")) {
    const sourcePath = normalizedRelativePath(entry.source.slice("experiment:".length), `source for ${relativePath}`);
    check(sourcePath.startsWith("guests/core/"), `binary guest experiment source must be under guests/core/: ${sourcePath}`);
    const guestRelativePath = sourcePath.slice("guests/core/".length);
    const source = assertExternalRegularFile(GUESTS_ROOT, GUESTS_ROOT, guestRelativePath, `source for ${relativePath}`);
    return { kind: "frozen-experiment-guest-source", path: sourcePath, sha256: sha256(source.contents) };
  }
  throw new Error(`binary guest source must start with candidate: or experiment: for ${relativePath}`);
}

function validateGeneratedFile(relativePath, entry, contents, entries, verifiedCanonical) {
  const actualSha256 = sha256(contents);
  check(actualSha256 === entry.sha256, `generated content hash mismatch for ${relativePath}: expected ${entry.sha256}, got ${actualSha256}`);
  if (entry.generated_kind === "canonical-copy") {
    const canonical = verifiedCanonical.get(entry.canonical_id);
    check(canonical !== undefined, `canonical source was not verified for ${relativePath}`);
    check(contents.equals(canonical.contents), `canonical copy is not byte-for-byte identical to ${canonical.source}: ${relativePath}`);
    return {
      kind: "canonical-copy",
      canonical_id: entry.canonical_id,
      source: canonical.source,
      sha256: canonical.sha256,
    };
  }
  check(contents.length >= WASM_CORE_HEADER.length, `binary guest is shorter than a WebAssembly header: ${relativePath}`);
  check(
    contents.subarray(0, WASM_CORE_HEADER.length).equals(WASM_CORE_HEADER),
    `binary guest does not have the frozen core WebAssembly magic/version header: ${relativePath}`,
  );
  const source = validateBinaryGuestSource(entry, relativePath, entries);
  return { kind: "binary-guest", sha256: actualSha256, source };
}

function validateProductionRustTopology(candidateRoot, entries) {
  for (const [relativePath, entry] of entries) {
    if (entry.role !== "production" || path.posix.extname(relativePath) !== ".rs") continue;
    const contents = fs.readFileSync(path.join(candidateRoot, ...relativePath.split("/")));
    const text = decodeUtf8(contents, relativePath);
    check(
      !INCLUDE_ESCAPE_PATTERN.test(text),
      `production Rust may not use #[path] or include/include_str/include_bytes escapes: ${relativePath}`,
    );
  }
}

let verifiedRustfmtVersion = null;
function validateRustFormatting(candidateRoot, entries) {
  if (verifiedRustfmtVersion === null) {
    const version = childProcess.spawnSync("rustfmt", ["--version"], {
      cwd: candidateRoot,
      encoding: "utf8",
      windowsHide: true,
    });
    if (version.error) throw new Error(`rustfmt --version could not run: ${version.error.message}`);
    check(version.status === 0, `rustfmt --version failed: ${(version.stderr || version.stdout).trim()}`);
    check(
      version.stdout.trim() === EXPECTED_RUSTFMT_VERSION,
      `rustfmt version drift: expected ${EXPECTED_RUSTFMT_VERSION}, got ${version.stdout.trim()}`,
    );
    verifiedRustfmtVersion = version.stdout.trim();
  }

  const checked = [];
  for (const [relativePath, entry] of entries) {
    if (path.posix.extname(relativePath) !== ".rs") continue;
    if (!["production", "test", "generated"].includes(entry.role)) continue;
    const absolute = path.join(candidateRoot, ...relativePath.split("/"));
    const result = childProcess.spawnSync("rustfmt", ["--check", "--edition", "2018", absolute], {
      cwd: candidateRoot,
      encoding: "utf8",
      maxBuffer: 16 * 1024 * 1024,
      windowsHide: true,
    });
    if (result.error) throw new Error(`rustfmt --check could not run for ${relativePath}: ${result.error.message}`);
    check(
      result.status === 0,
      `rustfmt --check rejected ${relativePath}: ${(result.stderr || result.stdout).trim()}`,
    );
    checked.push(relativePath);
  }
  return { version: verifiedRustfmtVersion, edition: "2018", checked_files: checked };
}

function classifyExternalPath(candidate, candidateRoot, resolvedPath) {
  if (isWithin(candidateRoot, resolvedPath)) return "candidate-internal";
  if (isWithin(ORACLE_ROOT, resolvedPath)) throw new Error(`oracle path dependency is forbidden: ${resolvedPath}`);
  if (candidate !== "lunatic") throw new Error(`${candidate} path dependency escapes candidate root: ${resolvedPath}`);
  if (samePath(resolvedPath, REPOSITORY_ROOT) || isWithin(REPOSITORY_SRC, resolvedPath)) {
    return "external-existing-runtime:repo-src";
  }
  if (isWithin(REPOSITORY_CRATES, resolvedPath)) return "external-existing-runtime:repo-crates";
  throw new Error(`lunatic path dependency is not an allowed product src/crates path: ${resolvedPath}`);
}

function cratesIoSource(source) {
  return source === CRATES_IO_SOURCE;
}

function sortedFeatures(dependency) {
  check(Array.isArray(dependency.features), `dependency ${dependency.name} has no Cargo feature array`);
  for (const feature of dependency.features) {
    check(nonEmptyString(feature), `dependency ${dependency.name} has an invalid Cargo feature`);
  }
  return [...dependency.features].sort();
}

function cargoCrateName(packageName) {
  return packageName.replaceAll("-", "_");
}

function assertExactDependencyShape(dependency, specification) {
  const label = `direct dependency ${dependency.name}`;
  check((dependency.rename ?? null) === null, `${label} may not rename its crate`);
  check(dependency.kind === null, `${label} must be a normal dependency; build/dev dependencies are forbidden`);
  check((dependency.target ?? null) === null, `${label} must be target-independent`);
  check(dependency.optional === false, `${label} must set optional = false`);
  check(
    dependency.uses_default_features === specification.default_features,
    `${label} default-features must be ${specification.default_features}`,
  );
  check(
    JSON.stringify(sortedFeatures(dependency)) === JSON.stringify(specification.features),
    `${label} features must be exactly [${specification.features.join(", ")}]`,
  );
  check(
    dependency.req === specification.requirement,
    `${label} must be pinned as '${specification.requirement}', got ${dependency.req}`,
  );
  check((dependency.registry ?? null) === null, `${label} may not select a named Cargo registry`);
}

function resolvedDirectPackage(candidatePackage, metadata, dependency) {
  check(metadata.resolve && Array.isArray(metadata.resolve.nodes), "cargo metadata JSON has no resolve graph");
  const rootNode = metadata.resolve.nodes.find((node) => node.id === candidatePackage.id);
  check(rootNode !== undefined, `cargo resolve graph has no node for candidate package ${candidatePackage.id}`);
  // Cargo metadata exposes `resolve.nodes[].deps[].name` as the Rust crate
  // identifier, so an unrenamed package such as `lunatic-process` appears as
  // `lunatic_process`. Package declarations and resolved package names retain
  // the hyphenated package spelling.
  const expectedEdgeName = cargoCrateName(dependency.name);
  const edges = (rootNode.deps ?? []).filter((edge) => edge.name === expectedEdgeName);
  check(edges.length === 1, `direct dependency ${dependency.name} must resolve through exactly one root edge`);
  const edgeKinds = edges[0].dep_kinds ?? [];
  check(
    edgeKinds.length === 1 && edgeKinds[0].kind === null && edgeKinds[0].target === null,
    `direct dependency ${dependency.name} resolve edge must be normal and target-independent`,
  );
  const resolved = metadata.packages.find((pkg) => pkg.id === edges[0].pkg);
  check(resolved !== undefined, `resolved package is missing for direct dependency ${dependency.name}`);
  return resolved;
}

export function validateFrozenDirectDependencies(
  candidate,
  candidatePackage,
  metadata,
  candidateRoot,
  options = {},
) {
  const policy = FROZEN_DIRECT_DEPENDENCY_POLICY[candidate];
  check(policy !== undefined, `no frozen dependency policy for candidate ${candidate}`);
  const declarations = candidatePackage.dependencies ?? [];
  const seen = new Set();
  const verified = [];

  for (const dependency of declarations) {
    check(!seen.has(dependency.name), `direct dependency ${dependency.name} is declared more than once`);
    seen.add(dependency.name);

    if (dependency.path !== null && dependency.path !== undefined) {
      check(fs.existsSync(dependency.path), `direct path dependency does not exist: ${dependency.path}`);
      const actualPath = fs.realpathSync.native(dependency.path);
      check(
        !isWithin(candidateRoot, actualPath),
        `candidate-internal helper package is forbidden: ${dependency.name} at ${actualPath}`,
      );
      const specification = policy.product_paths[dependency.name];
      check(
        specification !== undefined,
        `direct path dependency ${dependency.name} is not in the frozen ${candidate} product allowlist`,
      );
      check(candidate === "lunatic", `only the lunatic candidate may use a product path dependency`);
      assertExactDependencyShape(dependency, specification);
      check(dependency.source === null, `direct path dependency ${dependency.name} must have a null source`);
      const expectedPath = fs.realpathSync.native(
        path.join(REPOSITORY_ROOT, ...specification.product_path.split("/")),
      );
      check(
        samePath(actualPath, expectedPath),
        `direct path dependency ${dependency.name} must resolve to ${specification.product_path}`,
      );
      const resolved = resolvedDirectPackage(candidatePackage, metadata, dependency);
      check(resolved.name === dependency.name, `resolved path package name must be ${dependency.name}`);
      check(
        resolved.version === specification.version,
        `resolved ${dependency.name} version must be ${specification.version}, got ${resolved.version}`,
      );
      check(resolved.source === null, `resolved path package ${dependency.name} must have a null source`);
      check(
        samePath(fs.realpathSync.native(path.dirname(resolved.manifest_path)), expectedPath),
        `resolved path package ${dependency.name} escaped its frozen product path`,
      );
      verified.push({
        name: dependency.name,
        kind: "product-path",
        product_path: specification.product_path,
        version: specification.version,
        default_features: specification.default_features,
        features: specification.features,
      });
      continue;
    }

    const specification = policy.registry[dependency.name];
    check(
      specification !== undefined,
      `direct registry dependency ${dependency.name} is not in the frozen ${candidate} allowlist`,
    );
    assertExactDependencyShape(dependency, specification);
    check(
      cratesIoSource(dependency.source),
      `direct dependency ${dependency.name} must come from crates.io, got ${dependency.source}`,
    );
    const resolved = resolvedDirectPackage(candidatePackage, metadata, dependency);
    check(resolved.name === dependency.name, `resolved registry package name must be ${dependency.name}`);
    check(
      resolved.version === specification.version,
      `resolved ${dependency.name} version must be ${specification.version}, got ${resolved.version}`,
    );
    check(
      cratesIoSource(resolved.source),
      `resolved package ${dependency.name} must come from crates.io, got ${resolved.source}`,
    );
    verified.push({
      name: dependency.name,
      kind: "registry",
      source: CRATES_IO_SOURCE,
      version: specification.version,
      default_features: specification.default_features,
      features: specification.features,
    });
  }

  for (const required of policy.required_registry) {
    check(
      seen.has(required),
      `${candidate} candidate must declare frozen normal dependency ${required} = '${policy.registry[required].requirement}'`,
    );
  }
  if (!(candidate === "lunatic" && options.allow_missing_product_path === true)) {
    for (const required of policy.required_product_paths) {
      check(
        seen.has(required),
        `lunatic candidate must declare frozen product path dependency ${required}`,
      );
    }
  }

  const serializedPolicy = JSON.stringify(policy);
  return {
    policy: "embedded-v3-direct-dependencies-v1",
    policy_sha256: sha256(Buffer.from(serializedPolicy, "utf8")),
    declarations: verified.sort((left, right) => left.name.localeCompare(right.name, "en")),
  };
}

let lunaticProductBinding = null;
function verifyLunaticProductBinding() {
  if (lunaticProductBinding !== null) return lunaticProductBinding;
  const verifyCommit = childProcess.spawnSync("git", ["cat-file", "-e", `${PINNED_LUNATIC_PRODUCT_COMMIT}^{commit}`], {
    cwd: REPOSITORY_ROOT,
    encoding: "utf8",
    windowsHide: true,
  });
  if (verifyCommit.error) throw new Error(`git product pin verification could not run: ${verifyCommit.error.message}`);
  check(verifyCommit.status === 0, `pinned Lunatic product commit is unavailable: ${PINNED_LUNATIC_PRODUCT_COMMIT}`);
  const diff = childProcess.spawnSync(
    "git",
    ["diff", "--quiet", PINNED_LUNATIC_PRODUCT_COMMIT, "--", ...LUNATIC_PRODUCT_PATHS],
    { cwd: REPOSITORY_ROOT, encoding: "utf8", windowsHide: true },
  );
  check(diff.status === 0, `Lunatic product source differs from pinned commit ${PINNED_LUNATIC_PRODUCT_COMMIT}`);
  const status = childProcess.spawnSync(
    "git",
    ["status", "--porcelain=v1", "--untracked-files=all", "--", ...LUNATIC_PRODUCT_PATHS],
    { cwd: REPOSITORY_ROOT, encoding: "utf8", windowsHide: true },
  );
  if (status.error) throw new Error(`git product status could not run: ${status.error.message}`);
  check(status.status === 0, `git product status failed: ${status.stderr.trim()}`);
  check(status.stdout.trim() === "", `Lunatic product source has tracked/untracked changes: ${status.stdout.trim()}`);
  const manifest = {
    policy: "embedded-v3-lunatic-product-v1",
    pinned_git_commit: PINNED_LUNATIC_PRODUCT_COMMIT,
    verified_paths: LUNATIC_PRODUCT_PATHS,
  };
  lunaticProductBinding = { ...manifest, binding_sha256: sha256(Buffer.from(JSON.stringify(manifest), "utf8")) };
  return lunaticProductBinding;
}

function candidateRelativePath(candidateRoot, absolutePath, label) {
  const realPath = fs.realpathSync.native(absolutePath);
  check(isWithin(candidateRoot, realPath), `${label} escapes the candidate root: ${realPath}`);
  return path.relative(candidateRoot, realPath).split(path.sep).join("/");
}

function validateCargoTargets(candidateRoot, candidatePackage, entries) {
  check(candidatePackage.edition === "2018", `candidate Cargo package edition must be 2018, got ${candidatePackage.edition}`);
  const targets = [];
  for (const target of candidatePackage.targets ?? []) {
    check(nonEmptyString(target.src_path), `Cargo target ${target.name ?? "<unknown>"} has no src_path`);
    const relativePath = candidateRelativePath(candidateRoot, target.src_path, `Cargo target ${target.name}`);
    const entry = entries.get(relativePath);
    check(entry !== undefined, `Cargo target source is not attributed: ${relativePath}`);
    const kinds = Array.isArray(target.kind) ? target.kind : [];
    const pureTestTarget = kinds.length > 0 && kinds.every((kind) => kind === "test");
    if (pureTestTarget) {
      check(
        entry.role === "test" || entry.role === "production",
        `Cargo test target source must use test/production role: ${relativePath}`,
      );
    } else {
      check(entry.role === "production", `non-test Cargo target source must use production role: ${relativePath}`);
      check(
        relativePath === "build.rs" || relativePath.startsWith("src/"),
        `non-test Cargo target source must be root build.rs or under src/: ${relativePath}`,
      );
    }
    targets.push({ name: target.name, kind: kinds, src_path: relativePath, role: entry.role });
  }
  return targets;
}

function validateCargoMetadata(candidate, candidateRoot, enumeratedFiles, entries, options) {
  const cargoManifests = enumeratedFiles.filter((relativePath) => path.posix.basename(relativePath) === "Cargo.toml");
  const cargoLocks = enumeratedFiles.filter((relativePath) => path.posix.basename(relativePath) === "Cargo.lock");
  check(cargoManifests.length === 1 && cargoManifests[0] === "Cargo.toml", "candidate must contain exactly one root Cargo.toml");
  check(cargoLocks.length === 1 && cargoLocks[0] === "Cargo.lock", "candidate must contain exactly one root Cargo.lock");

  const manifestPath = path.join(candidateRoot, "Cargo.toml");
  const commandArguments = ["metadata", "--format-version", "1", "--locked", "--manifest-path", manifestPath];
  const result = childProcess.spawnSync("cargo", commandArguments, {
    cwd: candidateRoot,
    encoding: "utf8",
    maxBuffer: 64 * 1024 * 1024,
    windowsHide: true,
  });
  if (result.error) throw new Error(`cargo metadata --locked could not run: ${result.error.message}`);
  check(result.status === 0, `cargo metadata --locked failed (exit ${result.status}): ${(result.stderr || result.stdout).trim()}`);
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
      recordPath(dependencyPath, classification, `dependency:${pkg.name}@${pkg.version}->${dependency.name}`);
    }
  }

  check(candidatePackages.length === 1, `candidate must be exactly one root Cargo package, got ${candidatePackages.length}`);
  const candidatePackage = candidatePackages[0];
  check(
    samePath(path.dirname(candidatePackage.manifest_path), candidateRoot),
    `candidate Cargo package must be rooted at ${candidateRoot}`,
  );
  const targets = validateCargoTargets(candidateRoot, candidatePackage, entries);
  const directDependencies = validateFrozenDirectDependencies(
    candidate,
    candidatePackage,
    metadata,
    candidateRoot,
    { allow_missing_product_path: options.selfTest },
  );
  const externalPathDependencies = [...externalPaths.values()]
    .map((item) => ({ ...item, owners: [...item.owners].sort() }))
    .sort((left, right) => left.path.localeCompare(right.path, "en"));
  let lunaticProduct = null;
  if (candidate === "lunatic") {
    lunaticProduct = verifyLunaticProductBinding();
    if (!options.selfTest) {
      check(
        externalPathDependencies.some((item) => item.classification.startsWith("external-existing-runtime:")),
        "lunatic candidate must use at least one allowed pinned Lunatic product path dependency",
      );
    }
  }
  return {
    status: "verified",
    command: "cargo metadata --format-version 1 --locked --manifest-path <candidate>/Cargo.toml",
    package_count: metadata.packages.length,
    candidate_package_count: candidatePackages.length,
    targets,
    extism_version: candidate === "extism" ? "1.30.0" : null,
    wasmtime_version: candidate === "direct-wasmtime" ? "46.0.1" : null,
    direct_dependencies: directDependencies,
    external_path_dependencies: externalPathDependencies,
    lunatic_product_binding: lunaticProduct,
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

export function analyzeCandidate(candidate, candidateRoot, options = {}) {
  check(CANDIDATES.has(candidate), `unsupported candidate: ${candidate}`);
  const analysisOptions = { selfTest: options.selfTest === true };
  const expectedRoot = path.resolve(candidateRoot);
  const verifiedCanonical = verifyCanonicalRegistry();
  const enumeration = enumerateCandidate(expectedRoot);
  check(enumeration.files.includes("sloc-manifest.json"), "candidate root has no sloc-manifest.json");
  const { manifest, contents: manifestContents } = parseManifest(path.join(expectedRoot, "sloc-manifest.json"));
  validateManifestShape(manifest, candidate);
  const entries = parseAndValidateEntries(manifest, enumeration.files);

  const roleTotals = Object.fromEntries([...ROLES].sort().map((role) => [role, { files: 0, bytes: 0, sloc: 0 }]));
  const categoryTotals = {};
  const files = [];
  for (const relativePath of enumeration.files) {
    const entry = entries.get(relativePath);
    const contents = fs.readFileSync(path.join(expectedRoot, ...relativePath.split("/")));
    const exclusion = entry.role === "generated"
      ? validateGeneratedFile(relativePath, entry, contents, entries, verifiedCanonical)
      : null;
    const lines = entry.role === "production" || entry.role === "test"
      ? nonBlankPhysicalLines(contents, relativePath)
      : 0;
    const roleTotal = roleTotals[entry.role];
    roleTotal.files += 1;
    roleTotal.bytes += contents.length;
    roleTotal.sloc += lines;
    const category = categoryTotals[entry.category] ?? {
      production_sloc: 0,
      test_sloc: 0,
      excluded_files: 0,
      files: 0,
    };
    category.files += 1;
    if (entry.role === "production") category.production_sloc += lines;
    if (entry.role === "test") category.test_sloc += lines;
    if (entry.role !== "production" && entry.role !== "test") category.excluded_files += 1;
    categoryTotals[entry.category] = category;
    files.push({
      path: relativePath,
      role: entry.role,
      category: entry.category,
      bytes: contents.length,
      sloc: lines,
      sha256: sha256(contents),
      ...(exclusion === null ? {} : { exclusion }),
    });
  }

  validateProductionRustTopology(expectedRoot, entries);
  const rustfmt = validateRustFormatting(expectedRoot, entries);
  const cargoMetadata = validateCargoMetadata(
    candidate,
    enumeration.rootRealPath,
    enumeration.files,
    entries,
    analysisOptions,
  );
  return {
    schema_version: 3,
    accounting_policy: "embedded-v3-sloc-frozen-v3",
    candidate,
    manifest_sha256: sha256(manifestContents),
    production_sloc: roleTotals.production.sloc,
    test_sloc: roleTotals.test.sloc,
    formatting: {
      ...rustfmt,
      maximum_counted_line_bytes: MAX_COUNTED_LINE_BYTES,
      include_and_path_macros: "forbidden in production Rust",
    },
    enumeration: {
      attributed_regular_files: enumeration.files.length,
      exclusions: [],
      root_target_policy: "forbidden; all present regular files must be attributed",
    },
    frozen_canonical_allowlist: [...verifiedCanonical.entries()].map(([id, item]) => ({
      canonical_id: id,
      destination: item.destination,
      source: item.source,
      sha256: item.sha256,
      line_endings: "LF",
      required_exactly_once: true,
    })),
    role_totals: roleTotals,
    category_totals: Object.fromEntries(
      Object.entries(categoryTotals).sort(([left], [right]) => left.localeCompare(right, "en")),
    ),
    cargo_metadata: cargoMetadata,
    files,
  };
}

export function candidateFromCliManifest(argument) {
  const suppliedPath = path.resolve(process.cwd(), argument);
  for (const candidate of [...CANDIDATES].sort()) {
    const allowedPath = path.join(CANDIDATES_ROOT, candidate, "sloc-manifest.json");
    if (samePath(suppliedPath, allowedPath)) return { candidate, candidateRoot: path.dirname(allowedPath) };
  }
  throw new Error(
    "manifest must be exactly ../candidates/{lunatic,direct-wasmtime,extism}/sloc-manifest.json relative to this tool",
  );
}
