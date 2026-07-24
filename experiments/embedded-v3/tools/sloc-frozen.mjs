#!/usr/bin/env node

// Canonical embedded-v3 SLOC entry point. The core and self-test modules are
// separate so their hashes can be frozen and audited independently.

import process from "node:process";

import { analyzeCandidate, candidateFromCliManifest } from "./sloc-frozen-core.mjs";

function check(condition, message) {
  if (!condition) throw new Error(message);
}

async function main() {
  check(
    process.argv.length === 3,
    "usage: node sloc-frozen.mjs <fixed candidate sloc-manifest.json> | --self-test",
  );
  if (process.argv[2] === "--self-test") {
    const { selfTest } = await import("./sloc-frozen-self-test.mjs");
    console.log(JSON.stringify({ self_test: "ok", ...selfTest() }, null, 2));
    return;
  }
  const { candidate, candidateRoot } = candidateFromCliManifest(process.argv[2]);
  console.log(JSON.stringify(analyzeCandidate(candidate, candidateRoot), null, 2));
}

main().catch((error) => {
  console.error(`sloc-frozen: ${error.message}`);
  process.exitCode = 2;
});
