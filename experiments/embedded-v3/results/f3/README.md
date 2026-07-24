# Embedded v3 Frozen Result Bundle

This directory is the compact, post-execution bundle for the decision-bearing
f3 run completed on 2026-07-24 UTC. The canonical interpretation is
[`docs/comparisons/EMBEDDED_V3_RESULTS.md`](../../../../docs/comparisons/EMBEDDED_V3_RESULTS.md).

The official frozen verdict is:

- verdict: `LUNATIC_PACKAGING_VALUE_SUPPORTED`;
- packaging strength: `moderate`;
- eligibility: Lunatic `true`, Extism `false`, direct Wasmtime `true`.

## Contents

- `b00-*.json` through `b09-*.json`: the 30 frozen run configurations;
- `freeze-manifest.json`: source, executable, scenario, topology, and
  exclusion identities;
- `config-hashes.json`: the 30 config hashes plus freeze-manifest hash;
- `execution-log.ndjson`: one exit-zero record for every run;
- `final-evidence-index.json`: post-hoc index of all 30 raw summaries,
  manifests, retained summaries, and recorded trace identities;
- `verdict-input.json`: exact 27-retained-run input to the frozen CLI;
- `verdict-output.json`: exact frozen CLI result.

Raw traces are intentionally not duplicated here. On the execution machine they
remain under `C:/tmp/e3f3-e`. Candidate and Oracle source snapshots and release
executables remain under `C:/tmp/embedded-v3-freeze` and
`C:/tmp/embedded-v3-oracle-target-f3`, with their hashes bound in every run
manifest and in `freeze-manifest.json`.

## Seal Values

- config bundle:
  `23eac1fd06a241082b136bcc6f36f68e04a0aad4712ca46a12c047e157f8c88f`;
- execution log:
  `69fe7e918510abae96620101a139d9f60c21412a72aa95140dd47e1626a7e7f9`;
- final evidence index:
  `921b4f6941099cbc16df96deacbbe3d41adea37443bd33aebf1fea743ddbe204`;
- verdict input:
  `042906ce2e3525fad2e577d3eadc2185892d530561e80a79b61ad288952b3dc1`;
- verdict output:
  `1ebb424a0adb2cec78311b5f603f4e35b2d8ab16b7c73980e6645c4c3addb794`;
- 30 raw-summary ledger:
  `3c59deb5d22303673f87247033dc58ab2780c4475c0c2e7ab9a2c10483fa4b60`;
- 30 run-manifest ledger:
  `6248db05960a5675e1ca612f0a20c2aab7c48e0c9b75bb9b7f89ec683797bd1f`;
- 30 retained-run ledger:
  `4396c2e2e4fb760c551052177112a08735888ca04d5a97ddbc9710073d105cfc`.

`final-evidence-index.json` is a post-hoc, unsigned convenience ledger. An
external publication should pin the index hash and the verdict input/output
hashes together. The per-run immutable manifests and raw-summary evidence remain
the primary artifact bindings.
