#!/usr/bin/env python3
"""Deterministic physical SLOC accounting for embedded-v2 candidates.

Usage:
    python count_sloc.py path/to/sloc-manifest.json
    python count_sloc.py --self-test

The manifest is deliberately explicit: every supported source/configuration file
under the candidate root must be attributed.  That makes moving correctness
logic into an unusual directory visible instead of silently excluding it.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
SUPPORTED_SUFFIXES = {".rs", ".toml", ".wat", ".wit", ".json", ".yaml", ".yml"}
IGNORED_DIRECTORIES = {".git", "target"}
COUNTED_ROLES = {"production"}
VALID_ROLES = COUNTED_ROLES | {"test", "generated", "lockfile"}


@dataclass
class CommentState:
    block_depth: int = 0


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def style_for(path: Path) -> str:
    if path.suffix == ".rs":
        return "rust"
    if path.suffix in {".wat", ".wit"}:
        return "wasm-text"
    if path.suffix in {".toml", ".yaml", ".yml"}:
        return "hash"
    if path.suffix == ".json":
        return "json"
    raise ValueError(f"unsupported source type: {path}")


def strip_comments(line: str, style: str, state: CommentState) -> str:
    if style == "json":
        return line

    if style == "rust":
        line_comment, block_open, block_close = "//", "/*", "*/"
    elif style == "wasm-text":
        line_comment, block_open, block_close = ";;", "(;", ";)"
    elif style == "hash":
        line_comment, block_open, block_close = "#", None, None
    else:
        raise ValueError(f"unknown comment style: {style}")

    output: list[str] = []
    index = 0
    quote: str | None = None
    escaped = False

    while index < len(line):
        if state.block_depth:
            if block_open and line.startswith(block_open, index):
                state.block_depth += 1
                index += len(block_open)
            elif block_close and line.startswith(block_close, index):
                state.block_depth -= 1
                index += len(block_close)
            else:
                index += 1
            continue

        character = line[index]
        if quote is not None:
            output.append(character)
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == quote:
                quote = None
            index += 1
            continue

        if character == '"' or (style == "hash" and character == "'"):
            quote = character
            output.append(character)
            index += 1
            continue

        if line.startswith(line_comment, index):
            break
        if block_open and line.startswith(block_open, index):
            state.block_depth += 1
            index += len(block_open)
            continue

        output.append(character)
        index += 1

    return "".join(output)


def count_file(path: Path) -> int:
    style = style_for(path)
    state = CommentState()
    count = 0
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            if strip_comments(raw_line, style, state).strip():
                count += 1
    if state.block_depth:
        raise ValueError(f"unterminated block comment: {path}")
    return count


def supported_files(root: Path) -> set[str]:
    discovered: set[str] = set()
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        relative = path.relative_to(root)
        if any(part in IGNORED_DIRECTORIES for part in relative.parts):
            continue
        if path.suffix in SUPPORTED_SUFFIXES or path.name == "Cargo.lock":
            discovered.add(relative.as_posix())
    return discovered


def load_manifest(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError("manifest root must be an object")
    return value


def analyze(manifest_path: Path) -> dict[str, Any]:
    manifest_path = manifest_path.resolve()
    manifest = load_manifest(manifest_path)
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(f"schema_version must be {SCHEMA_VERSION}")
    candidate = manifest.get("candidate")
    if not isinstance(candidate, str) or not candidate:
        raise ValueError("candidate must be a non-empty string")
    root_value = manifest.get("root")
    if not isinstance(root_value, str) or not root_value:
        raise ValueError("root must be a non-empty string")
    root = (manifest_path.parent / root_value).resolve()
    if not root.is_dir():
        raise ValueError(f"candidate root is not a directory: {root}")

    raw_entries = manifest.get("entries")
    if not isinstance(raw_entries, list):
        raise ValueError("entries must be an array")

    attributed: set[str] = set()
    rows: list[dict[str, Any]] = []
    category_totals: dict[str, int] = {}
    role_totals: dict[str, int] = {role: 0 for role in sorted(VALID_ROLES)}

    for entry in raw_entries:
        if not isinstance(entry, dict):
            raise ValueError("each entry must be an object")
        relative_value = entry.get("path")
        role = entry.get("role")
        category = entry.get("category")
        if not isinstance(relative_value, str) or not relative_value:
            raise ValueError("entry.path must be a non-empty string")
        relative = Path(relative_value).as_posix()
        if relative in attributed:
            raise ValueError(f"duplicate entry: {relative}")
        attributed.add(relative)
        if role not in VALID_ROLES:
            raise ValueError(f"invalid role for {relative}: {role!r}")
        if not isinstance(category, str) or not category:
            raise ValueError(f"entry.category must be non-empty for {relative}")
        if role in {"generated", "lockfile"} and not entry.get("reason"):
            raise ValueError(f"{role} entry requires a reason: {relative}")

        source = (root / relative).resolve()
        try:
            source.relative_to(root)
        except ValueError as error:
            raise ValueError(f"entry escapes candidate root: {relative}") from error
        if not source.is_file():
            raise ValueError(f"attributed file does not exist: {relative}")

        lines = 0
        if role not in {"generated", "lockfile"}:
            lines = count_file(source)
        role_totals[role] += lines
        if role in COUNTED_ROLES:
            category_totals[category] = category_totals.get(category, 0) + lines
        rows.append(
            {
                "path": relative,
                "role": role,
                "category": category,
                "sloc": lines,
                "sha256": sha256(source),
            }
        )

    discovered = supported_files(root)
    missing = sorted(discovered - attributed)
    extra = sorted(attributed - discovered)
    if missing:
        raise ValueError(f"unattributed supported files: {missing}")
    if extra:
        raise ValueError(f"entries are not supported source/config files: {extra}")

    rows.sort(key=lambda row: row["path"])
    return {
        "schema_version": SCHEMA_VERSION,
        "candidate": candidate,
        "manifest": manifest_path.as_posix(),
        "manifest_sha256": sha256(manifest_path),
        "counted_roles": sorted(COUNTED_ROLES),
        "production_sloc": role_totals["production"],
        "role_totals": role_totals,
        "category_totals": dict(sorted(category_totals.items())),
        "files": rows,
    }


def self_test() -> None:
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        (root / "src").mkdir()
        (root / "src" / "main.rs").write_text(
            "// comment\nfn main() { /* inline */ }\n/* block\ncomment */\nlet x = \"//\";\n",
            encoding="utf-8",
        )
        (root / "Cargo.toml").write_text(
            "# comment\n[package]\nname = \"fixture#name\"\n",
            encoding="utf-8",
        )
        manifest = {
            "schema_version": 1,
            "candidate": "self-test",
            "root": ".",
            "entries": [
                {"path": "Cargo.toml", "role": "production", "category": "configuration"},
                {"path": "src/main.rs", "role": "production", "category": "host"},
            ],
        }
        manifest_path = root / "sloc-manifest.json"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        result = analyze(manifest_path)
        if result["production_sloc"] != 5:
            raise AssertionError(f"expected 5 SLOC, got {result['production_sloc']}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", nargs="?", type=Path)
    parser.add_argument("--self-test", action="store_true")
    arguments = parser.parse_args()
    try:
        if arguments.self_test:
            self_test()
            print("self-test: ok")
            return 0
        if arguments.manifest is None:
            parser.error("manifest is required unless --self-test is used")
        print(json.dumps(analyze(arguments.manifest), indent=2, sort_keys=True))
        return 0
    except (OSError, ValueError, json.JSONDecodeError, AssertionError) as error:
        print(f"count_sloc: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
