#!/usr/bin/env python3
"""Validate the workspace version train and changelog release contract."""

from __future__ import annotations

import argparse
import os
import re
import sys
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_GUIDE = "docs/MIGRATING_0.13_TO_0.14.md"
FIRST_RELEASE_PACKAGES = {"lunatic-otp-patterns": "0.1.0"}
QUARANTINED_PACKAGE = "crates/lunatic-control-submillisecond"


def load_manifest(path: Path) -> dict:
    with path.open("rb") as manifest:
        return tomllib.load(manifest)


def fail(message: str) -> None:
    print(f"release contract error: {message}", file=sys.stderr)
    raise SystemExit(1)


def version_requirement_matches(requirement: str, version: str) -> bool:
    major, minor, _patch = version.split(".", maxsplit=2)
    return requirement in {f"{major}.{minor}", f"^{major}.{minor}", version, f"={version}"}


def validate_versions(root_manifest: dict) -> str:
    release_version = root_manifest["package"]["version"]
    workspace = root_manifest["workspace"]
    dependencies = workspace["dependencies"]

    declared_members = {Path(member).as_posix() for member in workspace["members"]}
    excluded_members = {Path(member).as_posix() for member in workspace.get("exclude", [])}
    discovered_crates = {
        manifest.parent.relative_to(ROOT).as_posix()
        for manifest in (ROOT / "crates").glob("*/Cargo.toml")
    }
    unaccounted_crates = discovered_crates - declared_members - excluded_members
    if unaccounted_crates:
        fail(
            "crate manifests must be explicit workspace members or exclusions: "
            + ", ".join(sorted(unaccounted_crates))
        )
    missing_members = declared_members - discovered_crates
    if missing_members:
        fail("workspace members have no Cargo.toml: " + ", ".join(sorted(missing_members)))

    if QUARANTINED_PACKAGE not in workspace.get("exclude", []):
        fail(f"{QUARANTINED_PACKAGE} must remain explicitly excluded from the release workspace")
    if "lunatic-control-submillisecond" in dependencies:
        fail("the quarantined lunatic-control-submillisecond crate must not be a workspace dependency")

    quarantined = load_manifest(ROOT / QUARANTINED_PACKAGE / "Cargo.toml")
    if quarantined["package"].get("publish") is not False:
        fail("the quarantined lunatic-control-submillisecond crate must remain publish = false")

    for member in workspace["members"]:
        manifest_path = ROOT / member / "Cargo.toml"
        manifest = load_manifest(manifest_path)
        package = manifest["package"]
        package_name = package["name"]
        package_version = package["version"]
        expected = FIRST_RELEASE_PACKAGES.get(package_name, release_version)
        if package_version != expected:
            fail(
                f"{manifest_path.relative_to(ROOT)} has version {package_version}; "
                f"expected {expected}"
            )

        dependency = dependencies.get(package_name)
        if dependency is None:
            fail(f"workspace dependency {package_name!r} is missing")
        requirement = dependency.get("version") if isinstance(dependency, dict) else None
        if requirement is None or not version_requirement_matches(requirement, package_version):
            fail(
                f"workspace dependency {package_name!r} requires {requirement!r}; "
                f"expected the {package_version} release line"
            )

    otp_manifest = load_manifest(ROOT / "crates/lunatic-otp-patterns/Cargo.toml")
    for dependency_name in ("lunatic-distributed", "lunatic-process"):
        dependency = otp_manifest["dependencies"][dependency_name]
        requirement = dependency.get("version") if isinstance(dependency, dict) else None
        if requirement is None or not version_requirement_matches(requirement, release_version):
            fail(
                f"lunatic-otp-patterns dependency {dependency_name!r} must declare "
                f"the {release_version} release line"
            )

    examples_manifest = load_manifest(ROOT / "examples/rust/Cargo.toml")
    if examples_manifest["package"].get("publish") is not False:
        fail("examples/rust is a path-dependent fixture and must remain publish = false")

    try:
        changelog_template = workspace["metadata"]["git-cliff"]["changelog"]["body"]
    except KeyError:
        fail("workspace.metadata.git-cliff.changelog.body is missing")
    if "## v{{ version" not in changelog_template or "## Unreleased" not in changelog_template:
        fail("git-cliff headings must match the release-note parser's vX.Y.Z/Unreleased format")

    return release_version


def validate_changelog(release_version: str, release_tag: str | None) -> None:
    if not (ROOT / MIGRATION_GUIDE).is_file():
        fail(f"migration guide {MIGRATION_GUIDE} is missing")

    changelog = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    if release_tag is None:
        if not re.search(r"(?m)^## Unreleased\s*$", changelog):
            fail("CHANGELOG.md must contain an '## Unreleased' section")
        if f"Target release: v{release_version}." not in changelog:
            fail(f"CHANGELOG.md must identify v{release_version} as the target release")
        if MIGRATION_GUIDE not in changelog:
            fail(f"CHANGELOG.md must link {MIGRATION_GUIDE}")
        return

    expected_tag = f"v{release_version}"
    if release_tag != expected_tag:
        fail(f"tag {release_tag!r} does not match workspace version {expected_tag!r}")

    release = re.search(
        rf"(?ms)^## {re.escape(expected_tag)}\s*$\n\s*"
        rf"Released \d{{4}}-\d{{2}}-\d{{2}}\.\s*$\n"
        rf"(?P<body>.*?)(?=^## |\Z)",
        changelog,
    )
    if release is None:
        fail(
            f"CHANGELOG.md must contain '## {expected_tag}' followed by "
            "'Released YYYY-MM-DD.'"
        )
    if not re.search(r"(?m)^### \S", release.group("body")):
        fail(f"CHANGELOG.md release section for {expected_tag} has no release notes")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--release-tag",
        help="Validate a tag release in addition to the development contract",
    )
    args = parser.parse_args()

    release_tag = args.release_tag
    if release_tag is None and os.environ.get("GITHUB_REF_TYPE") == "tag":
        release_tag = os.environ.get("GITHUB_REF_NAME")

    root_manifest = load_manifest(ROOT / "Cargo.toml")
    release_version = validate_versions(root_manifest)
    validate_changelog(release_version, release_tag)
    print(f"release contract OK for v{release_version}")


if __name__ == "__main__":
    main()
