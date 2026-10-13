#!/usr/bin/env python3
"""Record the exact checkout used by the two native qualification runners."""

import argparse
import base64
import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys


def digest(data):
    return hashlib.sha256(data).hexdigest()


def git(repo, *args):
    return subprocess.check_output(["git", "-C", str(repo), *args])


def snapshot(repo):
    revision = git(repo, "rev-parse", "HEAD").decode().strip()
    status = git(repo, "status", "--porcelain=v1", "-z", "--untracked-files=all")
    files = {}
    for kind, args in (("tracked", ("ls-files", "-z")),
                       ("untracked", ("ls-files", "--others", "--exclude-standard", "-z"))):
        for raw in git(repo, *args).split(b"\0"):
            if not raw:
                continue
            name = os.fsdecode(raw)
            path = repo / name
            if path.is_symlink():
                value = {"kind": kind, "symlink": os.readlink(path)}
            elif path.is_file():
                value = {"kind": kind, "sha256": digest(path.read_bytes()),
                         "executable": bool(path.stat().st_mode & 0o111)}
            elif not path.exists():
                value = {"kind": kind, "missing": True}
            else:
                raise ValueError("unsupported checkout entry")
            files[name] = value
    source = {"revision": revision, "dirty": bool(status),
              "status_porcelain_base64": base64.b64encode(status).decode(),
              "status_porcelain": git(repo, "status", "--porcelain=v1", "--untracked-files=all").decode(),
              "files": files}
    source["snapshot_sha256"] = digest(json.dumps(source, sort_keys=True, separators=(",", ":")).encode())
    return source


def save(path, value, repo=None):
    path = path.resolve()
    if repo is not None and path.is_relative_to(repo):
        raise ValueError("provenance output must be outside the checkout")
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as stream:
        os.chmod(path, 0o600)
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("capture", "check", "artifact"))
    parser.add_argument("--repo", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--expected", type=Path)
    parser.add_argument("--source", type=Path)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--metadata", type=Path)
    args = parser.parse_args()
    if args.mode == "artifact":
        source = json.loads(args.source.read_text())
        if source.get("qualification_decision") != "accepted":
            raise ValueError("artifact source was not accepted")
        result = {"revision": source["revision"], "source_dirty": source["dirty"],
                  "source_snapshot_sha256": source["snapshot_sha256"],
                  "binary": args.binary.name, "binary_sha256": digest(args.binary.read_bytes()),
                  "build_metadata": args.metadata.read_text(),
                  "recorded_at": datetime.datetime.now(datetime.timezone.utc).isoformat()}
        save(args.output, result, args.repo.resolve() if args.repo else None)
        return 0
    repo = args.repo.resolve()
    source = snapshot(repo)
    accepted = True
    if args.mode == "check":
        if os.environ.get("CI") and source["dirty"]:
            accepted = False
        elif args.expected:
            expected = json.loads(args.expected.read_text())
            accepted = source["snapshot_sha256"] == expected.get("snapshot_sha256")
        elif source["dirty"]:
            accepted = False
    source["qualification_decision"] = "captured" if args.mode == "capture" else ("accepted" if accepted else "rejected")
    save(args.output, source, repo)
    if not accepted:
        print("Qualification refused unexpected source changes; retained source manifest identifies the checkout.", file=sys.stderr)
        return 1
    print("Qualification source " + source["snapshot_sha256"] + (" dirty" if source["dirty"] else " clean"))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, subprocess.CalledProcessError):
        print("Qualification provenance failed; no native build is authorized by this check.", file=sys.stderr)
        sys.exit(1)
