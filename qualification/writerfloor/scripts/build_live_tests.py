#!/usr/bin/env python3
"""Build Linux qualification tests and bind their published graph to their bytes."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess


def module_graph(raw):
    decoder = json.JSONDecoder()
    modules = []
    while raw.strip():
        raw = raw.lstrip()
        value, end = decoder.raw_decode(raw)
        if value.get("Replace") or value.get("Error"):
            raise ValueError("qualification requires resolved published modules")
        modules.append(value)
        raw = raw[end:]
    if not modules or len(modules) > 1024:
        raise ValueError("module graph is outside the qualification bound")
    return modules


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="new private output directory")
    parser.add_argument("--arch", choices=("arm64", "amd64"), default="arm64")
    parser.add_argument("--race", action="store_true", help="requires a Linux C compiler for the selected architecture")
    args = parser.parse_args()
    output = args.output.absolute()
    output.mkdir(mode=0o700)
    module = Path(__file__).resolve().parents[1]
    env = dict(os.environ, GOWORK="off", GOTOOLCHAIN="go1.26.9", GOOS="linux",
               GOARCH=args.arch, CGO_ENABLED="1" if args.race else "0",
               GOMAXPROCS="2", GOFLAGS="-p=1")
    binary = output / "managedhost.test"
    command = ["go", "test", "-c", "-o", str(binary)]
    if args.race:
        command.append("-race")
    subprocess.run(command + ["./internal/managedhost"], cwd=module, env=env, check=True)
    raw = subprocess.check_output(["go", "list", "-m", "-json", "all"], cwd=module, env=env, text=True)
    hasher = hashlib.sha256()
    with binary.open("rb") as source:
        for block in iter(lambda: source.read(1 << 20), b""):
            hasher.update(block)
    digest = hasher.hexdigest()
    provenance = {"schema": "writerfloor.test-build-provenance.v1",
                  "executable_sha256": digest, "modules": module_graph(raw)}
    encoded = json.dumps(provenance, separators=(",", ":")).encode()
    if len(encoded) > 1 << 20:
        raise ValueError("build provenance exceeds one MiB")
    path = output / "build.json"
    with path.open("xb") as target:
        target.write(encoded)
    path.chmod(0o600)
    binary.chmod(0o700)
    print(f"Built {binary}; SHA256 {digest}")
    print("Install build.json as root, mode 0600, inside a root-owned 0700 directory.")


if __name__ == "__main__":
    main()
