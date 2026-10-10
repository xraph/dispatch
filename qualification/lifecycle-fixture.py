#!/usr/bin/env python3
"""Create and resume real deployment deferrals through the secured local host."""
import argparse
import json
import os
from pathlib import Path
import time
import urllib.error
import urllib.parse
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "resume", "inspect"))
    parser.add_argument("--state-file", type=Path, required=True)
    parser.add_argument("--activation-file", type=Path)
    args = parser.parse_args()
    until = time.monotonic() + 15
    while True:
        try:
            state = json.loads(args.state_file.read_text())
            break
        except (FileNotFoundError, json.JSONDecodeError):
            if time.monotonic() >= until:
                raise RuntimeError("host state did not become available") from None
            time.sleep(0.05)
    url = state["url"]
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme != "http" or parsed.hostname not in ("127.0.0.1", "::1"):
        raise ValueError("fixture requires numeric loopback HTTP")
    token = state["credentials"]["commander"]["token"]
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def request(path, body=None):
        req = urllib.request.Request(url + path, data=body, headers={
            "Authorization": "Bearer " + token, "Content-Type": "application/json"})
        try:
            with opener.open(req, timeout=10) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            raise RuntimeError("fixture request refused: HTTP " + str(error.code)) from None

    csrf = request("/api/dashboard/v1/csrf")["token"]

    def call(intent, payload, query=False):
        envelope = {"envelope": "v1", "kind": "query" if query else "command",
                    "contributor": "dispatch", "intent": intent, "intentVersion": 1,
                    "idempotencyKey": "fixture-" + intent, "csrf": csrf, "payload": payload}
        result = request("/api/dashboard/v1", json.dumps(envelope).encode())
        if not result.get("ok"):
            raise RuntimeError("fixture command was not accepted")
        return result["data"]

    build = {"namespace": "production", "build_id": "operator-v2"}
    kinds = ("deferred-child", "deferred-continue")
    if args.action == "prepare":
        if args.activation_file is None:
            parser.error("prepare requires --activation-file used by the host")
        call("durable.retirementEnroll", {"namespace": "production", "request_id": "fixture-enroll"})
        for identity in state["runtimes"]:
            target = {"namespace": "production", "build_id": identity["BuildID"]}
            call("durable.buildRegister", target | {"request_id": "fixture-build-" + identity["BuildID"], "expected_version": "0"})
            call("durable.queryRuntimeRegister", target | {"runtime_id": identity["RuntimeID"], "request_id": "fixture-runtime-" + identity["RuntimeID"], "expected_version": "0"})
        call("durable.buildRetire", build | {"request_id": "fixture-retire", "expected_version": "1", "expected_epoch": "1"})
        fd = os.open(args.activation_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.close(fd)
        for kind in kinds:
            key = {"namespace": "production", "workflow_id": kind, "run_id": "run-1"}
            call("durable.start", key | {"request_id": "fixture-start-" + kind, "workflow_type": kind, "build_id": "operator-v1", "queue": "operator"})
            call("durable.signal", key | {"request_id": "fixture-handoff-" + kind, "build_id": "operator-v1", "name": "handoff"})
    elif args.action == "resume":
        current = call("durable.buildRetire", build | {"request_id": "fixture-retire", "expected_version": "1", "expected_epoch": "1"})
        call("durable.buildResume", build | {"request_id": "fixture-resume", "expected_version": current["version"], "expected_epoch": current["epoch"]})

    until = time.monotonic() + 20
    while True:
        output = {}
        for kind in kinds:
            page = call("durable.tasks", {"namespace": "production", "workflow_id": kind, "run_id": "run-1", "limit": 100}, True)
            output[kind] = [task["deferral"] for task in page["items"] if task.get("deferral")]
        if args.action == "inspect" or all(any(row["active"] == (args.action == "prepare") for row in rows) for rows in output.values()):
            print(json.dumps(output, indent=2))
            return
        if time.monotonic() >= until:
            raise RuntimeError("expected persisted deferrals did not appear")
        time.sleep(0.1)


if __name__ == "__main__":
    main()
