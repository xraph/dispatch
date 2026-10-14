#!/usr/bin/env python3
"""Run the assembled Linux drills against an explicitly configured isolated host."""

import argparse
import os
from pathlib import Path
import subprocess


TESTS = (
    "AdmissionAuthenticationAndRestoreRefusal",
    "ProviderServiceEnrollmentAndNativeChannel",
    "KilledWorkerCannotReuseBootEnrollment",
    "ServiceForgeOperatorRegisterDrainAndBegin",
    "CoreOriginalLifecycleAndNativeBegin",
    "CoreOriginalQuiescenceAdmission",
    "ControllerProcessRestartRetainsOriginalWorker",
    "PostgresRestoreHistoryReceiptAndProcessRestart",
    "PostgresSameFloorRestoreQuarantinesIndependentLedger",
    "PostgresPhysicalReplicaPromotion",
    "WorkerLoadAndMidpointDrain",
    "ManagedRecoverySafeAbort",
    "ManagedRecoveryLostGrantClosesThenRetries",
    "ManagedRecoveryLostDockerResponseNeverResends",
    "ManagedRecoveryPositiveFinish",
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    args = parser.parse_args()
    required = ("DSN", "NATIVE_HOST", "DOCKER_HOST", "HOST", "IMAGE", "ARTIFACT", "PROVENANCE")
    missing = [name for name in required if not os.environ.get("WRITERFLOOR_SERVICE_TEST_" + name)]
    if missing:
        parser.error("missing WRITERFLOOR_SERVICE_TEST_ variables: " + ", ".join(missing))
    env = dict(os.environ, WRITERFLOOR_SERVICE_TEST_REQUIRED="1")
    pattern = "^TestLive(" + "|".join(TESTS) + ")$"
    return subprocess.run([str(args.binary.resolve()), "-test.run", pattern,
                           "-test.v", "-test.count=1", "-test.timeout=10m"],
                          env=env, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
