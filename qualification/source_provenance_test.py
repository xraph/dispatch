"""Exercise source refusal without building or publishing a native executable."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("source-provenance.py")


class SourceProvenanceTests(unittest.TestCase):
    def test_clean_and_explicit_dirty_snapshots(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repo = root / "repo"
            repo.mkdir()
            def git(*args):
                subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)
            git("init", "--initial-branch=main")
            git("config", "user.name", "Fixture")
            git("config", "user.email", "fixture@example.test")
            source = repo / "source.txt"
            source.write_text("original\n")
            git("add", "source.txt")
            git("commit", "-m", "test: initialize provenance fixture")
            sequence = 0
            def invoke(mode, expected=None, ci=False):
                nonlocal sequence
                sequence += 1
                output = root / (str(sequence) + ".json")
                args = ["python3", str(SCRIPT), mode, "--repo", str(repo), "--output", str(output)]
                if expected:
                    args += ["--expected", str(expected)]
                env = dict(os.environ)
                env.pop("CI", None)
                if ci:
                    env["CI"] = "true"
                result = subprocess.run(args, capture_output=True, env=env)
                return result.returncode, output, json.loads(output.read_text())
            code, _, clean = invoke("check")
            self.assertEqual(code, 0)
            self.assertFalse(clean["dirty"])
            source.write_text("known edit\n")
            (repo / "added.txt").write_text("known untracked\n")
            code, _, refused = invoke("check")
            self.assertEqual(code, 1)
            self.assertEqual(refused["qualification_decision"], "rejected")
            code, captured, dirty = invoke("capture")
            self.assertEqual(code, 0)
            self.assertTrue(dirty["dirty"])
            self.assertEqual(dirty["files"]["added.txt"]["kind"], "untracked")
            code, _, approved = invoke("check", captured)
            self.assertEqual(code, 0)
            self.assertEqual(approved["snapshot_sha256"], dirty["snapshot_sha256"])
            code, _, _ = invoke("check", captured, ci=True)
            self.assertEqual(code, 1)
            source.write_text("unexpected later edit\n")
            code, _, _ = invoke("check", captured)
            self.assertEqual(code, 1)


if __name__ == "__main__":
    unittest.main()
