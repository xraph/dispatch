package sinkhost

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Register removal verification before TempDir, and join workers before removal.
func processFiles(t *testing.T, evidence string, stop, verify func()) (dir, private string) {
	t.Helper()
	t.Cleanup(func() {
		if _, err := os.Stat(private); !errors.Is(err, os.ErrNotExist) {
			t.Error("private callback directory survived cleanup")
		}
	})
	dir = t.TempDir()
	if evidence != "" {
		dir = evidence
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal("cannot create retained evidence directory")
		}
	}
	private = t.TempDir()
	t.Cleanup(verify)
	t.Cleanup(stop)
	return dir, private
}

func checkEvidence(dir string, secrets, proofFiles []string) error {
	return filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return errors.New("cannot inspect retained evidence")
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return errors.New("unexpected retained evidence file type")
		}
		for _, name := range proofFiles {
			if entry.Name() == name || entry.Name() == name+".tmp" {
				return errors.New("callback proof file found in retained evidence")
			}
		}
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			return errors.New("cannot read retained evidence")
		}
		for _, secret := range secrets {
			if secret != "" && bytes.Contains(raw, []byte(secret)) {
				return errors.New("secret found in retained host evidence")
			}
		}
		return nil
	})
}

// Collect after worker joins as well, including proofs the scenario never read.
func collectCallbackProofs(dir string) (secrets, files []string, result error) {
	if dir == "" {
		return nil, nil, nil
	}
	result = filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return errors.New("cannot inspect private callback files")
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return errors.New("unsafe private callback file type")
		}
		files = append(files, strings.TrimSuffix(entry.Name(), ".tmp"))
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			return errors.New("cannot read private callback file")
		}
		var proof struct {
			Secret string `json:"secret"`
		}
		if json.Unmarshal(raw, &proof) != nil || proof.Secret == "" {
			return errors.New("invalid private callback proof")
		}
		secrets = append(secrets, proof.Secret)
		return nil
	})
	return secrets, files, result
}

func TestProcessFilesCleanup(t *testing.T) {
	evidence, result := t.TempDir(), filepath.Join(t.TempDir(), "result.json")
	// An old empty handle directory must not be reused or cause a mkdir collision.
	if err := os.Mkdir(filepath.Join(evidence, "callback-handles"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"success", "failure", "success"} {
		cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestProcessFilesCleanupChild$")
		cmd.Env = append(os.Environ(), "DISPATCH_FILES_CHILD="+scenario, "DISPATCH_FILES_EVIDENCE="+evidence, "DISPATCH_FILES_RESULT="+result)
		if err := os.Remove(result); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal("cannot clear redacted lifecycle result")
		}
		output, err := cmd.CombinedOutput()
		if (scenario == "failure") != (err != nil) {
			t.Fatalf("unexpected child outcome for %s", scenario)
		}
		if bytes.Contains(output, []byte("opaque-fixture-proof")) {
			t.Fatal("child output exposed fixture proof")
		}
		var state struct{ Removed, Joined, ProofPresentAtJoin, EvidenceChecked bool }
		raw, readErr := os.ReadFile(result)
		if readErr != nil || json.Unmarshal(raw, &state) != nil || !state.Removed || !state.Joined || !state.ProofPresentAtJoin || !state.EvidenceChecked {
			t.Fatal("private fixture cleanup did not complete")
		}
		if err := checkEvidence(evidence, []string{"opaque-fixture-proof"}, []string{"proof.json"}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestProcessFilesCleanupChild(t *testing.T) {
	scenario := os.Getenv("DISPATCH_FILES_CHILD")
	if scenario == "" {
		t.Skip("subprocess lifecycle probe")
	}
	var private string
	var state struct{ Removed, Joined, ProofPresentAtJoin, EvidenceChecked bool }
	t.Cleanup(func() {
		_, err := os.Stat(private)
		state.Removed = errors.Is(err, os.ErrNotExist)
		raw, marshalErr := json.Marshal(state)
		if marshalErr != nil || os.WriteFile(os.Getenv("DISPATCH_FILES_RESULT"), raw, 0o600) != nil {
			t.Error("cannot save redacted cleanup result")
		}
	})
	stop, done := make(chan struct{}), make(chan bool, 1)
	var dir string
	dir, private = processFiles(t, os.Getenv("DISPATCH_FILES_EVIDENCE"), func() {
		close(stop)
		state.ProofPresentAtJoin = <-done
		state.Joined = true
	}, func() {
		secrets, files, err := collectCallbackProofs(private)
		if err != nil {
			t.Error(err)
		}
		if err := checkEvidence(dir, secrets, files); err != nil {
			t.Error(err)
		} else if len(secrets) == 1 {
			state.EvidenceChecked = true
		}
	})
	path := filepath.Join(private, "proof.json")
	ready := make(chan bool, 1)
	go func() {
		err := os.WriteFile(path, []byte(`{"secret":"opaque-fixture-proof"}`), 0o600)
		ready <- err == nil
		<-stop
		raw, err := os.ReadFile(path)
		done <- err == nil && bytes.Contains(raw, []byte("opaque-fixture-proof"))
	}()
	if !<-ready {
		t.Fatal("cannot save private fixture proof")
	}
	if scenario == "failure" {
		t.Fatal("deliberate fixture failure")
	}
}

func TestEvidenceRejectsNestedProofs(t *testing.T) {
	dir := t.TempDir()
	nested := filepath.Join(dir, "nested")
	if err := os.Mkdir(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(nested, "proof.json")
	if err := os.WriteFile(path, []byte("opaque-fixture-proof"), 0o600); err != nil {
		t.Fatal(err)
	}
	if checkEvidence(dir, []string{"opaque-fixture-proof"}, nil) == nil || checkEvidence(dir, nil, []string{"proof.json"}) == nil {
		t.Fatal("nested private evidence escaped detection")
	}
}

func TestCallbackProofCollectionRejectsLinks(t *testing.T) {
	private, evidence := t.TempDir(), t.TempDir()
	proof := filepath.Join(private, "proof.json")
	if err := os.WriteFile(proof, []byte(`{"secret":"unobserved-worker-proof"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	secrets, files, err := collectCallbackProofs(private)
	if err != nil || len(secrets) != 1 || len(files) != 1 {
		t.Fatal("unobserved worker proof missing")
	}
	if err := os.WriteFile(filepath.Join(evidence, "renamed.log"), []byte("unobserved-worker-proof"), 0o600); err != nil {
		t.Fatal(err)
	}
	if checkEvidence(evidence, secrets, files) == nil {
		t.Fatal("unobserved proof retained")
	}
	if err := os.Symlink(proof, filepath.Join(evidence, "link")); err != nil {
		t.Fatal(err)
	}
	if checkEvidence(evidence, nil, nil) == nil {
		t.Fatal("retained symlink accepted")
	}
	if err := os.Symlink(evidence, filepath.Join(private, "link")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := collectCallbackProofs(private); err == nil {
		t.Fatal("private symlink accepted")
	}
}
