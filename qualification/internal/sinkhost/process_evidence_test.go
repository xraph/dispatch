package sinkhost

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"

	ca "github.com/xraph/chronicle/acceptance"
	ra "github.com/xraph/relay/acceptance"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/durable/delivery/ecosystem"
)

func (r *processRig) verifyBindings() {
	rows, err := r.db["dispatch"].Query(r.ctx, "SELECT envelope,receipt FROM dispatch_durable_outbox WHERE delivered_at IS NOT NULL ORDER BY id")
	if err != nil {
		r.t.Fatal(err)
	}
	defer rows.Close()
	type binding struct {
		Source  durable.Delivery
		Receipt durable.SinkReceipt
	}
	evidence := []binding{}
	for rows.Next() {
		var raw, ack []byte
		if err = rows.Scan(&raw, &ack); err != nil {
			r.t.Fatal(err)
		}
		var item binding
		if err = json.Unmarshal(raw, &item.Source); err != nil {
			r.t.Fatal(err)
		}
		if err = json.Unmarshal(ack, &item.Receipt); err != nil {
			r.t.Fatal(err)
		}
		if item.Receipt.Verify(item.Source) != nil || item.Receipt.MappingVersion != ecosystem.MappingVersion {
			r.t.Fatal("persisted source receipt binding invalid")
		}
		var fingerprint string
		role := string(item.Source.Destination)
		if role == "chronicle" {
			req, e := ecosystem.ChronicleRequest(r.c.Binding, item.Source)
			if e != nil {
				r.t.Fatal(e)
			}
			fingerprint, err = ca.Fingerprint(req)
		} else {
			req, e := ecosystem.RelayRequest(r.c.Binding, item.Source)
			if e != nil {
				r.t.Fatal(e)
			}
			fingerprint, err = ra.Fingerprint(req)
		}
		if err != nil || fingerprint != item.Receipt.SinkFingerprint || fingerprint == item.Source.Fingerprint {
			r.t.Fatal("persisted semantic fingerprint mismatch")
		}
		var accepted []byte
		if err = r.db[role].QueryRow(r.ctx, "SELECT receipt FROM "+role+"_acceptances WHERE receipt->>'source_key'=$1", item.Source.ID).Scan(&accepted); err != nil {
			r.t.Fatal(err)
		}
		want, e := ra.CanonicalJSON(accepted)
		if e != nil {
			r.t.Fatal(e)
		}
		got, e := ra.CanonicalJSON([]byte(item.Receipt.Evidence))
		if e != nil || !bytes.Equal(got, want) {
			r.t.Fatal("source acknowledgement changed sink receipt evidence")
		}
		evidence = append(evidence, item)
	}
	if err = rows.Err(); err != nil {
		r.t.Fatal(err)
	}
	raw, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		r.t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(r.dir, "verified-receipts.json"), raw, 0o600); err != nil {
		r.t.Fatal(err)
	}
}
func (r *processRig) verifyNoSecrets() {
	secrets := make([]string, 0, 6+len(r.c.Credentials))
	secrets = append(secrets, r.c.WebhookSecret, hex.EncodeToString(r.c.HMACKey), base64.StdEncoding.EncodeToString(r.c.HMACKey), hex.EncodeToString(r.c.TokenKey), base64.StdEncoding.EncodeToString(r.c.TokenKey), "qualification-payload-must-not-leak")
	for _, credential := range r.c.Credentials {
		secrets = append(secrets, credential.Secret)
	}
	proofSecrets, proofFiles, proofErr := collectCallbackProofs(r.c.CallbackDirectory)
	if proofErr != nil {
		r.t.Error(proofErr)
	}
	secrets = append(secrets, r.callbackSecrets...)
	secrets = append(secrets, proofSecrets...)
	proofFiles = append(proofFiles, r.callbackFiles...)
	if err := checkEvidence(r.dir, secrets, proofFiles); err != nil {
		r.t.Error(err)
	}
}
