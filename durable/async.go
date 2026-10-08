package durable

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
)

// HashAsyncSecret hashes a canonical hexadecimal encoding of a 32-byte secret.
// Generate the secret with a cryptographic random source before handing it off.
func HashAsyncSecret(secret string) (string, error) {
	if !validAsyncHex(secret) {
		return "", fmt.Errorf("%w: asynchronous secret must encode 32 bytes as lowercase hex", ErrInvalid)
	}
	decoded, err := hex.DecodeString(secret)
	if err != nil {
		return "", fmt.Errorf("%w: invalid asynchronous secret encoding", ErrInvalid)
	}
	digest := sha256.Sum256(decoded)
	return hex.EncodeToString(digest[:]), nil
}

func validAsyncHex(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, c := range value {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func validateAsyncSecret(token TaskToken, secret string) error {
	if token.LeaseKind == LeaseAsync {
		if !validAsyncHex(secret) {
			return fmt.Errorf("%w: asynchronous grant requires a secret", ErrInvalid)
		}
	} else if secret != "" {
		return fmt.Errorf("%w: secret supplied for a worker grant", ErrInvalid)
	}
	return nil
}

func checkAsyncSecret(task Task, secret string) error {
	if task.LeaseKind != LeaseAsync {
		return nil
	}
	digest, err := HashAsyncSecret(secret)
	if err != nil || subtle.ConstantTimeCompare([]byte(digest), []byte(task.AsyncKeyHash)) != 1 {
		return ErrLeaseLost
	}
	return nil
}
