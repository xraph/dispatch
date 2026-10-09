package operator

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/security"
)

// CursorKeys are host-managed 32-byte keys. Retain old versions for the cursor
// lifetime when rotating. Never derive keys from browser input or public IDs.
type CursorKeys struct {
	Active string
	Keys   map[string][]byte
}
type cursorCodec struct {
	active string
	keys   map[string]cipher.AEAD
}
type cursorState struct {
	Binding   string   `json:"binding"`
	Expires   int64    `json:"expires"`
	Position  string   `json:"position"`
	Scope     []string `json:"scope,omitempty"`
	HighWater int64    `json:"high_water,omitempty"`
	Revision  int64    `json:"revision,omitempty"`
}

func newCodec(keys CursorKeys) (cursorCodec, error) {
	c := cursorCodec{active: keys.Active, keys: map[string]cipher.AEAD{}}
	if !durable.DeliveryIdentifier(keys.Active) || strings.Contains(keys.Active, ".") || len(keys.Keys) > 8 {
		return c, durable.ErrInvalid
	}
	for version, key := range keys.Keys {
		if !durable.DeliveryIdentifier(version) || strings.Contains(version, ".") || len(key) != 32 {
			return c, durable.ErrInvalid
		}
		block, err := aes.NewCipher(key)
		if err != nil {
			return c, err
		}
		a, err := cipher.NewGCM(block)
		if err != nil {
			return c, err
		}
		c.keys[version] = a
	}
	if c.keys[c.active] == nil {
		return c, durable.ErrInvalid
	}
	return c, nil
}
func binding(p security.Principal, installation, operation string, filter any) string {
	b, _ := json.Marshal([]any{p.Kind, p.Subject, installation, operation, filter}) //nolint:errcheck // Filters are closed request structs containing JSON primitives.
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func (c cursorCodec) seal(state cursorState, bind string) (string, error) {
	state.Binding = bind
	state.Expires = time.Now().Add(10 * time.Minute).Unix()
	data, err := json.Marshal(state)
	if err != nil {
		return "", err
	}
	a := c.keys[c.active]
	nonce := make([]byte, a.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return "", err
	}
	return c.active + "." + base64.RawURLEncoding.EncodeToString(a.Seal(nonce, nonce, data, []byte(c.active))), nil
}
func (c cursorCodec) open(token, bind string) (cursorState, error) {
	var state cursorState
	if token == "" {
		return state, nil
	}
	if len(token) > 16384 {
		return state, durable.ErrInvalid
	}
	version, encoded, ok := strings.Cut(token, ".")
	a := c.keys[version]
	if !ok || a == nil {
		return state, durable.ErrInvalid
	}
	data, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(data) < a.NonceSize() {
		return state, durable.ErrInvalid
	}
	plain, err := a.Open(nil, data[:a.NonceSize()], data[a.NonceSize():], []byte(version))
	if err != nil || json.Unmarshal(plain, &state) != nil || state.Binding != bind || state.Expires <= time.Now().Unix() {
		return cursorState{}, durable.ErrInvalid
	}
	return state, nil
}
