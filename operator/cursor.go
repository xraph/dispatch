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
	"unicode/utf8"

	"github.com/xraph/dispatch/durable"
	"github.com/xraph/dispatch/security"
)

// Outer state contains an inner read cursor or a raw persisted ID, at most 32
// catalog scope IDs, a SHA-256 binding and three int64s. Conservatively allow
// six JSON bytes per string byte (including the inner position), 20 per signed
// int64, and exact object/array punctuation. Default AES-GCM adds a 12-byte
// nonce and 16-byte tag. Raw base64 uses ceil(8*n/6), plus the version and dot.
const maxCursorJSON = len(`{"binding":"","expires":,"position":"","scope":[],"high_water":,"revision":}`) + 6*64 + 3*20 + 6*durable.MaxReadCursorBytes + DiscoveryBudget*(6*durable.MaxDeliveryIdentifierBytes+3)
const maxCursorBytes = durable.MaxDeliveryIdentifierBytes + 1 + (8*(maxCursorJSON+12+16)+5)/6

func validCursorState(state cursorState) bool {
	if len(state.Binding) > 64 || !utf8.ValidString(state.Binding) || len(state.Position) > durable.MaxReadCursorBytes || !utf8.ValidString(state.Position) || len(state.Scope) > DiscoveryBudget {
		return false
	}
	for _, namespace := range state.Scope {
		if !durable.DeliveryIdentifier(namespace) {
			return false
		}
	}
	return true
}

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
	if !validCursorState(state) {
		return "", durable.ErrInvalid
	}
	data, err := json.Marshal(state)
	if err != nil || len(data) > maxCursorJSON {
		return "", durable.ErrInvalid
	}
	a := c.keys[c.active]
	nonce := make([]byte, a.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return "", err
	}
	token := c.active + "." + base64.RawURLEncoding.EncodeToString(a.Seal(nonce, nonce, data, []byte(c.active)))
	if len(token) > maxCursorBytes {
		return "", durable.ErrInvalid
	}
	return token, nil
}
func (c cursorCodec) open(token, bind string) (cursorState, error) {
	var state cursorState
	if token == "" {
		return state, nil
	}
	if len(token) > maxCursorBytes {
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
	if err != nil || len(plain) > maxCursorJSON || json.Unmarshal(plain, &state) != nil || !validCursorState(state) || state.Binding != bind || state.Expires <= time.Now().Unix() {
		return cursorState{}, durable.ErrInvalid
	}
	return state, nil
}
