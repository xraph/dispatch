package qualification_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/xraph/authsome/authprovider"
	"github.com/xraph/authsome/dpop"

	"github.com/xraph/dispatch/dwp"
)

func proofKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key
}
func proof(t *testing.T, key *ecdsa.PrivateKey, token, method, uri string) string {
	t.Helper()
	jwk := map[string]string{"kty": "EC", "crv": "P-256", "x": base64.RawURLEncoding.EncodeToString(key.X.FillBytes(make([]byte, 32))), "y": base64.RawURLEncoding.EncodeToString(key.Y.FillBytes(make([]byte, 32)))}
	claims := jwt.MapClaims{"jti": fmt.Sprintf("proof-%d", time.Now().UnixNano()), "htm": method, "htu": uri, "iat": time.Now().Unix(), "ath": dpop.AccessTokenHash(token)}
	p := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	p.Header["typ"] = "dpop+jwt"
	p.Header["jwk"] = jwk
	raw, err := p.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func thumbprint(t *testing.T, key *ecdsa.PrivateKey) string {
	t.Helper()
	p, err := dpop.Parse(proof(t, key, "", http.MethodGet, "https://host/dwp"))
	if err != nil {
		t.Fatal(err)
	}
	return p.JKT
}

func TestBoundFrameProofs(t *testing.T) {
	for _, variant := range []string{"bound-bearer", "missing-proof", "malformed-proof", "wrong-key", "wrong-token-hash", "wrong-method", "wrong-uri", "valid", "fresh-request-replay", "equal-bound-cookie-frame"} {
		t.Run(variant, func(t *testing.T) {
			h := newCredentialHost(t)
			key := proofKey(t)
			bound := h.issue(t, 1, thumbprint(t, key))
			presentation := "DPoP " + bound.Token
			proofToken, method, uri := bound.Token, http.MethodGet, h.http.URL+"/dwp"
			signingKey := key
			switch variant {
			case "bound-bearer":
				presentation = "Bearer " + bound.Token
			case "wrong-key":
				signingKey = proofKey(t)
			case "wrong-token-hash":
				proofToken = h.sessions[0].Token
			case "wrong-method":
				method = http.MethodPost
			case "wrong-uri":
				uri = h.http.URL + "/other"
			}
			headers := http.Header{}
			if variant != "missing-proof" {
				headers.Set("DPoP", proof(t, signingKey, proofToken, method, uri))
			}
			if variant == "malformed-proof" {
				headers.Set("DPoP", "invalid-proof")
			}
			if variant == "equal-bound-cookie-frame" {
				headers.Set("Cookie", authprovider.DefaultSessionCookieName+"="+bound.Token)
			}
			response := h.socket(t, headers).exchange(t, dwp.Frame{ID: "auth", Method: dwp.MethodAuth, Token: presentation})
			switch variant {
			case "valid", "fresh-request-replay", "equal-bound-cookie-frame":
				h.assertAdmission(t, response, 1)
			default:
				h.assertAuthenticationDenied(t, response)
			}
			if variant == "fresh-request-replay" {
				// Same proof on a new upgraded request must reach the frame provider
				// and fail replay checks, with an unbound cookie at middleware.
				response = h.socket(t, headers).exchange(t, dwp.Frame{ID: "replay", Method: dwp.MethodAuth, Token: presentation})
				if response.Error == nil || response.Error.Code != dwp.ErrCodeUnauthorized || len(h.policyChecks()) != 2 {
					t.Fatal("fresh request accepted a replayed proof")
				}
				denials := 0
				for _, d := range h.deliveries(t) {
					if d.Outcome == "unauthenticated" && d.Metadata.ActorKind == "anonymous" {
						denials++
					}
				}
				if denials != 1 {
					t.Fatal("missing replay denial audit")
				}
			}
		})
	}
}
