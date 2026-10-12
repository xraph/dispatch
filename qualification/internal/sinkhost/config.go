// Package sinkhost implements the local process qualification composition.
package sinkhost

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"os"
	"strings"
	"time"

	"github.com/xraph/grove"
	"github.com/xraph/grove/drivers/pgdriver"
	_ "github.com/xraph/grove/drivers/pgdriver/pgmigrate" // PostgreSQL migrations.

	"github.com/xraph/dispatch/durable/delivery/ecosystem"
	"github.com/xraph/dispatch/qualification/internal/authority"
)

type Config struct {
	CallbackDirectory  string                          `json:"callback_directory,omitempty"`
	Binding            ecosystem.Binding               `json:"binding"`
	EnvironmentID      string                          `json:"environment_id"`
	PolicyTenant       string                          `json:"policy_tenant"`
	DSNs               map[string]string               `json:"dsns"`
	Addresses          map[string]string               `json:"addresses"`
	Credentials        map[string]authority.Credential `json:"credentials"`
	TokenKey           []byte                          `json:"token_key"`
	HMACKey            []byte                          `json:"hmac_key"`
	WebhookSecret      string                          `json:"webhook_secret"`
	LostAckDestination string                          `json:"lost_ack_destination,omitempty"`
	LostAckMarker      string                          `json:"lost_ack_marker,omitempty"`
}

func (c Config) Validate() error {
	if c.Binding.Validate() != nil || c.PolicyTenant == "" || c.EnvironmentID == "" || len(c.HMACKey) != 32 || len(c.TokenKey) != 32 {
		return errors.New("qualification: invalid host configuration")
	}
	for _, role := range []string{"dispatch", "relay", "chronicle", "receiver"} {
		host, _, err := net.SplitHostPort(c.Addresses[role])
		if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
			return errors.New("qualification: loopback hosts required")
		}
	}
	return nil
}
func Save(path string, c Config) error {
	data, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}
func Load(path string) (Config, error) {
	var c Config
	info, err := os.Stat(path)
	if err != nil {
		return c, err
	}
	if info.Mode().Perm()&0o077 != 0 {
		return c, errors.New("qualification: private config mode required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	if err := json.Unmarshal(data, &c); err != nil {
		return c, err
	}
	return c, c.Validate()
}
func Open(ctx context.Context, dsn string) (*grove.DB, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	driver := pgdriver.New()
	if err := driver.Open(ctx, dsn); err != nil {
		return nil, errors.New("qualification: database unavailable")
	}
	return grove.Open(driver)
}

// SafeError strips every configured secret before retaining startup diagnostics.
func SafeError(err error, c Config) string {
	if err == nil {
		return ""
	}
	message := err.Error()
	secrets := []string{string(c.TokenKey), hex.EncodeToString(c.TokenKey), base64.StdEncoding.EncodeToString(c.TokenKey), c.WebhookSecret, string(c.HMACKey), hex.EncodeToString(c.HMACKey), base64.StdEncoding.EncodeToString(c.HMACKey)}
	for _, dsn := range c.DSNs {
		secrets = append(secrets, dsn)
	}
	for _, credential := range c.Credentials {
		secrets = append(secrets, credential.Secret)
	}
	for _, secret := range secrets {
		if secret != "" {
			message = strings.ReplaceAll(message, secret, "[redacted]")
		}
	}
	return message
}
