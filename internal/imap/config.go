// Package imap provides an IMAP email client implementing gmail.API.
package imap

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
)

// Config holds connection settings for an IMAP server.
type Config struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	TLS      bool   `json:"tls"`      // Implicit TLS (IMAPS, port 993)
	STARTTLS bool   `json:"starttls"` // STARTTLS upgrade (port 143)
	Username string `json:"username"`
}

// Addr returns the "host:port" string.
func (c *Config) Addr() string {
	port := c.Port
	if port == 0 {
		if c.TLS {
			port = 993
		} else {
			port = 143
		}
	}
	return fmt.Sprintf("%s:%d", c.Host, port)
}

// Identifier returns a canonical string like "imaps://user@host:port".
func (c *Config) Identifier() string {
	scheme := "imap"
	if c.TLS {
		scheme = "imaps"
	}
	port := c.Port
	if port == 0 {
		if c.TLS {
			port = 993
		} else {
			port = 143
		}
	}
	return fmt.Sprintf("%s://%s@%s:%d", scheme, url.PathEscape(c.Username), c.Host, port)
}

// ToJSON serializes the config to JSON.
func (c *Config) ToJSON() (string, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// ConfigFromJSON parses a config from JSON.
func ConfigFromJSON(s string) (*Config, error) {
	var c Config
	if err := json.Unmarshal([]byte(s), &c); err != nil {
		return nil, fmt.Errorf("parse IMAP config: %w", err)
	}
	return &c, nil
}

// ParseIdentifier parses a config from an identifier URL like "imaps://user@host:port".
func ParseIdentifier(identifier string) (*Config, error) {
	u, err := url.Parse(identifier)
	if err != nil {
		return nil, fmt.Errorf("parse IMAP identifier: %w", err)
	}

	cfg := &Config{}
	switch u.Scheme {
	case "imaps":
		cfg.TLS = true
	case "imap":
		cfg.TLS = false
	default:
		return nil, fmt.Errorf("unsupported scheme %q (expected imap or imaps)", u.Scheme)
	}

	cfg.Host = u.Hostname()
	cfg.Username = u.User.Username()

	portStr := u.Port()
	if portStr != "" {
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return nil, fmt.Errorf("invalid port %q: %w", portStr, err)
		}
		cfg.Port = port
	}

	return cfg, nil
}
