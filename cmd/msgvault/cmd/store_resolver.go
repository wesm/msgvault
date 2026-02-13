package cmd

import (
	"fmt"
	"time"

	"github.com/wesm/msgvault/internal/remote"
	"github.com/wesm/msgvault/internal/store"
)

// MessageStore is the interface for commands that need basic message operations.
// Both store.Store and remote.Store implement this interface.
type MessageStore interface {
	GetStats() (*store.Stats, error)
	ListMessages(offset, limit int) ([]store.APIMessage, int64, error)
	GetMessage(id int64) (*store.APIMessage, error)
	SearchMessages(query string, offset, limit int) ([]store.APIMessage, int64, error)
	Close() error
}

// RemoteStore extends MessageStore with remote-specific operations.
type RemoteStore interface {
	MessageStore
	ListAccounts() ([]remote.AccountInfo, error)
}

// IsRemoteMode returns true if commands should use remote server.
// Resolution order:
//  1. --local flag → always local
//  2. [remote].url set in config → use remote
//  3. Default → use local DB
func IsRemoteMode() bool {
	if useLocal {
		return false
	}
	return cfg != nil && cfg.Remote.URL != ""
}

// OpenStore returns either a local or remote store based on configuration.
// If [remote].url is set in config and --local is not specified, uses remote.
// Otherwise uses local SQLite database.
func OpenStore() (MessageStore, error) {
	if IsRemoteMode() {
		return openRemoteStore()
	}
	return openLocalStore()
}

// OpenRemoteStore opens a remote store, returning error if not configured.
// Unlike OpenStore, this always attempts remote connection.
func OpenRemoteStore() (RemoteStore, error) {
	if cfg.Remote.URL == "" {
		return nil, fmt.Errorf("remote server not configured\n\n" +
			"Configure in ~/.msgvault/config.toml:\n" +
			"  [remote]\n" +
			"  url = \"http://nas:8080\"\n" +
			"  api_key = \"your-api-key\"\n" +
			"  allow_insecure = true  # for trusted networks")
	}
	return openRemoteStore()
}

// openLocalStore opens the local SQLite database.
func openLocalStore() (*store.Store, error) {
	dbPath := cfg.DatabaseDSN()
	return store.Open(dbPath)
}

// openRemoteStore creates a remote store client.
func openRemoteStore() (*remote.Store, error) {
	return remote.New(remote.Config{
		URL:           cfg.Remote.URL,
		APIKey:        cfg.Remote.APIKey,
		AllowInsecure: cfg.Remote.AllowInsecure,
		Timeout:       30 * time.Second,
	})
}

// MustBeLocal returns an error if remote mode is active.
// Use this for commands that only work with local database.
func MustBeLocal(cmdName string) error {
	if IsRemoteMode() && !useLocal {
		return fmt.Errorf("%s requires local database\n\n"+
			"This command cannot run against a remote server.\n"+
			"Use --local flag to force local database.", cmdName)
	}
	return nil
}
