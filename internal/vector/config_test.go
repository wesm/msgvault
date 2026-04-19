package vector

import (
	"testing"
	"time"

	"github.com/BurntSushi/toml"
)

func TestConfig_DefaultsAndParse(t *testing.T) {
	input := `
enabled = true
backend = "sqlite-vec"
db_path = "/tmp/vectors.db"

[embeddings]
endpoint = "http://mac-studio.tailnet:8080/v1"
api_key_env = "MSGVAULT_EMBED_KEY"
model = "nomic-embed-text-v1.5"
dimension = 768
batch_size = 32
timeout = "15s"
max_retries = 2
max_input_chars = 16000

[preprocess]
strip_quotes = true
strip_signatures = true

[search]
rrf_k = 60
k_per_signal = 100
subject_boost = 2.0
max_page_size_hybrid = 50

[embed.schedule]
cron = "*/5 * * * *"
run_after_sync = true
`
	var c Config
	if _, err := toml.Decode(input, &c); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !c.Enabled {
		t.Fatal("Enabled should be true")
	}
	if c.Backend != "sqlite-vec" {
		t.Errorf("Backend=%q, want sqlite-vec", c.Backend)
	}
	if c.Embeddings.Dimension != 768 {
		t.Errorf("Dimension=%d, want 768", c.Embeddings.Dimension)
	}
	if c.Embeddings.Timeout != 15*time.Second {
		t.Errorf("Timeout=%v, want 15s", c.Embeddings.Timeout)
	}
	if c.Search.RRFK != 60 {
		t.Errorf("RRFK=%d, want 60", c.Search.RRFK)
	}
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{"OK", func(c *Config) {}, ""},
		{"MissingEndpoint", func(c *Config) { c.Embeddings.Endpoint = "" }, "endpoint"},
		{"InvalidEndpoint", func(c *Config) { c.Embeddings.Endpoint = "::not a url" }, "endpoint"},
		{"ZeroDim", func(c *Config) { c.Embeddings.Dimension = 0 }, "dimension"},
		{"NegativeDim", func(c *Config) { c.Embeddings.Dimension = -1 }, "dimension"},
		{"UnknownBackend", func(c *Config) { c.Backend = "mystery" }, "backend"},
		{"ZeroBatch", func(c *Config) { c.Embeddings.BatchSize = 0 }, "batch_size"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := validConfig()
			tt.mutate(&c)
			err := c.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q missing %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func validConfig() Config {
	return Config{
		Enabled: true,
		Backend: "sqlite-vec",
		DBPath:  "/tmp/v.db",
		Embeddings: EmbeddingsConfig{
			Endpoint:      "http://localhost:8080/v1",
			Model:         "nomic-embed-text",
			Dimension:     768,
			BatchSize:     32,
			Timeout:       10 * time.Second,
			MaxRetries:    2,
			MaxInputChars: 16000,
		},
		Search: SearchConfig{
			RRFK:              60,
			KPerSignal:        100,
			SubjectBoost:      2.0,
			MaxPageSizeHybrid: 50,
		},
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
