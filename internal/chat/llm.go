package chat

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/ollama/ollama/api"
)

// LLMClient abstracts LLM providers for swappability.
type LLMClient interface {
	// ChatStream sends messages and streams response tokens via callback.
	ChatStream(ctx context.Context, messages []Message, onToken func(string)) error
	// Chat sends messages and returns the complete response.
	Chat(ctx context.Context, messages []Message) (string, error)
}

// OllamaClient implements LLMClient using the Ollama API.
type OllamaClient struct {
	client *api.Client
	model  string
}

// NewOllamaClient creates an OllamaClient for the given server and model.
func NewOllamaClient(serverURL, model string) (*OllamaClient, error) {
	if serverURL == "" {
		serverURL = "http://localhost:11434"
	}
	// Prepend scheme if missing so url.Parse produces a valid host.
	if !strings.HasPrefix(serverURL, "http://") && !strings.HasPrefix(serverURL, "https://") {
		serverURL = "http://" + serverURL
	}
	u, err := url.Parse(serverURL)
	if err != nil {
		return nil, fmt.Errorf("invalid server URL %q: %w", serverURL, err)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("invalid server URL %q: missing host", serverURL)
	}
	client := api.NewClient(u, &http.Client{})
	return &OllamaClient{client: client, model: model}, nil
}

func toOllamaMessages(msgs []Message) []api.Message {
	out := make([]api.Message, len(msgs))
	for i, m := range msgs {
		out[i] = api.Message{Role: m.Role, Content: m.Content}
	}
	return out
}

func (o *OllamaClient) ChatStream(ctx context.Context, messages []Message, onToken func(string)) error {
	req := &api.ChatRequest{
		Model:    o.model,
		Messages: toOllamaMessages(messages),
		Stream:   new(bool), // non-nil = streaming
	}
	*req.Stream = true

	return o.client.Chat(ctx, req, func(resp api.ChatResponse) error {
		if resp.Message.Content != "" {
			onToken(resp.Message.Content)
		}
		return nil
	})
}

func (o *OllamaClient) Chat(ctx context.Context, messages []Message) (string, error) {
	var sb strings.Builder
	req := &api.ChatRequest{
		Model:    o.model,
		Messages: toOllamaMessages(messages),
		Stream:   new(bool),
	}
	*req.Stream = false

	err := o.client.Chat(ctx, req, func(resp api.ChatResponse) error {
		sb.WriteString(resp.Message.Content)
		return nil
	})
	if err != nil {
		return "", err
	}
	return sb.String(), nil
}
