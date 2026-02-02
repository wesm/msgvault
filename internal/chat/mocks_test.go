package chat

import (
	"context"

	"github.com/wesm/msgvault/internal/query/querytest"
)

// stubLLM is a test double for LLMClient.
type stubLLM struct {
	chatResp   string
	chatErr    error
	streamResp string
	streamErr  error
}

func (s *stubLLM) Chat(_ context.Context, _ []Message) (string, error) {
	return s.chatResp, s.chatErr
}

func (s *stubLLM) ChatStream(_ context.Context, _ []Message, onToken func(string)) error {
	if s.streamErr != nil {
		return s.streamErr
	}
	onToken(s.streamResp)
	return nil
}

// newTestSession creates a Session with sensible defaults for testing.
// Pass nil for eng or llm to use zero-value stubs.
func newTestSession(eng *querytest.MockEngine, llm *stubLLM) *Session {
	if eng == nil {
		eng = &querytest.MockEngine{}
	}
	if llm == nil {
		llm = &stubLLM{}
	}
	return NewSession(eng, llm, Config{MaxResults: 10, MaxBodyLen: 500})
}
