package chat

import (
	"context"
	"fmt"

	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/search"
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

// stubEngine implements the minimal query.Engine methods used by chat.
type stubEngine struct {
	searchFastResults []query.MessageSummary
	searchResults     []query.MessageSummary
	messages          map[int64]*query.MessageDetail
}

func (e *stubEngine) SearchFast(_ context.Context, _ *search.Query, _ query.MessageFilter, _, _ int) ([]query.MessageSummary, error) {
	return e.searchFastResults, nil
}

func (e *stubEngine) Search(_ context.Context, _ *search.Query, _, _ int) ([]query.MessageSummary, error) {
	return e.searchResults, nil
}

func (e *stubEngine) GetMessage(_ context.Context, id int64) (*query.MessageDetail, error) {
	if m, ok := e.messages[id]; ok {
		return m, nil
	}
	return nil, fmt.Errorf("not found")
}

// Unused interface methods — return zero values.
func (e *stubEngine) AggregateBySender(context.Context, query.AggregateOptions) ([]query.AggregateRow, error) {
	return nil, nil
}
func (e *stubEngine) AggregateBySenderName(context.Context, query.AggregateOptions) ([]query.AggregateRow, error) {
	return nil, nil
}
func (e *stubEngine) AggregateByRecipient(context.Context, query.AggregateOptions) ([]query.AggregateRow, error) {
	return nil, nil
}
func (e *stubEngine) AggregateByRecipientName(context.Context, query.AggregateOptions) ([]query.AggregateRow, error) {
	return nil, nil
}
func (e *stubEngine) AggregateByDomain(context.Context, query.AggregateOptions) ([]query.AggregateRow, error) {
	return nil, nil
}
func (e *stubEngine) AggregateByLabel(context.Context, query.AggregateOptions) ([]query.AggregateRow, error) {
	return nil, nil
}
func (e *stubEngine) AggregateByTime(context.Context, query.AggregateOptions) ([]query.AggregateRow, error) {
	return nil, nil
}
func (e *stubEngine) SubAggregate(context.Context, query.MessageFilter, query.ViewType, query.AggregateOptions) ([]query.AggregateRow, error) {
	return nil, nil
}
func (e *stubEngine) ListMessages(context.Context, query.MessageFilter) ([]query.MessageSummary, error) {
	return nil, nil
}
func (e *stubEngine) GetMessageBySourceID(context.Context, string) (*query.MessageDetail, error) {
	return nil, nil
}
func (e *stubEngine) SearchFastCount(context.Context, *search.Query, query.MessageFilter) (int64, error) {
	return 0, nil
}
func (e *stubEngine) GetAttachment(context.Context, int64) (*query.AttachmentInfo, error) {
	return nil, nil
}
func (e *stubEngine) GetGmailIDsByFilter(context.Context, query.MessageFilter) ([]string, error) {
	return nil, nil
}
func (e *stubEngine) ListAccounts(context.Context) ([]query.AccountInfo, error) { return nil, nil }
func (e *stubEngine) GetTotalStats(context.Context, query.StatsOptions) (*query.TotalStats, error) {
	return nil, nil
}
func (e *stubEngine) Close() error { return nil }

// newTestSession creates a Session with sensible defaults for testing.
// Pass nil for eng or llm to use zero-value stubs.
func newTestSession(eng *stubEngine, llm *stubLLM) *Session {
	if eng == nil {
		eng = &stubEngine{}
	}
	if llm == nil {
		llm = &stubLLM{}
	}
	return NewSession(eng, llm, Config{MaxResults: 10, MaxBodyLen: 500})
}
