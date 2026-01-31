package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/search"
)

// Config holds chat session configuration.
type Config struct {
	Model      string
	MaxResults int // top-K messages to include (default: 20)
	MaxBodyLen int // truncate body per message (default: 2000 chars)
}

// Message represents a chat message.
type Message struct {
	Role    string // "system", "user", "assistant"
	Content string
}

// RetrievedMessage holds a message formatted for context injection.
type RetrievedMessage struct {
	ID      int64
	From    string
	To      string
	Date    string
	Subject string
	Labels  string
	Body    string
}

// Session manages a chat conversation with RAG over the email archive.
type Session struct {
	engine  query.Engine
	llm     LLMClient
	history []Message
	cfg     Config
}

// NewSession creates a new chat session.
func NewSession(engine query.Engine, llm LLMClient, cfg Config) *Session {
	if cfg.MaxResults <= 0 {
		cfg.MaxResults = 20
	}
	if cfg.MaxBodyLen <= 0 {
		cfg.MaxBodyLen = 2000
	}
	return &Session{engine: engine, llm: llm, cfg: cfg}
}

// Ask processes a user question: plans a query, retrieves messages, and streams an answer.
func (s *Session) Ask(ctx context.Context, userQuestion string) error {
	s.history = append(s.history, Message{Role: "user", Content: userQuestion})

	// Step 1: Plan query
	sq, reasoning, err := s.planQuery(ctx, userQuestion)
	if err != nil {
		return fmt.Errorf("plan query: %w", err)
	}
	if reasoning != "" {
		fmt.Printf("[Search: %s]\n", reasoning)
	}

	// Step 2: Retrieve messages
	retrieved, err := s.retrieve(ctx, sq)
	if err != nil {
		return fmt.Errorf("retrieve: %w", err)
	}
	fmt.Printf("[Found %d messages]\n\n", len(retrieved))

	// Step 3: Generate answer (streaming)
	contextBlock := formatContext(retrieved)
	answer, err := s.generate(ctx, contextBlock, userQuestion)
	if err != nil {
		return fmt.Errorf("generate: %w", err)
	}

	s.history = append(s.history, Message{Role: "assistant", Content: answer})
	return nil
}

// queryPlan is the JSON structure returned by the LLM query planner.
type queryPlan struct {
	SearchText    string `json:"search_text"`
	From          string `json:"from"`
	To            string `json:"to"`
	After         string `json:"after"`
	Before        string `json:"before"`
	Label         string `json:"label"`
	HasAttachment *bool  `json:"has_attachment"`
	Reasoning     string `json:"reasoning"`
}

func (s *Session) planQuery(ctx context.Context, userQuestion string) (*search.Query, string, error) {
	msgs := []Message{
		{Role: "system", Content: queryPlannerSystem},
	}
	// Include recent history for context
	msgs = append(msgs, s.history...)

	resp, err := s.llm.Chat(ctx, msgs)
	if err != nil {
		return nil, "", fmt.Errorf("llm chat: %w", err)
	}

	// Extract JSON from response (handle markdown code blocks)
	jsonStr := extractJSON(resp)

	var plan queryPlan
	if err := json.Unmarshal([]byte(jsonStr), &plan); err != nil {
		// If parsing fails, fall back to a broad text search
		q := &search.Query{TextTerms: strings.Fields(userQuestion)}
		return q, "broad search (couldn't parse plan)", nil
	}

	return planToQuery(&plan), plan.Reasoning, nil
}

func planToQuery(plan *queryPlan) *search.Query {
	q := &search.Query{}
	if plan.SearchText != "" {
		q.TextTerms = strings.Fields(plan.SearchText)
	}
	if plan.From != "" {
		q.FromAddrs = []string{plan.From}
	}
	if plan.To != "" {
		q.ToAddrs = []string{plan.To}
	}
	if plan.Label != "" {
		q.Labels = []string{plan.Label}
	}
	if plan.HasAttachment != nil {
		q.HasAttachment = plan.HasAttachment
	}
	if plan.After != "" {
		if t, err := time.Parse("2006-01-02", plan.After); err == nil {
			q.AfterDate = &t
		}
	}
	if plan.Before != "" {
		if t, err := time.Parse("2006-01-02", plan.Before); err == nil {
			q.BeforeDate = &t
		}
	}
	return q
}

func (s *Session) retrieve(ctx context.Context, sq *search.Query) ([]RetrievedMessage, error) {
	// Try fast metadata search first
	results, err := s.engine.SearchFast(ctx, sq, query.MessageFilter{}, s.cfg.MaxResults, 0)
	if err != nil {
		return nil, fmt.Errorf("search fast: %w", err)
	}

	// If we have text terms and few fast results, also try FTS
	if len(sq.TextTerms) > 0 && len(results) < s.cfg.MaxResults {
		ftsResults, err := s.engine.Search(ctx, sq, s.cfg.MaxResults, 0)
		if err == nil {
			results = mergeResults(results, ftsResults, s.cfg.MaxResults)
		}
	}

	// Fetch full details for each result
	var retrieved []RetrievedMessage
	for _, r := range results {
		detail, err := s.engine.GetMessage(ctx, r.ID)
		if err != nil {
			continue
		}
		retrieved = append(retrieved, formatMessage(detail, s.cfg.MaxBodyLen))
	}
	return retrieved, nil
}

func (s *Session) generate(ctx context.Context, contextBlock, userQuestion string) (string, error) {
	msgs := []Message{
		{Role: "system", Content: answerGenerationSystem},
		{Role: "system", Content: "Retrieved emails:\n\n" + contextBlock},
	}
	// Add conversation history (skip the last user message, we'll add it explicitly)
	msgs = append(msgs, s.history[:len(s.history)-1]...)
	msgs = append(msgs, Message{Role: "user", Content: userQuestion})

	var answer strings.Builder
	err := s.llm.ChatStream(ctx, msgs, func(token string) {
		fmt.Print(token)
		answer.WriteString(token)
	})
	fmt.Println() // newline after streaming
	if err != nil {
		return "", err
	}
	return answer.String(), nil
}

func formatMessage(d *query.MessageDetail, maxBodyLen int) RetrievedMessage {
	var fromParts []string
	for _, a := range d.From {
		if a.Name != "" {
			fromParts = append(fromParts, fmt.Sprintf("%s <%s>", a.Name, a.Email))
		} else {
			fromParts = append(fromParts, a.Email)
		}
	}
	var toParts []string
	for _, a := range d.To {
		if a.Name != "" {
			toParts = append(toParts, fmt.Sprintf("%s <%s>", a.Name, a.Email))
		} else {
			toParts = append(toParts, a.Email)
		}
	}

	body := d.BodyText
	if len(body) > maxBodyLen {
		body = body[:maxBodyLen] + "..."
	}

	return RetrievedMessage{
		ID:      d.ID,
		From:    strings.Join(fromParts, ", "),
		To:      strings.Join(toParts, ", "),
		Date:    d.SentAt.Format("2006-01-02"),
		Subject: d.Subject,
		Labels:  strings.Join(d.Labels, ", "),
		Body:    body,
	}
}

func formatContext(msgs []RetrievedMessage) string {
	if len(msgs) == 0 {
		return "(No emails found matching the search criteria)"
	}
	var sb strings.Builder
	for i, m := range msgs {
		fmt.Fprintf(&sb, "=== Message %d ===\n", i+1)
		fmt.Fprintf(&sb, "From: %s\n", m.From)
		fmt.Fprintf(&sb, "To: %s\n", m.To)
		fmt.Fprintf(&sb, "Date: %s\n", m.Date)
		fmt.Fprintf(&sb, "Subject: %s\n", m.Subject)
		if m.Labels != "" {
			fmt.Fprintf(&sb, "Labels: %s\n", m.Labels)
		}
		fmt.Fprintf(&sb, "\n%s\n\n", m.Body)
	}
	return sb.String()
}

func mergeResults(a, b []query.MessageSummary, maxResults int) []query.MessageSummary {
	seen := make(map[int64]bool, len(a))
	for _, r := range a {
		seen[r.ID] = true
	}
	merged := append([]query.MessageSummary{}, a...)
	for _, r := range b {
		if !seen[r.ID] && len(merged) < maxResults {
			merged = append(merged, r)
			seen[r.ID] = true
		}
	}
	return merged
}

// extractJSON attempts to extract a JSON object from text that may contain
// markdown code blocks or other wrapping.
func extractJSON(s string) string {
	s = strings.TrimSpace(s)
	// Strip markdown code fences
	if strings.HasPrefix(s, "```") {
		lines := strings.Split(s, "\n")
		// Remove first and last lines (fences)
		if len(lines) >= 2 {
			lines = lines[1:]
		}
		if len(lines) >= 1 && strings.HasPrefix(strings.TrimSpace(lines[len(lines)-1]), "```") {
			lines = lines[:len(lines)-1]
		}
		s = strings.Join(lines, "\n")
	}
	// Find first { and last }
	start := strings.Index(s, "{")
	end := strings.LastIndex(s, "}")
	if start >= 0 && end > start {
		return s[start : end+1]
	}
	return s
}
