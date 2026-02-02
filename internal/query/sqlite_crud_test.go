package query

import (
	"context"
	"testing"

	"github.com/wesm/msgvault/internal/testutil/dbtest"
)

func TestListMessages(t *testing.T) {
	env := newTestEnv(t)

	messages := env.MustListMessages(MessageFilter{})
	if len(messages) != 5 {
		t.Errorf("expected 5 messages, got %d", len(messages))
	}

	messages = env.MustListMessages(MessageFilter{Sender: "alice@example.com"})
	if len(messages) != 3 {
		t.Errorf("expected 3 messages from alice, got %d", len(messages))
	}

	messages = env.MustListMessages(MessageFilter{Label: "Work"})
	if len(messages) != 2 {
		t.Errorf("expected 2 messages with Work label, got %d", len(messages))
	}
}

func TestListMessagesWithLabels(t *testing.T) {
	env := newTestEnv(t)

	messages := env.MustListMessages(MessageFilter{})

	msg1 := messages[len(messages)-1]
	if len(msg1.Labels) != 2 {
		t.Errorf("expected 2 labels on msg1, got %d", len(msg1.Labels))
	}
}

func TestGetMessage(t *testing.T) {
	env := newTestEnv(t)

	msg, err := env.Engine.GetMessage(env.Ctx, 1)
	if err != nil {
		t.Fatalf("GetMessage: %v", err)
	}

	if msg == nil {
		t.Fatal("expected message, got nil")
	}

	if msg.Subject != "Hello World" {
		t.Errorf("expected subject 'Hello World', got %q", msg.Subject)
	}

	if len(msg.From) != 1 || msg.From[0].Email != "alice@example.com" {
		t.Errorf("expected from alice, got %v", msg.From)
	}

	if len(msg.To) != 2 {
		t.Errorf("expected 2 recipients, got %d", len(msg.To))
	}

	if len(msg.Labels) != 2 {
		t.Errorf("expected 2 labels, got %d", len(msg.Labels))
	}

	if msg.BodyText != "Message body 1" {
		t.Errorf("expected body text 'Message body 1', got %q", msg.BodyText)
	}
}

func TestGetMessageWithAttachments(t *testing.T) {
	env := newTestEnv(t)

	msg, err := env.Engine.GetMessage(env.Ctx, 2)
	if err != nil {
		t.Fatalf("GetMessage: %v", err)
	}

	if len(msg.Attachments) != 2 {
		t.Errorf("expected 2 attachments, got %d", len(msg.Attachments))
	}

	found := false
	for _, att := range msg.Attachments {
		if att.Filename == "doc.pdf" {
			found = true
			if att.MimeType != "application/pdf" {
				t.Errorf("expected mime type application/pdf, got %s", att.MimeType)
			}
			if att.Size != 10000 {
				t.Errorf("expected size 10000, got %d", att.Size)
			}
		}
	}
	if !found {
		t.Error("expected to find doc.pdf attachment")
	}
}

func TestGetMessageBySourceID(t *testing.T) {
	env := newTestEnv(t)

	msg, err := env.Engine.GetMessageBySourceID(env.Ctx, "msg3")
	if err != nil {
		t.Fatalf("GetMessageBySourceID: %v", err)
	}

	if msg == nil {
		t.Fatal("expected message, got nil")
	}

	if msg.Subject != "Follow up" {
		t.Errorf("expected subject 'Follow up', got %q", msg.Subject)
	}
}

func TestListAccounts(t *testing.T) {
	env := newTestEnv(t)

	accounts, err := env.Engine.ListAccounts(env.Ctx)
	if err != nil {
		t.Fatalf("ListAccounts: %v", err)
	}

	if len(accounts) != 1 {
		t.Errorf("expected 1 account, got %d", len(accounts))
	}

	if accounts[0].Identifier != "test@gmail.com" {
		t.Errorf("expected test@gmail.com, got %s", accounts[0].Identifier)
	}
}

func TestGetTotalStats(t *testing.T) {
	env := newTestEnv(t)

	stats := env.MustGetTotalStats(StatsOptions{})

	if stats.MessageCount != 5 {
		t.Errorf("expected 5 messages, got %d", stats.MessageCount)
	}

	if stats.AttachmentCount != 3 {
		t.Errorf("expected 3 attachments, got %d", stats.AttachmentCount)
	}

	expectedSize := int64(1000 + 2000 + 1500 + 3000 + 500)
	if stats.TotalSize != expectedSize {
		t.Errorf("expected total size %d, got %d", expectedSize, stats.TotalSize)
	}

	expectedAttSize := int64(10000 + 5000 + 20000)
	if stats.AttachmentSize != expectedAttSize {
		t.Errorf("expected attachment size %d, got %d", expectedAttSize, stats.AttachmentSize)
	}
}

func TestGetTotalStatsWithSourceID(t *testing.T) {
	env := newTestEnv(t)

	src2 := env.AddSource(dbtest.SourceOpts{Identifier: "other@gmail.com", DisplayName: "Other Account"})
	env.AddLabel(dbtest.LabelOpts{SourceID: src2, SourceLabelID: "INBOX", Name: "INBOX", Type: "system"})
	env.AddLabel(dbtest.LabelOpts{SourceID: src2, SourceLabelID: "personal", Name: "Personal"})
	conv2 := env.AddConversation(dbtest.ConversationOpts{SourceID: src2, Title: "Other Thread"})
	env.AddMessage(dbtest.MessageOpts{
		SourceID:       src2,
		ConversationID: conv2,
		Subject:        "Other msg",
		SentAt:         "2024-01-20 10:00:00",
		SizeEstimate:   500,
	})

	allStats := env.MustGetTotalStats(StatsOptions{})

	if allStats.MessageCount != 6 {
		t.Errorf("expected 6 total messages, got %d", allStats.MessageCount)
	}
	if allStats.LabelCount != 5 {
		t.Errorf("expected 5 total labels, got %d", allStats.LabelCount)
	}
	if allStats.AccountCount != 2 {
		t.Errorf("expected 2 accounts, got %d", allStats.AccountCount)
	}

	sourceID := int64(1)
	source1Stats := env.MustGetTotalStats(StatsOptions{SourceID: &sourceID})

	if source1Stats.MessageCount != 5 {
		t.Errorf("expected 5 messages for source 1, got %d", source1Stats.MessageCount)
	}
	if source1Stats.LabelCount != 3 {
		t.Errorf("expected 3 labels for source 1, got %d", source1Stats.LabelCount)
	}
	if source1Stats.AccountCount != 1 {
		t.Errorf("expected account count 1 when filtering by source, got %d", source1Stats.AccountCount)
	}
}

func TestGetTotalStatsWithInvalidSourceID(t *testing.T) {
	env := newTestEnv(t)

	nonExistentID := int64(9999)
	stats := env.MustGetTotalStats(StatsOptions{SourceID: &nonExistentID})

	if stats.MessageCount != 0 {
		t.Errorf("expected 0 messages for non-existent source, got %d", stats.MessageCount)
	}
	if stats.LabelCount != 0 {
		t.Errorf("expected 0 labels for non-existent source, got %d", stats.LabelCount)
	}
	if stats.AccountCount != 0 {
		t.Errorf("expected 0 account count for non-existent source, got %d", stats.AccountCount)
	}
	if stats.AttachmentCount != 0 {
		t.Errorf("expected 0 attachments for non-existent source, got %d", stats.AttachmentCount)
	}
}

func TestWithAttachmentsOnlyStats(t *testing.T) {
	env := newTestEnv(t)

	allStats := env.MustGetTotalStats(StatsOptions{})
	if allStats.MessageCount != 5 {
		t.Errorf("expected 5 total messages, got %d", allStats.MessageCount)
	}

	attStats := env.MustGetTotalStats(StatsOptions{WithAttachmentsOnly: true})

	if attStats.MessageCount != 2 {
		t.Errorf("expected 2 messages with attachments, got %d", attStats.MessageCount)
	}

	if attStats.AttachmentCount == 0 {
		t.Error("expected non-zero attachment count for messages with attachments")
	}
}

func TestDeletedMessagesIncludedWithFlag(t *testing.T) {
	env := newTestEnv(t)

	env.MarkDeletedByID(1)

	rows, err := env.Engine.AggregateBySender(env.Ctx, DefaultAggregateOptions())
	if err != nil {
		t.Fatalf("AggregateBySender: %v", err)
	}

	for _, row := range rows {
		if row.Key == "alice@example.com" && row.Count != 3 {
			t.Errorf("expected alice count 3 (including deleted), got %d", row.Count)
		}
	}

	messages := env.MustListMessages(MessageFilter{})

	if len(messages) != 5 {
		t.Errorf("expected 5 messages (including deleted), got %d", len(messages))
	}

	var foundDeleted bool
	for _, msg := range messages {
		if msg.ID == 1 {
			if msg.DeletedAt == nil {
				t.Error("expected DeletedAt to be set for deleted message")
			}
			foundDeleted = true
		} else {
			if msg.DeletedAt != nil {
				t.Errorf("expected DeletedAt to be nil for non-deleted message %d", msg.ID)
			}
		}
	}
	if !foundDeleted {
		t.Error("deleted message not found in results")
	}

	stats := env.MustGetTotalStats(StatsOptions{})

	if stats.MessageCount != 5 {
		t.Errorf("expected 5 messages in stats (including deleted), got %d", stats.MessageCount)
	}
}

func TestGetMessageIncludesDeleted(t *testing.T) {
	env := newTestEnv(t)

	env.MarkDeletedByID(1)

	msg, err := env.Engine.GetMessage(env.Ctx, 1)
	if err != nil {
		t.Fatalf("GetMessage: %v", err)
	}
	if msg == nil {
		t.Error("expected deleted message to be returned, got nil")
	}

	msg, err = env.Engine.GetMessage(env.Ctx, 2)
	if err != nil {
		t.Fatalf("GetMessage: %v", err)
	}
	if msg == nil {
		t.Error("expected message, got nil")
	}
}

func TestGetMessageBySourceIDIncludesDeleted(t *testing.T) {
	env := newTestEnv(t)

	env.MarkDeletedBySourceID("msg3")

	msg, err := env.Engine.GetMessageBySourceID(env.Ctx, "msg3")
	if err != nil {
		t.Fatalf("GetMessageBySourceID: %v", err)
	}
	if msg == nil {
		t.Error("expected deleted message to be returned, got nil")
	}

	msg, err = env.Engine.GetMessageBySourceID(env.Ctx, "msg2")
	if err != nil {
		t.Fatalf("GetMessageBySourceID: %v", err)
	}
	if msg == nil {
		t.Error("expected message, got nil")
	}
}

func TestListMessagesTimePeriodInference(t *testing.T) {
	env := newTestEnv(t)

	filter := MessageFilter{TimePeriod: "2024-01"}
	messages := env.MustListMessages(filter)

	if len(messages) != 2 {
		t.Errorf("expected 2 messages for 2024-01, got %d", len(messages))
	}

	messages = env.MustListMessages(MessageFilter{TimePeriod: "2024-01-15"})
	if len(messages) != 1 {
		t.Errorf("expected 1 message for 2024-01-15, got %d", len(messages))
	}

	messages = env.MustListMessages(MessageFilter{TimePeriod: "2024", TimeGranularity: TimeYear})
	if len(messages) != 5 {
		t.Errorf("expected 5 messages for 2024, got %d", len(messages))
	}
}

func TestListMessages_SenderNameFilter(t *testing.T) {
	env := newTestEnv(t)

	filter := MessageFilter{SenderName: "Alice Smith"}
	messages := env.MustListMessages(filter)

	if len(messages) != 3 {
		t.Errorf("expected 3 messages from Alice Smith, got %d", len(messages))
	}
}

func TestListMessages_MatchEmptySenderName(t *testing.T) {
	env := newTestEnvWithEmptyBuckets(t)

	filter := MessageFilter{MatchEmptySenderName: true}
	messages := env.MustListMessages(filter)

	if len(messages) != 1 {
		t.Errorf("expected 1 message with empty sender name, got %d", len(messages))
	}
	if len(messages) > 0 && messages[0].Subject != "No Sender" {
		t.Errorf("expected 'No Sender', got %q", messages[0].Subject)
	}
}

func TestMatchEmptySenderName_MixedFromRecipients(t *testing.T) {
	env := newTestEnv(t)

	nullID := env.AddParticipant(dbtest.ParticipantOpts{Email: nil, DisplayName: nil, Domain: ""})
	env.AddMessage(dbtest.MessageOpts{Subject: "Mixed From", SentAt: "2024-06-01 10:00:00", FromID: 1})
	// Add a second 'from' with null participant
	lastMsgID := env.LastMessageID()
	_, err := env.DB.Exec(`INSERT INTO message_recipients (message_id, participant_id, recipient_type) VALUES (?, ?, 'from')`, lastMsgID, nullID)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	filter := MessageFilter{MatchEmptySenderName: true}
	messages := env.MustListMessages(filter)

	for _, m := range messages {
		if m.Subject == "Mixed From" {
			t.Error("MatchEmptySenderName should not match message with at least one valid from sender")
		}
	}
}

func TestMatchEmptySenderName_CombinedWithDomain(t *testing.T) {
	env := newTestEnvWithEmptyBuckets(t)

	filter := MessageFilter{
		MatchEmptySenderName: true,
		Domain:               "example.com",
	}
	messages := env.MustListMessages(filter)

	if len(messages) != 0 {
		t.Errorf("expected 0 messages for MatchEmptySenderName+Domain, got %d", len(messages))
	}
}

func TestListMessages_MatchEmptySenderName_NotExists(t *testing.T) {
	env := newTestEnv(t)

	env.AddMessage(dbtest.MessageOpts{Subject: "Ghost Message", SentAt: "2024-06-01 10:00:00"})

	filter := MessageFilter{MatchEmptySenderName: true}
	messages := env.MustListMessages(filter)

	if len(messages) != 1 {
		t.Errorf("expected 1 message with empty sender name, got %d", len(messages))
	}
	if len(messages) > 0 && messages[0].Subject != "Ghost Message" {
		t.Errorf("expected 'Ghost Message', got %q", messages[0].Subject)
	}

	for _, m := range messages {
		if m.Subject == "Hello World" || m.Subject == "Re: Hello" {
			t.Errorf("should not match message with valid sender: %q", m.Subject)
		}
	}
}

func TestGetGmailIDsByFilter_SenderName(t *testing.T) {
	env := newTestEnv(t)

	filter := MessageFilter{SenderName: "Alice Smith"}
	ids, err := env.Engine.GetGmailIDsByFilter(env.Ctx, filter)
	if err != nil {
		t.Fatalf("GetGmailIDsByFilter: %v", err)
	}

	if len(ids) != 3 {
		t.Errorf("expected 3 gmail IDs for Alice Smith, got %d", len(ids))
	}
}

func TestListMessages_ConversationIDFilter(t *testing.T) {
	env := newTestEnv(t)

	conv2 := env.AddConversation(dbtest.ConversationOpts{SourceID: 1, Title: "Second Thread"})
	env.AddMessage(dbtest.MessageOpts{
		ConversationID: conv2,
		Subject:        "Thread 2 Message 1",
		SentAt:         "2024-04-01 10:00:00",
		SizeEstimate:   100,
		FromID:         1, // Alice
		ToIDs:          []int64{2}, // Bob
	})
	env.AddMessage(dbtest.MessageOpts{
		ConversationID: conv2,
		Subject:        "Thread 2 Message 2",
		SentAt:         "2024-04-02 11:00:00",
		SizeEstimate:   200,
		FromID:         2, // Bob
		ToIDs:          []int64{1}, // Alice
	})

	convID1 := int64(1)
	messages1 := env.MustListMessages(MessageFilter{ConversationID: &convID1})

	if len(messages1) != 5 {
		t.Errorf("expected 5 messages in conversation 1, got %d", len(messages1))
	}

	for _, msg := range messages1 {
		if msg.ConversationID != 1 {
			t.Errorf("expected conversation_id=1, got %d for message %d", msg.ConversationID, msg.ID)
		}
	}

	messages2 := env.MustListMessages(MessageFilter{ConversationID: &conv2})

	if len(messages2) != 2 {
		t.Errorf("expected 2 messages in conversation 2, got %d", len(messages2))
	}

	for _, msg := range messages2 {
		if msg.ConversationID != conv2 {
			t.Errorf("expected conversation_id=%d, got %d for message %d", conv2, msg.ConversationID, msg.ID)
		}
	}

	filter2Asc := MessageFilter{
		ConversationID: &conv2,
		SortField:      MessageSortByDate,
		SortDirection:  SortAsc,
	}

	messagesAsc := env.MustListMessages(filter2Asc)

	if len(messagesAsc) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(messagesAsc))
	}

	if messagesAsc[0].Subject != "Thread 2 Message 1" {
		t.Errorf("expected first message to be 'Thread 2 Message 1', got %q", messagesAsc[0].Subject)
	}
	if messagesAsc[1].Subject != "Thread 2 Message 2" {
		t.Errorf("expected second message to be 'Thread 2 Message 2', got %q", messagesAsc[1].Subject)
	}
}

// =============================================================================
// MatchEmpty* filter tests
// =============================================================================

func TestListMessages_MatchEmptySender(t *testing.T) {
	env := newTestEnvWithEmptyBuckets(t)

	filter := MessageFilter{MatchEmptySender: true}
	messages := env.MustListMessages(filter)

	if len(messages) != 1 {
		t.Errorf("expected 1 message with empty sender, got %d", len(messages))
	}

	if len(messages) > 0 && messages[0].Subject != "No Sender" {
		t.Errorf("expected 'No Sender' message, got %q", messages[0].Subject)
	}
}

func TestListMessages_MatchEmptyRecipient(t *testing.T) {
	env := newTestEnvWithEmptyBuckets(t)

	filter := MessageFilter{MatchEmptyRecipient: true}
	messages := env.MustListMessages(filter)

	if len(messages) != 2 {
		t.Errorf("expected 2 messages with empty recipients, got %d", len(messages))
	}
}

func TestListMessages_MatchEmptyDomain(t *testing.T) {
	env := newTestEnvWithEmptyBuckets(t)

	filter := MessageFilter{MatchEmptyDomain: true}
	messages := env.MustListMessages(filter)

	if len(messages) != 2 {
		t.Errorf("expected 2 messages with empty domain, got %d", len(messages))
	}
}

func TestListMessages_MatchEmptyLabel(t *testing.T) {
	env := newTestEnvWithEmptyBuckets(t)

	filter := MessageFilter{MatchEmptyLabel: true}
	messages := env.MustListMessages(filter)

	if len(messages) != 4 {
		t.Errorf("expected 4 messages with no labels, got %d", len(messages))
	}
}

func TestListMessages_MatchEmptyFiltersAreIndependent(t *testing.T) {
	env := newTestEnvWithEmptyBuckets(t)

	messages := env.MustListMessages(MessageFilter{
		MatchEmptyLabel: true,
		Sender:          "alice@example.com",
	})

	if len(messages) != 2 {
		t.Errorf("expected 2 messages with MatchEmptyLabel + alice sender, got %d", len(messages))
	}

	foundMsg9 := false
	foundMsg7 := false
	for _, msg := range messages {
		if msg.Subject == "No Labels" {
			foundMsg9 = true
		}
		if msg.Subject == "No Recipients" {
			foundMsg7 = true
		}
	}
	if !foundMsg9 {
		t.Error("expected 'No Labels' (msg9) with MatchEmptyLabel + alice sender")
	}
	if !foundMsg7 {
		t.Error("expected 'No Recipients' (msg7) with MatchEmptyLabel + alice sender")
	}

	messages = env.MustListMessages(MessageFilter{
		MatchEmptyLabel:  true,
		MatchEmptySender: true,
	})

	if len(messages) != 1 {
		t.Errorf("expected 1 message with MatchEmptyLabel + MatchEmptySender, got %d", len(messages))
	}
	if len(messages) > 0 && messages[0].Subject != "No Sender" {
		t.Errorf("expected 'No Sender' message, got %q", messages[0].Subject)
	}

	messages = env.MustListMessages(MessageFilter{MatchEmptyLabel: true})

	if len(messages) != 4 {
		t.Errorf("expected 4 messages with no labels, got %d", len(messages))
	}
}

// =============================================================================
// RecipientName CRUD tests
// =============================================================================

func TestListMessages_RecipientNameFilter(t *testing.T) {
	env := newTestEnv(t)

	filter := MessageFilter{RecipientName: "Bob Jones"}
	messages := env.MustListMessages(filter)

	if len(messages) != 3 {
		t.Errorf("expected 3 messages to Bob Jones, got %d", len(messages))
	}
}

func TestListMessages_MatchEmptyRecipientName(t *testing.T) {
	env := newTestEnvWithEmptyBuckets(t)

	filter := MessageFilter{MatchEmptyRecipientName: true}
	messages := env.MustListMessages(filter)

	if len(messages) == 0 {
		t.Fatal("expected at least 1 message with empty recipient name, got 0")
	}
	found := false
	for _, m := range messages {
		if m.Subject == "No Recipients" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected 'No Recipients' message in results")
		for _, m := range messages {
			t.Logf("  got: %q", m.Subject)
		}
	}
}

func TestGetGmailIDsByFilter_RecipientName(t *testing.T) {
	env := newTestEnv(t)

	filter := MessageFilter{RecipientName: "Bob Jones"}
	ids, err := env.Engine.GetGmailIDsByFilter(env.Ctx, filter)
	if err != nil {
		t.Fatalf("GetGmailIDsByFilter: %v", err)
	}

	if len(ids) != 3 {
		t.Errorf("expected 3 gmail IDs for Bob Jones, got %d", len(ids))
	}
}

func TestMatchEmptyRecipientName_CombinedWithSender(t *testing.T) {
	env := newTestEnv(t)

	filter := MessageFilter{
		MatchEmptyRecipientName: true,
		Sender:                  "alice@example.com",
	}
	messages := env.MustListMessages(filter)

	if len(messages) != 0 {
		t.Errorf("expected 0 messages for MatchEmptyRecipientName+Sender, got %d", len(messages))
	}
}

func TestCombinedRecipientAndRecipientNameFilter(t *testing.T) {
	env := newTestEnv(t)

	filter := MessageFilter{
		Recipient:     "bob@company.org",
		RecipientName: "Bob Jones",
	}
	messages := env.MustListMessages(filter)

	if len(messages) != 3 {
		t.Errorf("expected 3 messages matching both Recipient+RecipientName for Bob, got %d", len(messages))
	}
}

func TestCombinedRecipientAndRecipientName_Mismatch(t *testing.T) {
	env := newTestEnv(t)

	filter := MessageFilter{
		Recipient:     "bob@company.org",
		RecipientName: "Alice Smith",
	}
	messages := env.MustListMessages(filter)

	if len(messages) != 0 {
		t.Errorf("expected 0 messages for mismatched Recipient+RecipientName, got %d", len(messages))
	}
}

func TestCombinedRecipientAndRecipientName_NoOvercount(t *testing.T) {
	env := newTestEnv(t)

	filter := MessageFilter{
		Recipient:     "bob@company.org",
		RecipientName: "Bob Jones",
	}
	messages := env.MustListMessages(filter)

	seen := make(map[int64]int)
	for _, m := range messages {
		seen[m.ID]++
	}
	for id, count := range seen {
		if count > 1 {
			t.Errorf("message ID %d returned %d times (expected once)", id, count)
		}
	}
}

func TestRecipientName_WithMatchEmptyRecipient(t *testing.T) {
	env := newTestEnv(t)

	filter := MessageFilter{
		RecipientName:       "Bob Jones",
		MatchEmptyRecipient: true,
	}

	messages := env.MustListMessages(filter)
	if len(messages) != 0 {
		t.Errorf("expected 0 messages for contradictory RecipientName+MatchEmptyRecipient, got %d", len(messages))
	}
}

func TestGetGmailIDsByFilter_RecipientName_WithMatchEmptyRecipient(t *testing.T) {
	env := newTestEnv(t)

	filter := MessageFilter{
		RecipientName:       "Bob Jones",
		MatchEmptyRecipient: true,
	}
	ids, err := env.Engine.GetGmailIDsByFilter(env.Ctx, filter)
	if err != nil {
		t.Fatalf("GetGmailIDsByFilter: %v", err)
	}
	if len(ids) != 3 {
		t.Errorf("expected 3 gmail IDs, got %d", len(ids))
	}
}

func TestRecipientAndRecipientNameAndMatchEmptyRecipient(t *testing.T) {
	env := newTestEnv(t)

	filter := MessageFilter{
		Recipient:           "bob@company.org",
		RecipientName:       "Bob Jones",
		MatchEmptyRecipient: true,
	}

	messages := env.MustListMessages(filter)
	if len(messages) != 3 {
		t.Errorf("ListMessages: expected 3 messages, got %d", len(messages))
	}

	rows, err := env.Engine.SubAggregate(env.Ctx, filter, ViewSenders, AggregateOptions{Limit: 100})
	if err != nil {
		t.Fatalf("SubAggregate: %v", err)
	}
	if len(rows) != 1 || rows[0].Key != "alice@example.com" {
		t.Errorf("SubAggregate: unexpected rows: %v", rows)
	}
}

// TestRecipientNameFilter_IncludesBCC verifies that RecipientName filter includes BCC recipients.
// Regression test for a bug where RecipientName only searched 'to' and 'cc' but not 'bcc'.
func TestRecipientNameFilter_IncludesBCC(t *testing.T) {
	tdb := dbtest.NewTestDB(t, "../store/schema.sql")

	sp := dbtest.StrPtr
	aliceID := tdb.AddParticipant(dbtest.ParticipantOpts{Email: sp("alice@example.com"), DisplayName: sp("Alice Sender"), Domain: "example.com"})
	bobID := tdb.AddParticipant(dbtest.ParticipantOpts{Email: sp("bob@example.com"), DisplayName: sp("Bob ToRecipient"), Domain: "example.com"})
	secretID := tdb.AddParticipant(dbtest.ParticipantOpts{Email: sp("secret@example.com"), DisplayName: sp("Secret Bob"), Domain: "example.com"})

	tdb.AddSource(dbtest.SourceOpts{Identifier: "test@gmail.com"})
	tdb.AddMessage(dbtest.MessageOpts{
		Subject: "Test Subject",
		SentAt:  "2024-01-15 10:00:00",
		FromID:  aliceID,
		ToIDs:   []int64{bobID},
		BccIDs:  []int64{secretID},
	})

	engine := NewSQLiteEngine(tdb.DB)
	ctx := context.Background()

	t.Run("ListMessages", func(t *testing.T) {
		messages, err := engine.ListMessages(ctx, MessageFilter{RecipientName: "Secret Bob"})
		if err != nil {
			t.Fatalf("ListMessages: %v", err)
		}
		if len(messages) != 1 {
			t.Errorf("expected 1 message, got %d", len(messages))
		}
	})

	t.Run("AggregateByRecipientName", func(t *testing.T) {
		rows, err := engine.AggregateByRecipientName(ctx, AggregateOptions{Limit: 100})
		if err != nil {
			t.Fatalf("AggregateByRecipientName: %v", err)
		}
		found := false
		for _, row := range rows {
			if row.Key == "Secret Bob" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected BCC recipient 'Secret Bob' in aggregate, got: %v", rows)
		}
	})

	t.Run("SubAggregate", func(t *testing.T) {
		rows, err := engine.SubAggregate(ctx, MessageFilter{RecipientName: "Secret Bob"}, ViewSenders, AggregateOptions{Limit: 100})
		if err != nil {
			t.Fatalf("SubAggregate: %v", err)
		}
		if len(rows) != 1 || rows[0].Key != "alice@example.com" {
			t.Errorf("expected sender Alice, got: %v", rows)
		}
	})

	t.Run("GetGmailIDsByFilter", func(t *testing.T) {
		ids, err := engine.GetGmailIDsByFilter(ctx, MessageFilter{RecipientName: "Secret Bob"})
		if err != nil {
			t.Fatalf("GetGmailIDsByFilter: %v", err)
		}
		if len(ids) != 1 {
			t.Errorf("expected 1 gmail ID, got: %v", ids)
		}
	})

	t.Run("Recipient_email_also_finds_BCC", func(t *testing.T) {
		messages, err := engine.ListMessages(ctx, MessageFilter{Recipient: "secret@example.com"})
		if err != nil {
			t.Fatalf("ListMessages: %v", err)
		}
		if len(messages) != 1 {
			t.Errorf("expected 1 message, got %d", len(messages))
		}
	})
}
