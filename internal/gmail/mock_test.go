package gmail

import "testing"

func TestSetupMessages_NilEntries(t *testing.T) {
	mock := NewMockAPI()

	msg1 := &RawMessage{ID: "msg1", Raw: []byte("test1")}
	msg2 := &RawMessage{ID: "msg2", Raw: []byte("test2")}

	// Should not panic when nil entries are present
	mock.SetupMessages(msg1, nil, msg2, nil)

	if len(mock.Messages) != 2 {
		t.Errorf("expected 2 messages, got %d", len(mock.Messages))
	}
	if mock.Messages["msg1"] != msg1 {
		t.Error("msg1 not stored correctly")
	}
	if mock.Messages["msg2"] != msg2 {
		t.Error("msg2 not stored correctly")
	}
}

func TestSetupMessages_UninitializedMap(t *testing.T) {
	// Create mock without using constructor (simulates uninitialized map)
	mock := &MockAPI{}

	msg := &RawMessage{ID: "msg1", Raw: []byte("test")}

	// Should not panic when Messages map is nil
	mock.SetupMessages(msg)

	if len(mock.Messages) != 1 {
		t.Errorf("expected 1 message, got %d", len(mock.Messages))
	}
	if mock.Messages["msg1"] != msg {
		t.Error("msg1 not stored correctly")
	}
}

func TestSetupMessages_AllNil(t *testing.T) {
	mock := NewMockAPI()

	// Should not panic when all entries are nil
	mock.SetupMessages(nil, nil, nil)

	if len(mock.Messages) != 0 {
		t.Errorf("expected 0 messages, got %d", len(mock.Messages))
	}
}

func TestSetupMessages_Empty(t *testing.T) {
	mock := NewMockAPI()

	// Should handle empty call gracefully
	mock.SetupMessages()

	if len(mock.Messages) != 0 {
		t.Errorf("expected 0 messages, got %d", len(mock.Messages))
	}
}
