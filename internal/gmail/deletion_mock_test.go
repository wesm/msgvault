package gmail

import (
	"context"
	"errors"
	"testing"
)

func TestDeletionMockAPI_CallSequence(t *testing.T) {
	mockAPI := NewDeletionMockAPI()

	ctx := context.Background()
	_ = mockAPI.TrashMessage(ctx, "msg1")
	_ = mockAPI.DeleteMessage(ctx, "msg2")
	_ = mockAPI.BatchDeleteMessages(ctx, []string{"msg3", "msg4"})

	if len(mockAPI.CallSequence) != 3 {
		t.Fatalf("CallSequence length = %d, want 3", len(mockAPI.CallSequence))
	}

	if mockAPI.CallSequence[0].Operation != "trash" {
		t.Errorf("CallSequence[0].Operation = %q, want %q", mockAPI.CallSequence[0].Operation, "trash")
	}
	if mockAPI.CallSequence[1].Operation != "delete" {
		t.Errorf("CallSequence[1].Operation = %q, want %q", mockAPI.CallSequence[1].Operation, "delete")
	}
	if mockAPI.CallSequence[2].Operation != "batch_delete" {
		t.Errorf("CallSequence[2].Operation = %q, want %q", mockAPI.CallSequence[2].Operation, "batch_delete")
	}
}

func TestDeletionMockAPI_Reset(t *testing.T) {
	mockAPI := NewDeletionMockAPI()
	mockAPI.TrashErrors["msg1"] = errors.New("error")
	mockAPI.TrashCalls = []string{"msg1"}

	mockAPI.Reset()

	if len(mockAPI.TrashErrors) != 0 {
		t.Errorf("TrashErrors not cleared")
	}
	if len(mockAPI.TrashCalls) != 0 {
		t.Errorf("TrashCalls not cleared")
	}
}

func TestDeletionMockAPI_GetCallCount(t *testing.T) {
	mockAPI := NewDeletionMockAPI()
	ctx := context.Background()

	_ = mockAPI.TrashMessage(ctx, "msg1")
	_ = mockAPI.TrashMessage(ctx, "msg1")
	_ = mockAPI.TrashMessage(ctx, "msg2")

	if mockAPI.GetTrashCallCount("msg1") != 2 {
		t.Errorf("GetTrashCallCount(msg1) = %d, want 2", mockAPI.GetTrashCallCount("msg1"))
	}
	if mockAPI.GetTrashCallCount("msg2") != 1 {
		t.Errorf("GetTrashCallCount(msg2) = %d, want 1", mockAPI.GetTrashCallCount("msg2"))
	}
	if mockAPI.GetTrashCallCount("msg3") != 0 {
		t.Errorf("GetTrashCallCount(msg3) = %d, want 0", mockAPI.GetTrashCallCount("msg3"))
	}
}

func TestDeletionMockAPI_Close(t *testing.T) {
	mockAPI := NewDeletionMockAPI()
	if err := mockAPI.Close(); err != nil {
		t.Errorf("Close() error = %v, want nil", err)
	}
}

func TestDeletionMockAPI_SetTransientFailure(t *testing.T) {
	mockAPI := NewDeletionMockAPI()
	ctx := context.Background()

	// Set msg1 to fail 2 times then succeed
	mockAPI.SetTransientFailure("msg1", 2, true)

	// First call: fails
	err := mockAPI.TrashMessage(ctx, "msg1")
	if err == nil {
		t.Error("First call should fail")
	}

	// Second call: fails
	err = mockAPI.TrashMessage(ctx, "msg1")
	if err == nil {
		t.Error("Second call should fail")
	}

	// Third call: succeeds
	err = mockAPI.TrashMessage(ctx, "msg1")
	if err != nil {
		t.Errorf("Third call should succeed, got error: %v", err)
	}
}

func TestDeletionMockAPI_Hooks(t *testing.T) {
	mockAPI := NewDeletionMockAPI()
	ctx := context.Background()

	hookCalled := false
	mockAPI.BeforeTrash = func(messageID string) error {
		hookCalled = true
		if messageID == "blocked" {
			return errors.New("blocked by hook")
		}
		return nil
	}

	// Normal call - hook allows it
	err := mockAPI.TrashMessage(ctx, "msg1")
	if err != nil {
		t.Errorf("TrashMessage(msg1) error = %v", err)
	}
	if !hookCalled {
		t.Error("BeforeTrash hook was not called")
	}

	// Blocked call
	err = mockAPI.TrashMessage(ctx, "blocked")
	if err == nil {
		t.Error("TrashMessage(blocked) should error")
	}
}

func TestDeletionMockAPI_BeforeDeleteHook(t *testing.T) {
	mockAPI := NewDeletionMockAPI()
	ctx := context.Background()

	mockAPI.BeforeDelete = func(messageID string) error {
		return errors.New("delete hook error")
	}

	err := mockAPI.DeleteMessage(ctx, "msg1")
	if err == nil {
		t.Error("DeleteMessage should fail with hook error")
	}
}

func TestDeletionMockAPI_BeforeBatchDeleteHook(t *testing.T) {
	mockAPI := NewDeletionMockAPI()
	ctx := context.Background()

	mockAPI.BeforeBatchDelete = func(ids []string) error {
		return errors.New("batch hook error")
	}

	err := mockAPI.BatchDeleteMessages(ctx, []string{"msg1", "msg2"})
	if err == nil {
		t.Error("BatchDeleteMessages should fail with hook error")
	}
}

func TestDeletionMockAPI_GetDeleteCallCount(t *testing.T) {
	mockAPI := NewDeletionMockAPI()
	ctx := context.Background()

	_ = mockAPI.DeleteMessage(ctx, "msg1")
	_ = mockAPI.DeleteMessage(ctx, "msg1")

	if mockAPI.GetDeleteCallCount("msg1") != 2 {
		t.Errorf("GetDeleteCallCount(msg1) = %d, want 2", mockAPI.GetDeleteCallCount("msg1"))
	}
}

func TestDeletionMockAPI_SetTransientDeleteFailure(t *testing.T) {
	mockAPI := NewDeletionMockAPI()
	ctx := context.Background()

	// Set msg1 to fail 1 time then succeed (for delete, not trash)
	mockAPI.SetTransientFailure("msg1", 1, false)

	// First call: fails
	err := mockAPI.DeleteMessage(ctx, "msg1")
	if err == nil {
		t.Error("First delete call should fail")
	}

	// Second call: succeeds
	err = mockAPI.DeleteMessage(ctx, "msg1")
	if err != nil {
		t.Errorf("Second delete call should succeed, got error: %v", err)
	}
}
