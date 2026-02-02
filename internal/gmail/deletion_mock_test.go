package gmail

import (
	"context"
	"errors"
	"testing"
)

func setupDeletionMockTest(t *testing.T) (*DeletionMockAPI, context.Context) {
	t.Helper()
	return NewDeletionMockAPI(), context.Background()
}

func assertCallSequence(t *testing.T, mock *DeletionMockAPI, expectedOps ...string) {
	t.Helper()
	if len(mock.CallSequence) != len(expectedOps) {
		t.Fatalf("CallSequence length = %d, want %d", len(mock.CallSequence), len(expectedOps))
	}
	for i, want := range expectedOps {
		if got := mock.CallSequence[i].Operation; got != want {
			t.Errorf("CallSequence[%d].Operation = %q, want %q", i, got, want)
		}
	}
}

func TestDeletionMockAPI_CallSequence(t *testing.T) {
	mockAPI, ctx := setupDeletionMockTest(t)

	_ = mockAPI.TrashMessage(ctx, "msg1")
	_ = mockAPI.DeleteMessage(ctx, "msg2")
	_ = mockAPI.BatchDeleteMessages(ctx, []string{"msg3", "msg4"})

	assertCallSequence(t, mockAPI, "trash", "delete", "batch_delete")
}

func TestDeletionMockAPI_Reset(t *testing.T) {
	mockAPI, _ := setupDeletionMockTest(t)
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
	mockAPI, ctx := setupDeletionMockTest(t)

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
	mockAPI, _ := setupDeletionMockTest(t)
	if err := mockAPI.Close(); err != nil {
		t.Errorf("Close() error = %v, want nil", err)
	}
}

func TestDeletionMockAPI_Hooks(t *testing.T) {
	t.Run("BeforeTrash allows and blocks", func(t *testing.T) {
		mockAPI, ctx := setupDeletionMockTest(t)

		hookCalled := false
		mockAPI.BeforeTrash = func(messageID string) error {
			hookCalled = true
			if messageID == "blocked" {
				return errors.New("blocked by hook")
			}
			return nil
		}

		err := mockAPI.TrashMessage(ctx, "msg1")
		if err != nil {
			t.Errorf("TrashMessage(msg1) error = %v", err)
		}
		if !hookCalled {
			t.Error("BeforeTrash hook was not called")
		}

		err = mockAPI.TrashMessage(ctx, "blocked")
		if err == nil {
			t.Error("TrashMessage(blocked) should error")
		}
	})

	tests := []struct {
		name      string
		setupHook func(*DeletionMockAPI)
		act       func(context.Context, *DeletionMockAPI) error
	}{
		{
			name:      "BeforeDelete",
			setupHook: func(m *DeletionMockAPI) { m.BeforeDelete = func(string) error { return errors.New("hook error") } },
			act:       func(ctx context.Context, m *DeletionMockAPI) error { return m.DeleteMessage(ctx, "msg1") },
		},
		{
			name:      "BeforeBatchDelete",
			setupHook: func(m *DeletionMockAPI) { m.BeforeBatchDelete = func([]string) error { return errors.New("hook error") } },
			act:       func(ctx context.Context, m *DeletionMockAPI) error { return m.BatchDeleteMessages(ctx, []string{"msg1", "msg2"}) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name+" blocks", func(t *testing.T) {
			mockAPI, ctx := setupDeletionMockTest(t)
			tt.setupHook(mockAPI)
			if err := tt.act(ctx, mockAPI); err == nil {
				t.Error("expected hook error")
			}
		})
	}
}

func TestDeletionMockAPI_GetDeleteCallCount(t *testing.T) {
	mockAPI, ctx := setupDeletionMockTest(t)

	_ = mockAPI.DeleteMessage(ctx, "msg1")
	_ = mockAPI.DeleteMessage(ctx, "msg1")

	if mockAPI.GetDeleteCallCount("msg1") != 2 {
		t.Errorf("GetDeleteCallCount(msg1) = %d, want 2", mockAPI.GetDeleteCallCount("msg1"))
	}
}

func TestDeletionMockAPI_TransientFailures(t *testing.T) {
	tests := []struct {
		name       string
		failCount  int
		isTrash    bool
		callMethod func(context.Context, *DeletionMockAPI) error
	}{
		{
			name:       "TrashTransientFailure",
			failCount:  2,
			isTrash:    true,
			callMethod: func(ctx context.Context, m *DeletionMockAPI) error { return m.TrashMessage(ctx, "msg1") },
		},
		{
			name:       "DeleteTransientFailure",
			failCount:  1,
			isTrash:    false,
			callMethod: func(ctx context.Context, m *DeletionMockAPI) error { return m.DeleteMessage(ctx, "msg1") },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockAPI, ctx := setupDeletionMockTest(t)
			mockAPI.SetTransientFailure("msg1", tt.failCount, tt.isTrash)

			for i := 0; i < tt.failCount; i++ {
				if err := tt.callMethod(ctx, mockAPI); err == nil {
					t.Errorf("call %d should fail", i+1)
				}
			}

			if err := tt.callMethod(ctx, mockAPI); err != nil {
				t.Errorf("call after failures should succeed, got: %v", err)
			}
		})
	}
}
