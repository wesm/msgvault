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
	mockAPI, ctx := setupDeletionMockTest(t)

	// Dirty all trackable fields
	mockAPI.TrashErrors["msg1"] = errors.New("error")
	_ = mockAPI.TrashMessage(ctx, "msg1")
	_ = mockAPI.DeleteMessage(ctx, "msg2")
	_ = mockAPI.BatchDeleteMessages(ctx, []string{"msg3"})
	mockAPI.BeforeTrash = func(string) error { return nil }
	mockAPI.BeforeDelete = func(string) error { return nil }
	mockAPI.BeforeBatchDelete = func([]string) error { return nil }

	mockAPI.Reset()

	if len(mockAPI.TrashErrors) != 0 {
		t.Error("TrashErrors not cleared")
	}
	if len(mockAPI.TrashCalls) != 0 {
		t.Error("TrashCalls not cleared")
	}
	if len(mockAPI.DeleteCalls) != 0 {
		t.Error("DeleteCalls not cleared")
	}
	if len(mockAPI.BatchDeleteCalls) != 0 {
		t.Error("BatchDeleteCalls not cleared")
	}
	if len(mockAPI.CallSequence) != 0 {
		t.Error("CallSequence not cleared")
	}
	if mockAPI.BeforeTrash != nil {
		t.Error("BeforeTrash not cleared")
	}
	if mockAPI.BeforeDelete != nil {
		t.Error("BeforeDelete not cleared")
	}
	if mockAPI.BeforeBatchDelete != nil {
		t.Error("BeforeBatchDelete not cleared")
	}
}

func TestDeletionMockAPI_GetCallCount(t *testing.T) {
	mockAPI, ctx := setupDeletionMockTest(t)

	_ = mockAPI.TrashMessage(ctx, "msg1")
	_ = mockAPI.TrashMessage(ctx, "msg1")
	_ = mockAPI.TrashMessage(ctx, "msg2")

	tests := []struct {
		msgID string
		want  int
	}{
		{"msg1", 2},
		{"msg2", 1},
		{"msg3", 0},
	}

	for _, tt := range tests {
		if got := mockAPI.GetTrashCallCount(tt.msgID); got != tt.want {
			t.Errorf("GetTrashCallCount(%q) = %d, want %d", tt.msgID, got, tt.want)
		}
	}
}

func TestDeletionMockAPI_Close(t *testing.T) {
	mockAPI, _ := setupDeletionMockTest(t)
	if err := mockAPI.Close(); err != nil {
		t.Errorf("Close() error = %v, want nil", err)
	}
}

func TestDeletionMockAPI_Hooks(t *testing.T) {
	tests := []struct {
		name      string
		setupHook func(*DeletionMockAPI)
		act       func(context.Context, *DeletionMockAPI) error
		wantErr   bool
	}{
		{
			name:      "BeforeTrash allow",
			setupHook: func(m *DeletionMockAPI) { m.BeforeTrash = func(string) error { return nil } },
			act:       func(ctx context.Context, m *DeletionMockAPI) error { return m.TrashMessage(ctx, "msg1") },
			wantErr:   false,
		},
		{
			name:      "BeforeTrash block",
			setupHook: func(m *DeletionMockAPI) { m.BeforeTrash = func(string) error { return errors.New("blocked") } },
			act:       func(ctx context.Context, m *DeletionMockAPI) error { return m.TrashMessage(ctx, "msg1") },
			wantErr:   true,
		},
		{
			name:      "BeforeDelete allow",
			setupHook: func(m *DeletionMockAPI) { m.BeforeDelete = func(string) error { return nil } },
			act:       func(ctx context.Context, m *DeletionMockAPI) error { return m.DeleteMessage(ctx, "msg1") },
			wantErr:   false,
		},
		{
			name:      "BeforeDelete block",
			setupHook: func(m *DeletionMockAPI) { m.BeforeDelete = func(string) error { return errors.New("blocked") } },
			act:       func(ctx context.Context, m *DeletionMockAPI) error { return m.DeleteMessage(ctx, "msg1") },
			wantErr:   true,
		},
		{
			name:      "BeforeBatchDelete allow",
			setupHook: func(m *DeletionMockAPI) { m.BeforeBatchDelete = func([]string) error { return nil } },
			act: func(ctx context.Context, m *DeletionMockAPI) error {
				return m.BatchDeleteMessages(ctx, []string{"msg1", "msg2"})
			},
			wantErr: false,
		},
		{
			name:      "BeforeBatchDelete block",
			setupHook: func(m *DeletionMockAPI) { m.BeforeBatchDelete = func([]string) error { return errors.New("blocked") } },
			act: func(ctx context.Context, m *DeletionMockAPI) error {
				return m.BatchDeleteMessages(ctx, []string{"msg1", "msg2"})
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockAPI, ctx := setupDeletionMockTest(t)
			tt.setupHook(mockAPI)
			err := tt.act(ctx, mockAPI)
			if (err != nil) != tt.wantErr {
				t.Errorf("error = %v, wantErr %v", err, tt.wantErr)
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
