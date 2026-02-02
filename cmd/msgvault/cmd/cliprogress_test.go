package cmd

import (
	"testing"
	"time"
)

func TestCLIProgress_OnLatestDateBeforeOnStart(t *testing.T) {
	p := &CLIProgress{}
	p.OnLatestDate(time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC))

	if p.startTime.IsZero() {
		t.Fatal("startTime should be initialized when OnLatestDate is called before OnStart")
	}
	if time.Since(p.startTime) > time.Second {
		t.Fatalf("startTime should be recent, got %v ago", time.Since(p.startTime))
	}
}

func TestCLIProgress_OnProgressBeforeOnStart(t *testing.T) {
	p := &CLIProgress{}
	p.OnProgress(10, 5, 3)

	if p.startTime.IsZero() {
		t.Fatal("startTime should be initialized when OnProgress is called before OnStart")
	}
	if time.Since(p.startTime) > time.Second {
		t.Fatalf("startTime should be recent, got %v ago", time.Since(p.startTime))
	}
}

func TestCLIProgress_OnStartResetsForReuse(t *testing.T) {
	p := &CLIProgress{}
	p.OnStart(100)
	first := p.startTime

	time.Sleep(5 * time.Millisecond)
	p.OnStart(200)

	if !p.startTime.After(first) {
		t.Fatal("OnStart should reset startTime on subsequent calls")
	}
}
