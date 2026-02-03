package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/wesm/msgvault/cmd/msgvault/cmd"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := cmd.ExecuteContext(ctx); err != nil {
		os.Exit(1)
	}
}
