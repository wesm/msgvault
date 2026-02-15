package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"

	"github.com/wesm/msgvault/cmd/msgvault/cmd"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := cmd.ExecuteContext(ctx); err != nil {
		if errors.Is(err, context.Canceled) && ctx.Err() == context.Canceled {
			os.Exit(130)
		}
		os.Exit(1)
	}
}
