package main

import (
	"os"

	"github.com/wesm/msgvault/tools/devdata/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
