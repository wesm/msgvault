package cmd

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/internal/chat"
	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/store"
)

var (
	chatServer     string
	chatModel      string
	chatMaxResults int
	chatForceSQL   bool
)

var chatCmd = &cobra.Command{
	Use:   "chat",
	Short: "Chat with your email archive using a local LLM",
	Long: `Start an interactive chat session that uses a local LLM (via Ollama) to
answer questions about your email archive.

The chat command uses RAG (retrieval-augmented generation) to:
  1. Parse your natural language question into a structured search query
  2. Retrieve relevant emails from the archive
  3. Generate an answer based on the retrieved emails

Requires Ollama running with a model available.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Resolve config: flags override config file
		server := chatServer
		if server == "" {
			server = cfg.Chat.Server
		}
		model := chatModel
		if model == "" {
			model = cfg.Chat.Model
		}
		maxResults := chatMaxResults
		if maxResults == 0 {
			maxResults = cfg.Chat.MaxResults
		}

		// Open database
		dbPath := cfg.DatabasePath()
		s, err := store.Open(dbPath)
		if err != nil {
			return fmt.Errorf("open database: %w", err)
		}
		defer s.Close()

		// Initialize query engine (same pattern as tui.go)
		var engine query.Engine
		analyticsDir := cfg.AnalyticsDir()

		if !chatForceSQL && query.HasParquetData(analyticsDir) {
			duckEngine, err := query.NewDuckDBEngine(analyticsDir, dbPath, s.DB())
			if err != nil {
				fmt.Fprintf(os.Stderr, "Warning: Failed to open Parquet engine: %v\n", err)
				fmt.Fprintf(os.Stderr, "Falling back to SQLite\n")
				engine = query.NewSQLiteEngine(s.DB())
			} else {
				engine = duckEngine
				defer duckEngine.Close()
			}
		} else {
			engine = query.NewSQLiteEngine(s.DB())
		}

		// Create LLM client
		llm, err := chat.NewOllamaClient(server, model)
		if err != nil {
			return fmt.Errorf("create ollama client: %w", err)
		}

		// Create session
		session := chat.NewSession(engine, llm, chat.Config{
			Model:      model,
			MaxResults: maxResults,
			MaxBodyLen: 2000,
		})

		// REPL loop — Ctrl+C cancels the current request, not the session.
		// We create a fresh context per request so Ctrl+C during generation
		// doesn't poison subsequent questions.
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt)
		defer signal.Stop(sigCh)

		fmt.Printf("msgvault chat (model: %s, server: %s)\n", model, server)
		fmt.Println("Type your question, or 'quit' to exit. Ctrl+C cancels current request.")
		fmt.Println()

		scanner := bufio.NewScanner(os.Stdin)
		for {
			fmt.Print("> ")
			if !scanner.Scan() {
				break // EOF or Ctrl-D
			}
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			if line == "quit" || line == "exit" {
				break
			}

			// Per-request context: cancelled by Ctrl+C or when request completes
			reqCtx, reqCancel := context.WithCancel(context.Background())
			go func() {
				select {
				case <-sigCh:
					reqCancel()
				case <-reqCtx.Done():
				}
			}()

			if err := session.Ask(reqCtx, line); err != nil {
				if reqCtx.Err() != nil {
					fmt.Fprintln(os.Stderr, "\n(cancelled)")
				} else {
					fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				}
			}
			reqCancel()
			fmt.Println()
		}

		if err := scanner.Err(); err != nil {
			return fmt.Errorf("read input: %w", err)
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(chatCmd)
	chatCmd.Flags().StringVar(&chatServer, "server", "", "Ollama server URL (default from config or http://localhost:11434)")
	chatCmd.Flags().StringVar(&chatModel, "model", "", "Model name (default from config or gpt-oss-128k)")
	chatCmd.Flags().IntVar(&chatMaxResults, "max-results", 0, "Max messages to retrieve per query (default from config or 20)")
	chatCmd.Flags().BoolVar(&chatForceSQL, "force-sql", false, "Force SQLite queries instead of Parquet")
}
