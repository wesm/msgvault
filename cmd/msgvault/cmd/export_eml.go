package cmd

import (
	"fmt"
	"os"
	"strconv"

	"github.com/spf13/cobra"
	"github.com/wesm/msgvault/internal/fileutil"
	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/store"
)

var (
	exportEMLOutput string
)

var exportEMLCmd = &cobra.Command{
	Use:   "export-eml <id>",
	Short: "Export a message as .eml file",
	Long: `Export a message from the archive as a standard .eml (MIME) file.

This command retrieves the raw MIME data stored during sync and writes it
to a file. The .eml format is compatible with most email clients.

Examples:
  msgvault export-eml 12345
  msgvault export-eml 12345 --output message.eml
  msgvault export-eml 18f0abc123def -o important.eml`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		idStr := args[0]

		// Open database
		dbPath := cfg.DatabaseDSN()
		s, err := store.Open(dbPath)
		if err != nil {
			return fmt.Errorf("open database: %w", err)
		}
		defer s.Close()

		// Create query engine to look up the message
		engine := query.NewSQLiteEngine(s.DB())

		// Try to parse as numeric ID first
		var msgID int64
		var sourceMessageID string
		if id, err := strconv.ParseInt(idStr, 10, 64); err == nil {
			msgID = id
			// Get source message ID for output filename
			msg, err := engine.GetMessage(cmd.Context(), id)
			if err != nil {
				return fmt.Errorf("get message: %w", err)
			}
			if msg == nil {
				return fmt.Errorf("message not found: %s", idStr)
			}
			sourceMessageID = msg.SourceMessageID
		} else {
			// It's a source message ID (Gmail ID)
			sourceMessageID = idStr
			msg, err := engine.GetMessageBySourceID(cmd.Context(), idStr)
			if err != nil {
				return fmt.Errorf("get message: %w", err)
			}
			if msg == nil {
				return fmt.Errorf("message not found: %s", idStr)
			}
			msgID = msg.ID
		}

		// Get the raw MIME data
		rawData, err := s.GetMessageRaw(msgID)
		if err != nil {
			return fmt.Errorf("get raw message data: %w (message may not have raw data stored)", err)
		}

		// Determine output filename
		outputPath := exportEMLOutput
		if outputPath == "" {
			outputPath = fmt.Sprintf("%s.eml", sourceMessageID)
		}

		// Write to file or stdout
		if outputPath == "-" {
			_, err = os.Stdout.Write(rawData)
			return err
		}

		err = fileutil.SecureWriteFile(outputPath, rawData, 0600) // Restricted permissions for email content
		if err != nil {
			return fmt.Errorf("write file: %w", err)
		}

		fmt.Printf("Exported message to: %s (%d bytes)\n", outputPath, len(rawData))
		return nil
	},
}

func init() {
	rootCmd.AddCommand(exportEMLCmd)
	exportEMLCmd.Flags().StringVarP(&exportEMLOutput, "output", "o", "", "Output file path (default: <gmail_id>.eml, use - for stdout)")
}
