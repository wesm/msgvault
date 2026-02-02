package cmd

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

var (
	exportAttachmentOutput string
	exportAttachmentJSON   bool
	exportAttachmentBase64 bool
)

var exportAttachmentCmd = &cobra.Command{
	Use:   "export-attachment <content-hash>",
	Short: "Export an attachment by content hash",
	Long: `Export an attachment binary by its SHA-256 content hash.

Get the content hash from 'show-message --json':
  msgvault show-message 45 --json | jq '.attachments[0].content_hash'

Examples:
  msgvault export-attachment 61ccf192b5bd358738802dc2676d3ceab856f47d26dd29681ac3d335bfd5bbd0
  msgvault export-attachment 61ccf192... --output invoice.pdf

Export all attachments from a message with original filenames:
  msgvault show-message 45 --json | \
    jq -r '.attachments[] | "\(.content_hash)\t\(.filename)"' | \
    while IFS=$'\t' read -r hash name; do
      msgvault export-attachment "$hash" -o "$name"
    done
  msgvault export-attachment 61ccf192... -o -       # stdout (binary)
  msgvault export-attachment 61ccf192... --base64  # stdout (base64)
  msgvault export-attachment 61ccf192... --json    # JSON with base64 data`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		contentHash := args[0]

		// Validate hash format (64 hex characters = SHA-256)
		if len(contentHash) != 64 {
			return fmt.Errorf("invalid content hash: must be 64 hex characters (SHA-256), got %d", len(contentHash))
		}
		if _, err := hex.DecodeString(contentHash); err != nil {
			return fmt.Errorf("invalid content hash: must be hex characters: %w", err)
		}

		// Construct storage path: attachmentsDir/hash[:2]/hash
		attachmentsDir := cfg.AttachmentsDir()
		storagePath := filepath.Join(attachmentsDir, contentHash[:2], contentHash)

		// Read attachment file
		data, err := os.ReadFile(storagePath)
		if err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("attachment not found: no file for hash %s", contentHash)
			}
			return fmt.Errorf("read attachment: %w", err)
		}

		// Output based on flags
		if exportAttachmentJSON {
			output := map[string]any{
				"content_hash": contentHash,
				"size":         len(data),
				"data_base64":  base64.StdEncoding.EncodeToString(data),
			}
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			return enc.Encode(output)
		}

		if exportAttachmentBase64 {
			fmt.Println(base64.StdEncoding.EncodeToString(data))
			return nil
		}

		// Binary output
		outputPath := exportAttachmentOutput
		if outputPath == "" || outputPath == "-" {
			_, err = os.Stdout.Write(data)
			return err
		}

		// Write to file
		err = os.WriteFile(outputPath, data, 0600)
		if err != nil {
			return fmt.Errorf("write file: %w", err)
		}

		fmt.Fprintf(os.Stderr, "Exported attachment to: %s (%d bytes)\n", outputPath, len(data))
		return nil
	},
}

func init() {
	rootCmd.AddCommand(exportAttachmentCmd)
	exportAttachmentCmd.Flags().StringVarP(&exportAttachmentOutput, "output", "o", "", "Output file path (default: stdout, use - for stdout)")
	exportAttachmentCmd.Flags().BoolVar(&exportAttachmentJSON, "json", false, "Output as JSON with base64-encoded data")
	exportAttachmentCmd.Flags().BoolVar(&exportAttachmentBase64, "base64", false, "Output raw base64 to stdout")
}
