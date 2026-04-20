package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	embedFullRebuild bool
	embedYes         bool
)

var embedCmd = &cobra.Command{
	Use:     "build-embeddings",
	Aliases: []string{"embed"},
	Short:   "Build or update the vector embedding index (incremental by default; --full-rebuild for a new generation)",
	Long: `Build or update the vector embedding index for hybrid search.
Writes vectors to the co-located vectors.db. In the default incremental
mode, the command drains any pending rows in the active generation. With
--full-rebuild, it creates a new building generation, embeds the entire
corpus, and (on a clean completion) atomically activates it.

Requires [vector] to be enabled in config.toml and [vector.embeddings]
to point at a running OpenAI-compatible endpoint.

"embed" is accepted as an alias for backward compatibility.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if !cfg.Vector.Enabled {
			return fmt.Errorf("vector search not enabled; add [vector] enabled=true to config.toml first")
		}
		if cfg.Vector.Embeddings.Endpoint == "" || cfg.Vector.Embeddings.Model == "" {
			return fmt.Errorf("[vector.embeddings] endpoint and model are required")
		}
		return runEmbed(cmd.Context())
	},
}

func init() {
	rootCmd.AddCommand(embedCmd)
	embedCmd.Flags().BoolVar(&embedFullRebuild, "full-rebuild", false, "Create a new generation and rebuild from scratch")
	embedCmd.Flags().BoolVar(&embedYes, "yes", false, "Skip confirmation prompts")
}
