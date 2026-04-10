package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

var completionCmd = &cobra.Command{
	Use:   "completion [bash|zsh|fish|powershell]",
	Short: "Generate shell completion script",
	Long: `Generate a shell completion script for msgvault.

To load completions:

Bash:
  $ source <(msgvault completion bash)
  # To load completions for each session, execute once:
  # Linux:
  $ msgvault completion bash > /etc/bash_completion.d/msgvault
  # macOS:
  $ msgvault completion bash > $(brew --prefix)/etc/bash_completion.d/msgvault

Zsh:
  # If shell completion is not already enabled in your environment,
  # enable it by executing the following once:
  $ echo "autoload -U compinit; compinit" >> ~/.zshrc
  # To load completions for each session, execute once:
  $ msgvault completion zsh > "${fpath[1]}/_msgvault"

Fish:
  $ msgvault completion fish > ~/.config/fish/completions/msgvault.fish

PowerShell:
  PS> msgvault completion powershell | Out-String | Invoke-Expression
`,
	DisableFlagsInUseLine: true,
	ValidArgs:             []string{"bash", "zsh", "fish", "powershell"},
	Args:                  cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
	RunE: func(cmd *cobra.Command, args []string) error {
		switch args[0] {
		case "bash":
			return cmd.Root().GenBashCompletionV2(os.Stdout, true)
		case "zsh":
			return cmd.Root().GenZshCompletion(os.Stdout)
		case "fish":
			return cmd.Root().GenFishCompletion(os.Stdout, true)
		case "powershell":
			return cmd.Root().GenPowerShellCompletionWithDesc(os.Stdout)
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(completionCmd)
}
