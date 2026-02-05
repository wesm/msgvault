# Security Review Bot

This repository uses an automated security review bot powered by Claude 4.5 Sonnet to review all pull requests from external contributors.

## Purpose

Since msgvault handles highly sensitive data (20+ years of personal email, OAuth tokens, attachments), we maintain strict security standards. This bot provides:

- **Consistent baseline security review** for all external contributions
- **Early detection** of common security issues before human review
- **Supply chain monitoring** for dependency changes (go.mod/go.sum)
- **Workflow tamper detection** for .github/ directory changes

**Important:** This bot supplements, but does not replace, human security review.

## Setup

### 1. Get Anthropic API Key

1. Sign up at https://console.anthropic.com/
2. Add a payment method (pay-as-you-go)
3. Generate an API key from the dashboard
4. **Optional:** Set spending limits to control costs

### 2. Add API Key to GitHub Secrets

1. Go to your repository's **Settings** > **Secrets and variables** > **Actions**
2. Click **New repository secret**
3. Name: `ANTHROPIC_API_KEY`
4. Value: Your API key from step 1
5. Click **Add secret**

### 3. That's it!

The workflow will automatically run on all new PRs from external contributors.

## Trusted Contributors

PRs from trusted contributors (owners/maintainers) bypass the automated review to:
- Save API costs
- Speed up internal development
- Avoid noise on PRs from experienced maintainers

### Managing the Trusted List

Edit `.github/trusted-contributors.json`:

```json
{
  "trusted_github_usernames": [
    "wesm"
  ]
}
```

**When to add someone:**
- They're a repository owner/maintainer
- They have write access to the repository
- They have a proven track record with security

## What the Bot Reviews

### High Priority
- Hardcoded secrets, API keys, OAuth tokens
- Command injection via `exec.Command` or `os/exec`
- SQL injection in SQLite or DuckDB queries
- Path traversal in file operations (attachments, config, database)
- Weakened file permissions on tokens or database
- Unauthorized Gmail API operations (especially deletion)
- Use of Go `unsafe` package
- Workflow/CI tampering (.github/ changes)
- Dependency version downgrades (supply chain attack)

### Medium Priority
- New dependencies (trustworthiness evaluation)
- CGO-related changes (SQLite, DuckDB native bindings)
- Input validation issues
- Error messages leaking sensitive data
- Improper OAuth token handling

### Low Priority (not posted, logged only)
- Security documentation gaps
- Insecure default configurations
- Minor code quality issues with security implications

## How It Works

1. **Trigger:** PR opened/updated from non-trusted contributor
2. **Fetch:** Bot retrieves the full PR diff (from base branch, never checks out PR code)
3. **Classify:** Files categorized (workflow, dependency, sensitive, other)
4. **Analyze:** Claude reviews changes with msgvault security context
5. **Report:** Bot posts inline comments on specific issues
6. **Summary:** Bot posts overall summary comment

## Cost Monitoring

### Expected Costs

**Typical usage:**
- ~10 external PRs per month
- ~$0.05-0.15 per review
- **Total: $1-2/month**

### Monitoring

1. View usage at https://console.anthropic.com/
2. Check the **Usage** tab for daily/monthly costs
3. Set spending limits under **Settings** > **Limits**

## Prompt Injection Protection

### Multi-Layered Defense

1. **Explicit warnings in prompt** - Claude is told the diff contains untrusted content
2. **XML delimiters** - Untrusted content wrapped in `<untrusted_pull_request_diff>` tags
3. **Reinforced instructions after untrusted content** - Critical instructions repeated after the diff
4. **Pattern detection** - Bot scans for 14+ common injection patterns before sending to Claude
5. **Strict JSON validation** - Type checking, bounds validation, path traversal checks, 50-issue cap

### Limitations

Prompt injection defense is not perfect. Human review is still required. Don't blindly trust "no issues found."

## Security of the Bot Itself

### Preventing Secret Exfiltration

1. **Never execute untrusted code with secrets** - Workflow checks out the base branch only
2. **Branch verification** - Script verifies it's running from base branch before proceeding
3. **Minimal permissions** - Only `contents: read` and `pull-requests: write`
4. **Pinned dependencies** - Both GitHub Actions (SHA-pinned) and pip packages (version-pinned)
5. **Trusted contributor bypass** - Reduces attack surface

### Supply Chain Protections

- **CODEOWNERS** requires owner approval for go.mod, go.sum, and .github/ changes
- **Dependabot** automates dependency update PRs for both gomod and github-actions
- **govulncheck** runs in CI on every PR (call-graph aware vulnerability scanning)
- **Action SHA pinning** prevents tag-based supply chain attacks

## If You Suspect Compromise

1. **Immediately revoke** the API key in Anthropic console
2. **Generate new key** and update GitHub secret
3. **Check usage** in Anthropic dashboard for unauthorized calls
4. **Review workflow runs** in Actions tab for suspicious activity
5. **Check git history** for unauthorized changes to workflow files
