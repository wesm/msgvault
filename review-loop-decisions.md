# Review Loop Decisions (import-mbox PR)

## 2026-02-07

- Issue: `codex review --base main` did not converge to a final findings report (kept expanding exploratory output for an extended period).
  - Decision: Terminated the stuck `codex review` process and proceeded with targeted fixes based on concrete issues observed during the run.
  - Motivation: Keep the PR moving while still addressing correctness/UX problems that were clearly identified.

- Issue: Skipped (already-imported) messages were being counted as `MessagesUpdated`.
  - Decision: Do not increment `MessagesUpdated` when a message is skipped due to the fast existence check.
  - Motivation: Avoid misleading sync statistics; no update is performed on skipped messages.

- Issue: Checkpoint persistence failures were silently ignored.
  - Decision: Treat checkpoint save failures as non-fatal but visible: log a warning and increment error counters.
  - Motivation: Resume is a core feature; failures should be discoverable and reflected in summary output.

- Issue: CLI flag help text implied behaviors that are not true for reruns/skips.
  - Decision: Clarify `--label` as applying to newly imported messages, and clarify `--no-attachments` as skipping attachment storage in both disk and DB.
  - Motivation: Align user expectations with current importer behavior (skips do not reprocess messages).

- Issue: Ctrl+C handling only processed the first signal.
  - Decision: First interrupt triggers graceful cancel; second interrupt forces exit with code 130.
  - Motivation: Match common CLI expectations for "cancel, then force quit".

