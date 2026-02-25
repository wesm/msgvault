# Import MBOX Reorg Plan

## Goal
Make `import-mbox` mergeable by moving code into sustainable package boundaries without deleting behavior or tests.

## Non-goals
- No feature rollback.
- No removal of security hardening already implemented.
- No runtime behavior changes unless required by package extraction.

## Phase 1 (current)
Extract zip/source resolution from command code into importer-owned package while keeping compatibility wrappers in `cmd`.

### Move map
- Move zip/source logic from `cmd/msgvault/cmd/import_mbox.go` to `internal/importer/mboxzip`.
- Keep command helper names in place as wrappers so existing tests and call sites remain stable.

### Target package responsibilities
- `internal/importer/mboxzip`
  - Resolve export input (`.mbox`/`.mbx`/`.zip`).
  - Compute zip cache key.
  - Extract mbox files with safety limits.
  - Validate extraction cache.
  - Provide bounded copy helper used by extraction.
- `cmd/msgvault/cmd/import_mbox.go`
  - Parse CLI args.
  - Coordinate import run.
  - Delegate zip/source work to importer package.

## Phase 2
Thin command orchestration and pull multi-file import run flow into importer package.

### Move map
- Add importer entrypoint for command execution flow.
- Keep Cobra command as argument parsing + summary printing.

## Phase 3
Shared attachment storage organization without behavior loss.

### Move map
- Introduce neutral attachment package (`internal/attachments` or equivalent).
- Keep `internal/export/store_attachment.go` as a shim during migration.

## Phase 4
Isolate unrelated cleanup into separate commits for review clarity.

### Examples
- `cmd/msgvault/main.go` runtime exit handling cleanup.
- `cmd/msgvault/cmd/export_eml.go` refactor.
- Sync-path refactors not required for import flow.

## Commit strategy
1. `refactor(import): move zip/source handling into internal/importer/mboxzip with cmd wrappers`
2. `refactor(import): thin command orchestration and centralize run flow`
3. `refactor(shared): attachment storage package boundary with shim`
4. `chore: isolate unrelated runtime/command cleanups`

## Merge readiness checks
- Import behavior preserved for `.mbox` and `.zip`.
- Resume/checkpoint behavior unchanged.
- Existing command-level tests still call wrappers and pass after move.
- No cross-layer dependency inversion (`internal/importer/*` must not depend on `cmd/*`).
