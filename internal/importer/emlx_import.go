package importer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/wesm/msgvault/internal/emlx"
	"github.com/wesm/msgvault/internal/store"
)

// EmlxImportOptions configures an Apple Mail .emlx directory import.
type EmlxImportOptions struct {
	// SourceType is the sources.source_type value.
	// Defaults to "apple-mail".
	SourceType string

	// Identifier is the sources.identifier (e.g. "you@gmail.com").
	Identifier string

	// NoResume forces a fresh import even if a prior run exists.
	NoResume bool

	// CheckpointInterval controls how often (in messages) to persist
	// progress. Defaults to 200.
	CheckpointInterval int

	// AttachmentsDir controls where attachments are written.
	// Empty means no disk storage.
	AttachmentsDir string

	// MaxMessageBytes limits the maximum .emlx file size to read.
	// Defaults to 128 MiB.
	MaxMessageBytes int64

	// Logger is optional; defaults to slog.Default().
	Logger *slog.Logger
}

// EmlxImportSummary reports the results of an emlx import.
type EmlxImportSummary struct {
	WasResumed        bool
	Duration          time.Duration
	MailboxesTotal    int
	MailboxesImported int
	MessagesProcessed int64
	MessagesAdded     int64
	MessagesUpdated   int64
	MessagesSkipped   int64
	Errors            int64
	HardErrors        bool
}

type emlxCheckpoint struct {
	RootDir      string `json:"root_dir"`
	MailboxIndex int    `json:"mailbox_index"`
	LastFile     string `json:"last_file"`
}

const defaultMaxEmlxBytes int64 = 128 << 20 // 128 MiB

// ImportEmlxDir imports .emlx files from an Apple Mail directory tree.
//
// Messages are deduplicated by content hash (sha256 of raw MIME).
// When the same message appears in multiple mailboxes, the first
// occurrence is fully ingested; subsequent occurrences add their
// mailbox label to the existing message.
func ImportEmlxDir(
	ctx context.Context, st *store.Store,
	rootDir string, opts EmlxImportOptions,
) (*EmlxImportSummary, error) {
	if opts.SourceType == "" {
		opts.SourceType = "apple-mail"
	}
	if opts.Identifier == "" {
		return nil, fmt.Errorf("identifier is required")
	}
	if opts.CheckpointInterval <= 0 {
		opts.CheckpointInterval = 200
	}
	if opts.MaxMessageBytes <= 0 {
		opts.MaxMessageBytes = defaultMaxEmlxBytes
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}

	start := time.Now()
	summary := &EmlxImportSummary{}

	// Discover mailboxes.
	mailboxes, err := emlx.DiscoverMailboxes(rootDir)
	if err != nil {
		return nil, fmt.Errorf("discover mailboxes: %w", err)
	}
	summary.MailboxesTotal = len(mailboxes)
	if len(mailboxes) == 0 {
		summary.Duration = time.Since(start)
		return summary, nil
	}

	absRoot, err := filepath.Abs(rootDir)
	if err != nil {
		return nil, fmt.Errorf("abs path: %w", err)
	}

	src, err := st.GetOrCreateSource(opts.SourceType, opts.Identifier)
	if err != nil {
		return nil, fmt.Errorf("get/create source: %w", err)
	}

	// Resume support.
	var (
		syncID     int64
		cp         store.Checkpoint
		startMbox  int
		startAfter string // skip files <= this name within the start mailbox
	)

	if !opts.NoResume {
		active, err := st.GetActiveSync(src.ID)
		if err != nil {
			return nil, fmt.Errorf("check active sync: %w", err)
		}
		if active != nil {
			syncID = active.ID
			cp.MessagesProcessed = active.MessagesProcessed
			cp.MessagesAdded = active.MessagesAdded
			cp.MessagesUpdated = active.MessagesUpdated
			cp.ErrorsCount = active.ErrorsCount

			if active.CursorBefore.Valid &&
				active.CursorBefore.String != "" {
				var ecp emlxCheckpoint
				if err := json.Unmarshal(
					[]byte(active.CursorBefore.String), &ecp,
				); err == nil && ecp.RootDir == absRoot {
					startMbox = ecp.MailboxIndex
					startAfter = ecp.LastFile
					summary.WasResumed = true
					log.Info("resuming emlx import",
						"root", absRoot,
						"mailbox_index", startMbox,
						"last_file", startAfter,
						"processed", cp.MessagesProcessed,
					)
				}
			}
		}
	}

	if syncID == 0 {
		syncID, err = st.StartSync(src.ID, "import-emlx")
		if err != nil {
			return nil, fmt.Errorf("start sync: %w", err)
		}
	}

	// Save initial checkpoint.
	if err := saveEmlxCheckpoint(
		st, syncID, absRoot, startMbox, startAfter, &cp,
	); err != nil {
		cp.ErrorsCount++
		summary.Errors++
		log.Warn("failed to save initial checkpoint", "error", err)
	}

	hardErrors := false

	type pendingEmlxMsg struct {
		Raw       []byte
		RawHash   string
		SourceMsg string
		LabelIDs  []int64
		Fallback  time.Time
		MboxIdx   int
		FileName  string
	}

	const (
		batchSize  = 200
		batchBytes = 32 << 20 // 32 MiB
	)

	var pending []pendingEmlxMsg
	var pendingBytes int64
	lastCpMbox := startMbox
	lastCpFile := startAfter

	flushPending := func() (bool, error) {
		if len(pending) == 0 {
			return false, nil
		}

		ids := make([]string, len(pending))
		for i, p := range pending {
			ids[i] = p.SourceMsg
		}

		existingWithRaw, err := st.MessageExistsWithRawBatch(src.ID, ids)
		batchOK := err == nil
		if err != nil {
			cp.ErrorsCount++
			summary.Errors++
			log.Warn("existence check failed", "error", err)
		}

		existingAny, err := st.MessageExistsBatch(src.ID, ids)
		anyOK := err == nil
		if err != nil {
			cp.ErrorsCount++
			summary.Errors++
			log.Warn("existence check failed (any)", "error", err)
		}

		for _, p := range pending {
			if err := ctx.Err(); err != nil {
				summary.Duration = time.Since(start)
				if err := saveEmlxCheckpoint(
					st, syncID, absRoot, lastCpMbox, lastCpFile, &cp,
				); err != nil {
					log.Warn("checkpoint save failed", "error", err)
				}
				return true, nil
			}

			cp.MessagesProcessed++
			summary.MessagesProcessed++

			// Check if fully exists (with raw).
			exists := false
			if batchOK {
				msgID, ok := existingWithRaw[p.SourceMsg]
				if ok {
					exists = true
					// Add labels from this mailbox to the existing message.
					if len(p.LabelIDs) > 0 {
						if err := st.AddMessageLabels(
							msgID, p.LabelIDs,
						); err != nil {
							log.Warn("failed to add labels to existing message",
								"message_id", msgID, "error", err,
							)
						}
					}
				}
			} else {
				one, err := st.MessageExistsWithRawBatch(
					src.ID, []string{p.SourceMsg},
				)
				if err != nil {
					cp.ErrorsCount++
					summary.Errors++
				} else if msgID, ok := one[p.SourceMsg]; ok {
					exists = true
					if len(p.LabelIDs) > 0 {
						if err := st.AddMessageLabels(
							msgID, p.LabelIDs,
						); err != nil {
							log.Warn("failed to add labels",
								"message_id", msgID, "error", err,
							)
						}
					}
				}
			}

			if exists {
				summary.MessagesSkipped++
				lastCpMbox = p.MboxIdx
				lastCpFile = p.FileName
				checkpointIfDue(
					&cp, summary, opts.CheckpointInterval,
					st, syncID, absRoot, lastCpMbox, lastCpFile, log,
				)
				continue
			}

			alreadyExists := false
			if anyOK {
				_, alreadyExists = existingAny[p.SourceMsg]
			}

			if err := IngestRawMessage(
				ctx, st, src.ID, opts.Identifier,
				opts.AttachmentsDir, p.LabelIDs,
				p.SourceMsg, p.RawHash,
				p.Raw, p.Fallback, log,
			); err != nil {
				cp.ErrorsCount++
				summary.Errors++
				log.Warn("failed to ingest message",
					"source_msg", p.SourceMsg,
					"file", p.FileName,
					"error", err,
				)
				hardErrors = true
				continue
			}

			if alreadyExists {
				cp.MessagesUpdated++
				summary.MessagesUpdated++
			} else {
				cp.MessagesAdded++
				summary.MessagesAdded++
			}

			lastCpMbox = p.MboxIdx
			lastCpFile = p.FileName
			checkpointIfDue(
				&cp, summary, opts.CheckpointInterval,
				st, syncID, absRoot, lastCpMbox, lastCpFile, log,
			)
		}

		clear(pending)
		pending = pending[:0]
		pendingBytes = 0
		return false, nil
	}

	for mboxIdx := startMbox; mboxIdx < len(mailboxes); mboxIdx++ {
		mb := mailboxes[mboxIdx]

		labelID, err := st.EnsureLabel(
			src.ID, mb.Label, mb.Label, "user",
		)
		if err != nil {
			cp.ErrorsCount++
			summary.Errors++
			log.Warn("failed to ensure label",
				"label", mb.Label, "error", err,
			)
			continue
		}
		labelIDs := []int64{labelID}

		log.Info("importing mailbox",
			"label", mb.Label,
			"files", len(mb.Files),
			"index", mboxIdx,
		)

		for _, fileName := range mb.Files {
			if ctx.Err() != nil {
				break
			}

			// Resume: skip files already processed.
			if mboxIdx == startMbox && startAfter != "" {
				if fileName <= startAfter {
					continue
				}
			}

			filePath := filepath.Join(mb.Path, "Messages", fileName)

			// Check file size before reading.
			msg, err := emlx.ParseFile(filePath)
			if err != nil {
				cp.ErrorsCount++
				summary.Errors++
				log.Warn("failed to parse .emlx",
					"file", filePath, "error", err,
				)
				continue
			}

			if int64(len(msg.Raw)) > opts.MaxMessageBytes {
				cp.ErrorsCount++
				summary.Errors++
				log.Warn("message exceeds size limit",
					"file", filePath,
					"size", len(msg.Raw),
					"limit", opts.MaxMessageBytes,
				)
				continue
			}

			sum := sha256.Sum256(msg.Raw)
			rawHash := hex.EncodeToString(sum[:])
			sourceMsgID := "emlx-" + rawHash

			var fallbackDate time.Time
			if !msg.PlistDate.IsZero() {
				fallbackDate = msg.PlistDate
			}

			pending = append(pending, pendingEmlxMsg{
				Raw:       msg.Raw,
				RawHash:   rawHash,
				SourceMsg: sourceMsgID,
				LabelIDs:  labelIDs,
				Fallback:  fallbackDate,
				MboxIdx:   mboxIdx,
				FileName:  fileName,
			})
			pendingBytes += int64(len(msg.Raw))

			if len(pending) >= batchSize || pendingBytes >= batchBytes {
				stop, err := flushPending()
				if err != nil {
					return summary, err
				}
				if stop {
					return summary, nil
				}
			}
		}

		// Flush remaining for this mailbox.
		if stop, err := flushPending(); err != nil {
			return summary, err
		} else if stop {
			return summary, nil
		}

		summary.MailboxesImported++

		if ctx.Err() != nil {
			break
		}
	}

	summary.Duration = time.Since(start)
	summary.HardErrors = hardErrors

	// Final checkpoint.
	if err := saveEmlxCheckpoint(
		st, syncID, absRoot, lastCpMbox, lastCpFile, &cp,
	); err != nil {
		cp.ErrorsCount++
		summary.Errors++
		log.Warn("failed to save final checkpoint", "error", err)
	}

	if hardErrors {
		if err := st.FailSync(syncID, fmt.Sprintf(
			"completed with %d errors", cp.ErrorsCount,
		)); err != nil {
			return summary, fmt.Errorf("fail sync: %w", err)
		}
		return summary, nil
	}

	finalMsg := fmt.Sprintf(
		"mailboxes:%d messages:%d",
		summary.MailboxesImported, summary.MessagesAdded,
	)
	if cp.ErrorsCount > 0 {
		finalMsg = fmt.Sprintf(
			"mailboxes:%d messages:%d errors:%d",
			summary.MailboxesImported, summary.MessagesAdded,
			cp.ErrorsCount,
		)
	}
	if err := st.CompleteSync(syncID, finalMsg); err != nil {
		return summary, fmt.Errorf("complete sync: %w", err)
	}

	return summary, nil
}

func saveEmlxCheckpoint(
	st *store.Store, syncID int64,
	rootDir string, mboxIdx int, lastFile string,
	cp *store.Checkpoint,
) error {
	b, err := json.Marshal(emlxCheckpoint{
		RootDir:      rootDir,
		MailboxIndex: mboxIdx,
		LastFile:     lastFile,
	})
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}
	cp.PageToken = string(b)
	return st.UpdateSyncCheckpoint(syncID, cp)
}

func checkpointIfDue(
	cp *store.Checkpoint, summary *EmlxImportSummary,
	interval int,
	st *store.Store, syncID int64,
	rootDir string, mboxIdx int, lastFile string,
	log *slog.Logger,
) {
	if cp.MessagesProcessed%int64(interval) != 0 {
		return
	}
	if err := saveEmlxCheckpoint(
		st, syncID, rootDir, mboxIdx, lastFile, cp,
	); err != nil {
		cp.ErrorsCount++
		summary.Errors++
		log.Warn("failed to save checkpoint", "error", err)
	}
}
