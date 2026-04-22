// Package dedup provides cross-source duplicate detection and merging.
//
// # Scoping rules
//
// Dedup is always run against a single logical account. Without explicit
// account scoping, dedup operates on one source at a time (intra-source),
// which means a duplicate group can only contain messages that were
// ingested twice into the same source (for example, re-importing the
// same mbox twice). When the caller supplies an account, dedup operates
// on every source belonging to that account at once (for example, a
// Gmail API sync plus a mbox export of the same mailbox).
//
// Dedup intentionally never merges messages across different accounts.
// This is critical for sent messages: a message alice sends to bob is
// one logical message but it has a legitimate copy in alice's "Sent"
// collection and a legitimate copy in bob's "Inbox". Both copies share
// the same RFC822 Message-ID. If both accounts are archived in
// msgvault, they must be preserved independently because deleting one
// would change the other user's view of history. Sent-message handling
// is covered in more detail by FormatMethodology.
package dedup

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/textproto"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/wesm/msgvault/internal/deletion"
	"github.com/wesm/msgvault/internal/store"
)

// Config controls the dedup engine behaviour.
type Config struct {
	// SourcePreference orders source types when picking a survivor
	// inside a duplicate group. Earlier entries win.
	SourcePreference []string

	// DryRun reports what would happen without mutating the database
	// or writing deletion manifests.
	DryRun bool

	// ContentHashFallback also groups messages by normalized raw MIME
	// content after the RFC822 Message-ID pass. This is slower, but can
	// catch duplicates where Message-ID is missing or transport headers
	// are the only difference between copies.
	ContentHashFallback bool

	// AccountSourceIDs restricts dedup to the listed sources and
	// allows cross-source grouping between them. Callers that want
	// strict per-source dedup should leave this empty.
	AccountSourceIDs []int64

	// Account is the canonical identifier for the scoped account
	// (for example, "alice@gmail.com"). It is used when building
	// deletion manifests and in the methodology output.
	Account string

	// DeleteDupsFromSourceServer, when true, writes pending
	// deletion manifests for pruned duplicates that meet ALL of:
	//   1. the pruned copy lives in a remote source (gmail/imap),
	//   2. the surviving copy is in the SAME source_id (i.e. the
	//      very same remote mailbox holds the winner).
	//
	// This second rule is load-bearing: it guarantees that a
	// merged-pile dedup run can never cause deletions from the
	// user's authoritative Gmail/IMAP account just because a
	// duplicate was found in a local archive. Only true
	// intra-mailbox duplicates are ever proposed for remote
	// deletion.
	//
	// Even with this rule, the field defaults to false so that
	// destructive side effects never happen without an explicit
	// --delete-dups-from-source-server opt-in at the CLI layer.
	DeleteDupsFromSourceServer bool

	// DeletionsDir is the directory where staged deletion manifests
	// are written. Required when DeleteDupsFromSourceServer is true.
	DeletionsDir string

	// IdentityAddresses lists lower-cased email addresses the
	// user considers "me". When a pruned candidate's From:
	// matches any of them, the survivor-selection rule treats
	// the message as a sent copy — in addition to the existing
	// Gmail SENT label and messages.is_from_me signals.
	IdentityAddresses map[string]bool
}

// DefaultSourcePreference is the default source-type authority order.
var DefaultSourcePreference = []string{
	"gmail", "imap", "mbox", "emlx", "hey",
}

// remoteSourceTypes lists source types whose messages can be deleted
// via the deletion-staging machinery.
var remoteSourceTypes = map[string]bool{
	"gmail": true,
	"imap":  true,
}

// Engine orchestrates duplicate detection and merging.
type Engine struct {
	store  *store.Store
	config Config
	logger *slog.Logger
}

// NewEngine creates a new dedup engine.
func NewEngine(st *store.Store, cfg Config, logger *slog.Logger) *Engine {
	if len(cfg.SourcePreference) == 0 {
		cfg.SourcePreference = DefaultSourcePreference
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Engine{store: st, config: cfg, logger: logger}
}

// DuplicateGroup represents a set of messages that are duplicates of
// each other (share the same RFC822 Message-ID in the scoped sources).
type DuplicateGroup struct {
	Key      string             // RFC822 Message-ID or normalized hash
	KeyType  string             // "message-id" or "normalized-hash"
	Messages []DuplicateMessage // all messages in the group
	Survivor int                // index into Messages of the chosen survivor
}

// DuplicateMessage holds metadata for a single message in a duplicate
// group, including sent-message signals for safety checks.
type DuplicateMessage struct {
	ID               int64
	SourceID         int64
	SourceType       string
	SourceIdentifier string
	SourceMessageID  string
	Subject          string
	SentAt           time.Time
	HasRawMIME       bool
	LabelCount       int
	ArchivedAt       time.Time
	IsFromMe         bool
	HasSentLabel     bool
	FromEmail        string
	MatchedIdentity  bool
}

// IsSentCopy reports whether this message appears to be the sender-side
// copy of an outbound email. Three independent signals (OR-combined):
//   - Gmail SENT system label on the message
//   - messages.is_from_me set at ingest time
//   - From: address matches a configured identity address
func (m DuplicateMessage) IsSentCopy() bool {
	return m.HasSentLabel || m.IsFromMe || m.MatchedIdentity
}

// Report summarises the results of a dedup scan.
type Report struct {
	TotalMessages     int64
	DuplicateGroups   int
	DuplicateMessages int            // messages that would be pruned
	BySourcePair      map[string]int // "gmail+mbox" -> groups
	SampleGroups      []DuplicateGroup
	Groups            []DuplicateGroup
	BackfilledCount   int64
	ContentHashGroups int
}

// ExecutionSummary summarises the results of dedup execution.
type ExecutionSummary struct {
	GroupsMerged      int
	MessagesRemoved   int
	LabelsTransferred int
	RawMIMEBackfilled int
	BatchID           string
	StagedManifests   []StagedManifest
}

// StagedManifest records a single deletion manifest created by dedup.
type StagedManifest struct {
	Account      string
	SourceType   string
	ManifestID   string
	MessageCount int
}

// Scan finds all duplicate groups that dedup would prune.
func (e *Engine) Scan(ctx context.Context) (*Report, error) {
	count, err := e.store.CountMessagesWithoutRFC822ID()
	if err != nil {
		return nil, fmt.Errorf("count messages without rfc822 id: %w", err)
	}

	var backfilledCount int64
	if count > 0 {
		e.logger.Info("backfilling rfc822_message_id from stored MIME",
			"count", count)
		var backfillFailed int64
		backfilledCount, backfillFailed, err = e.store.BackfillRFC822IDs(
			func(done, total int64) {
				e.logger.Info("backfill progress",
					"done", done, "total", total)
			},
		)
		if err != nil {
			return nil, fmt.Errorf("backfill rfc822 ids: %w", err)
		}
		if backfilledCount > 0 {
			e.logger.Info("backfilled rfc822_message_id",
				"count", backfilledCount)
		}
		if backfillFailed > 0 {
			e.logger.Warn("backfill: some messages could not be parsed",
				"failed", backfillFailed)
		}
	}

	totalMessages, err := e.store.CountActiveMessages(
		e.config.AccountSourceIDs...,
	)
	if err != nil {
		return nil, fmt.Errorf("count active messages: %w", err)
	}

	storeGroups, err := e.store.FindDuplicatesByRFC822ID(
		e.config.AccountSourceIDs...,
	)
	if err != nil {
		return nil, fmt.Errorf("find duplicates: %w", err)
	}

	report := &Report{
		TotalMessages:   totalMessages,
		BySourcePair:    make(map[string]int),
		BackfilledCount: backfilledCount,
	}

	for _, sg := range storeGroups {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		msgs, err := e.store.GetDuplicateGroupMessages(
			sg.RFC822MessageID, e.config.AccountSourceIDs...,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"get group messages for %s: %w",
				sg.RFC822MessageID, err,
			)
		}
		if len(msgs) < 2 {
			continue
		}

		group := DuplicateGroup{
			Key:     sg.RFC822MessageID,
			KeyType: "message-id",
		}
		for _, m := range msgs {
			matched := false
			if len(e.config.IdentityAddresses) > 0 &&
				m.FromEmail != "" {
				matched = e.config.IdentityAddresses[m.FromEmail]
			}
			group.Messages = append(group.Messages, DuplicateMessage{
				ID:               m.ID,
				SourceID:         m.SourceID,
				SourceType:       m.SourceType,
				SourceIdentifier: m.SourceIdentifier,
				SourceMessageID:  m.SourceMessageID,
				Subject:          m.Subject,
				SentAt:           m.SentAt,
				HasRawMIME:       m.HasRawMIME,
				LabelCount:       m.LabelCount,
				ArchivedAt:       m.ArchivedAt,
				IsFromMe:         m.IsFromMe,
				HasSentLabel:     m.HasSentLabel,
				FromEmail:        m.FromEmail,
				MatchedIdentity:  matched,
			})
		}

		e.selectSurvivor(&group)
		report.Groups = append(report.Groups, group)
		report.BySourcePair[sourcePairKey(group.Messages)]++
	}

	if e.config.ContentHashFallback {
		excludeIDs := make(map[int64]bool, len(report.Groups)*2)
		for _, g := range report.Groups {
			for _, m := range g.Messages {
				excludeIDs[m.ID] = true
			}
		}

		contentHashGroups, err := e.scanNormalizedHashGroups(excludeIDs)
		if err != nil {
			return nil, fmt.Errorf(
				"scan normalized content hashes: %w", err,
			)
		}
		for _, g := range contentHashGroups {
			report.Groups = append(report.Groups, g)
			report.ContentHashGroups++
			report.BySourcePair[sourcePairKey(g.Messages)]++
		}
	}

	report.DuplicateGroups = len(report.Groups)
	for _, g := range report.Groups {
		report.DuplicateMessages += len(g.Messages) - 1
	}

	maxSamples := min(10, len(report.Groups))
	report.SampleGroups = report.Groups[:maxSamples]

	return report, nil
}

// rawWorkItem carries one compressed raw-MIME blob to a worker.
type rawWorkItem struct {
	candidate store.ContentHashCandidate
	rawData   []byte
	compress  string
}

// hashResult carries the normalized hash plus message metadata.
type hashResult struct {
	hash string
	msg  DuplicateMessage
}

// scanNormalizedHashGroups hashes raw MIME after stripping transport-specific
// headers. It skips messages already matched by the primary Message-ID pass.
func (e *Engine) scanNormalizedHashGroups(
	excludeIDs map[int64]bool,
) ([]DuplicateGroup, error) {
	candidates, err := e.store.GetAllRawMIMECandidates(
		e.config.AccountSourceIDs...,
	)
	if err != nil {
		return nil, err
	}

	candidateMap := make(map[int64]store.ContentHashCandidate, len(candidates))
	for _, c := range candidates {
		if !excludeIDs[c.ID] {
			candidateMap[c.ID] = c
		}
	}
	if len(candidateMap) == 0 {
		return nil, nil
	}

	ids := make([]int64, 0, len(candidateMap))
	for id := range candidateMap {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	numWorkers := runtime.NumCPU()
	if numWorkers > 16 {
		numWorkers = 16
	}
	if numWorkers > len(ids) {
		numWorkers = len(ids)
	}
	if numWorkers < 1 {
		numWorkers = 1
	}

	work := make(chan rawWorkItem, numWorkers*4)
	results := make(chan hashResult, numWorkers*4)

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range work {
				raw := item.rawData
				if item.compress == "zlib" {
					r, err := zlib.NewReader(bytes.NewReader(raw))
					if err != nil {
						continue
					}
					decompressed, err := io.ReadAll(r)
					_ = r.Close()
					if err != nil {
						continue
					}
					raw = decompressed
				}

				matched := false
				if len(e.config.IdentityAddresses) > 0 &&
					item.candidate.FromEmail != "" {
					matched = e.config.IdentityAddresses[item.candidate.FromEmail]
				}

				results <- hashResult{
					hash: sha256Hex(normalizeRawMIME(raw)),
					msg: DuplicateMessage{
						ID:               item.candidate.ID,
						SourceID:         item.candidate.SourceID,
						SourceType:       item.candidate.SourceType,
						SourceIdentifier: item.candidate.SourceIdentifier,
						SourceMessageID:  item.candidate.SourceMessageID,
						Subject:          item.candidate.Subject,
						SentAt:           item.candidate.SentAt,
						HasRawMIME:       true,
						LabelCount:       item.candidate.LabelCount,
						ArchivedAt:       item.candidate.ArchivedAt,
						IsFromMe:         item.candidate.IsFromMe,
						HasSentLabel:     item.candidate.HasSentLabel,
						FromEmail:        item.candidate.FromEmail,
						MatchedIdentity:  matched,
					},
				}
			}
		}()
	}

	type hashEntry struct {
		msgs []DuplicateMessage
	}
	hashMap := make(map[string]*hashEntry)
	collectDone := make(chan struct{})
	go func() {
		for r := range results {
			if entry, ok := hashMap[r.hash]; ok {
				entry.msgs = append(entry.msgs, r.msg)
			} else {
				hashMap[r.hash] = &hashEntry{msgs: []DuplicateMessage{r.msg}}
			}
		}
		close(collectDone)
	}()

	readErr := e.store.StreamMessageRaw(
		ids,
		func(messageID int64, rawData []byte, compression string) {
			c, ok := candidateMap[messageID]
			if !ok {
				return
			}
			dataCopy := make([]byte, len(rawData))
			copy(dataCopy, rawData)
			work <- rawWorkItem{
				candidate: c,
				rawData:   dataCopy,
				compress:  compression,
			}
		},
	)
	close(work)
	wg.Wait()
	close(results)
	<-collectDone

	if readErr != nil {
		return nil, fmt.Errorf("stream message raw: %w", readErr)
	}

	var groups []DuplicateGroup
	for hash, entry := range hashMap {
		if len(entry.msgs) < 2 {
			continue
		}
		g := DuplicateGroup{
			Key:      hash,
			KeyType:  "normalized-hash",
			Messages: entry.msgs,
		}
		e.selectSurvivor(&g)
		groups = append(groups, g)
	}
	return groups, nil
}

// transportHeaders vary across otherwise-identical copies of the same email.
var transportHeaders = map[string]bool{
	"Received":                   true,
	"Delivered-To":               true,
	"Return-Path":                true,
	"X-Received":                 true,
	"X-Gmail-Labels":             true,
	"X-Gmail-Received":           true,
	"X-Google-Smtp-Source":       true,
	"X-Gm-Message-State":         true,
	"Authentication-Results":     true,
	"Dkim-Signature":             true,
	"Arc-Seal":                   true,
	"Arc-Message-Signature":      true,
	"Arc-Authentication-Results": true,
	"X-Google-Dkim-Signature":    true,
	"X-Forwarded-To":             true,
	"X-Forwarded-For":            true,
	"X-Original-To":              true,
	"X-Apple-Mail-Labels":        true,
}

// normalizeRawMIME strips transport/export-specific headers before hashing.
func normalizeRawMIME(raw []byte) []byte {
	headerEnd := bytes.Index(raw, []byte("\r\n\r\n"))
	if headerEnd == -1 {
		headerEnd = bytes.Index(raw, []byte("\n\n"))
	}
	if headerEnd == -1 {
		return raw
	}

	headerSection := raw[:headerEnd]
	body := raw[headerEnd:]

	// Copy headerSection before appending to avoid mutating the
	// underlying raw buffer (headerSection is a sub-slice of raw).
	hdrBuf := make([]byte, len(headerSection)+4)
	copy(hdrBuf, headerSection)
	copy(hdrBuf[len(headerSection):], "\r\n\r\n")
	reader := textproto.NewReader(bufio.NewReader(bytes.NewReader(hdrBuf)))
	mimeHeader, err := reader.ReadMIMEHeader()
	if err != nil {
		return raw
	}

	var kept []string
	for key := range mimeHeader {
		if !transportHeaders[textproto.CanonicalMIMEHeaderKey(key)] {
			kept = append(kept, key)
		}
	}
	sort.Strings(kept)

	var buf bytes.Buffer
	for _, key := range kept {
		for _, val := range mimeHeader[key] {
			fmt.Fprintf(&buf, "%s: %s\n", key, val)
		}
	}
	buf.Write(body)
	return buf.Bytes()
}

func sha256Hex(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// selectSurvivor picks the best message to keep in a duplicate group.
func (e *Engine) selectSurvivor(group *DuplicateGroup) {
	if len(group.Messages) <= 1 {
		group.Survivor = 0
		return
	}

	priorityMap := make(map[string]int)
	for i, st := range e.config.SourcePreference {
		priorityMap[st] = i
	}

	candidates := allIndexes(len(group.Messages))
	var sentIdxs []int
	for _, i := range candidates {
		if group.Messages[i].IsSentCopy() {
			sentIdxs = append(sentIdxs, i)
		}
	}
	if len(sentIdxs) > 0 {
		candidates = sentIdxs
	}

	best := candidates[0]
	for _, i := range candidates[1:] {
		if e.isBetter(
			group.Messages[i], group.Messages[best], priorityMap,
		) {
			best = i
		}
	}
	group.Survivor = best
}

func allIndexes(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}

// isBetter returns true if candidate is a better survivor than current.
func (e *Engine) isBetter(
	candidate, current DuplicateMessage, priorityMap map[string]int,
) bool {
	candPri := sourcePriority(candidate.SourceType, priorityMap)
	currPri := sourcePriority(current.SourceType, priorityMap)
	if candPri != currPri {
		return candPri < currPri
	}
	if candidate.HasRawMIME != current.HasRawMIME {
		return candidate.HasRawMIME
	}
	if candidate.LabelCount != current.LabelCount {
		return candidate.LabelCount > current.LabelCount
	}
	if !candidate.ArchivedAt.IsZero() && !current.ArchivedAt.IsZero() {
		return candidate.ArchivedAt.Before(current.ArchivedAt)
	}
	return candidate.ID < current.ID
}

func sourcePriority(sourceType string, priorityMap map[string]int) int {
	if p, ok := priorityMap[sourceType]; ok {
		return p
	}
	return len(priorityMap)
}

// Execute merges every duplicate group: unions labels onto the
// survivor, soft-deletes the pruned duplicates, and — when
// DeleteDupsFromSourceServer is enabled AND a pruned copy shares a
// source_id with its survivor — writes a deletion manifest.
func (e *Engine) Execute(
	ctx context.Context, report *Report, batchID string,
) (*ExecutionSummary, error) {
	summary := &ExecutionSummary{BatchID: batchID}

	remoteByAccount := make(map[string][]string)
	remoteSourceType := make(map[string]string)

	for i, group := range report.Groups {
		if ctx.Err() != nil {
			return summary, ctx.Err()
		}

		survivor := group.Messages[group.Survivor]
		survivorID := survivor.ID
		var dupIDs []int64
		for j, m := range group.Messages {
			if j == group.Survivor {
				continue
			}
			dupIDs = append(dupIDs, m.ID)

			if !e.config.DeleteDupsFromSourceServer {
				continue
			}
			if !remoteSourceTypes[m.SourceType] {
				continue
			}
			if m.SourceID != survivor.SourceID {
				continue
			}
			acct := m.SourceIdentifier
			if acct == "" {
				acct = e.config.Account
			}
			remoteByAccount[acct] = append(
				remoteByAccount[acct], m.SourceMessageID,
			)
			if _, ok := remoteSourceType[acct]; !ok {
				remoteSourceType[acct] = m.SourceType
			}
		}

		mergeResult, err := e.store.MergeDuplicates(
			survivorID, dupIDs, batchID,
		)
		if err != nil {
			return summary, fmt.Errorf(
				"merge group %d (%s): %w", i, group.Key, err,
			)
		}

		summary.GroupsMerged++
		summary.MessagesRemoved += len(dupIDs)
		summary.LabelsTransferred += mergeResult.LabelsTransferred
		summary.RawMIMEBackfilled += mergeResult.RawMIMEBackfilled
	}

	if e.config.DeleteDupsFromSourceServer && len(remoteByAccount) > 0 {
		staged, err := e.stageDeletionManifests(
			batchID, remoteByAccount, remoteSourceType,
		)
		if err != nil {
			return summary, err
		}
		summary.StagedManifests = staged
	}

	return summary, nil
}

func (e *Engine) stageDeletionManifests(
	batchID string,
	byAccount map[string][]string,
	srcType map[string]string,
) ([]StagedManifest, error) {
	if e.config.DeletionsDir == "" {
		return nil, fmt.Errorf(
			"deletions dir not configured but " +
				"DeleteDupsFromSourceServer is true",
		)
	}

	mgr, err := deletion.NewManager(e.config.DeletionsDir)
	if err != nil {
		return nil, fmt.Errorf("open deletion manager: %w", err)
	}

	accounts := make([]string, 0, len(byAccount))
	for a := range byAccount {
		accounts = append(accounts, a)
	}
	sort.Strings(accounts)

	var staged []StagedManifest
	for _, acct := range accounts {
		ids := dedupStrings(byAccount[acct])
		if len(ids) == 0 {
			continue
		}

		description := fmt.Sprintf("Dedup pruned duplicates (%s)", batchID)
		manifest := deletion.NewManifest(description, ids)
		manifest.ID = manifestIDFor(batchID, acct)
		manifest.CreatedBy = "dedup"
		manifest.Filters.Account = acct

		path := filepath.Join(
			mgr.PendingDir(), manifest.ID+".json",
		)
		if err := manifest.Save(path); err != nil {
			return staged, fmt.Errorf(
				"save manifest for %s: %w", acct, err,
			)
		}
		staged = append(staged, StagedManifest{
			Account:      acct,
			SourceType:   srcType[acct],
			ManifestID:   manifest.ID,
			MessageCount: len(ids),
		})
	}
	return staged, nil
}

func manifestIDFor(batchID, account string) string {
	return fmt.Sprintf("%s-%s", batchID, sanitizeAccount(account))
}

func sanitizeAccount(a string) string {
	var b strings.Builder
	for _, r := range a {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '-' || r == '_':
			b.WriteRune(r)
		case r == '@' || r == '.':
			b.WriteRune('-')
		}
	}
	s := b.String()
	if s == "" {
		s = "account"
	}
	if len(s) > 40 {
		s = s[:40]
	}
	return s
}

func dedupStrings(in []string) []string {
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// Undo restores every message with the given batch ID and cancels any
// pending deletion manifests that dedup created for that batch.
func (e *Engine) Undo(batchID string) (int64, []string, error) {
	restored, err := e.store.UndoDedup(batchID)
	if err != nil {
		return 0, nil, err
	}

	if e.config.DeletionsDir == "" {
		return restored, nil, nil
	}

	mgr, err := deletion.NewManager(e.config.DeletionsDir)
	if err != nil {
		return restored, nil, fmt.Errorf("open deletion manager: %w", err)
	}
	pending, err := mgr.ListPending()
	if err != nil {
		return restored, nil, fmt.Errorf("list pending: %w", err)
	}
	inProgress, err := mgr.ListInProgress()
	if err != nil {
		return restored, nil, fmt.Errorf("list in-progress: %w", err)
	}

	var stillExecuting []string
	prefix := batchID + "-"
	for _, m := range pending {
		if !strings.HasPrefix(m.ID, prefix) && m.ID != batchID {
			continue
		}
		if err := mgr.CancelManifest(m.ID); err != nil {
			return restored, stillExecuting, fmt.Errorf(
				"cancel manifest %s: %w", m.ID, err,
			)
		}
	}
	for _, m := range inProgress {
		if !strings.HasPrefix(m.ID, prefix) && m.ID != batchID {
			continue
		}
		stillExecuting = append(stillExecuting, m.ID)
	}
	return restored, stillExecuting, nil
}

// FormatReport renders a human-readable report of the scan results.
func (e *Engine) FormatReport(r *Report) string {
	var sb strings.Builder
	sb.WriteString("\n=== Deduplication Report ===\n\n")

	if r.BackfilledCount > 0 {
		fmt.Fprintf(&sb,
			"Backfilled %d messages with RFC822 Message-ID "+
				"from stored MIME.\n\n",
			r.BackfilledCount)
	}

	if r.DuplicateGroups == 0 {
		sb.WriteString("No duplicates found.\n")
		return sb.String()
	}

	fmt.Fprintf(&sb, "Duplicate groups found: %d\n", r.DuplicateGroups)
	fmt.Fprintf(&sb, "Messages to prune:      %d\n", r.DuplicateMessages)
	if r.ContentHashGroups > 0 {
		fmt.Fprintf(&sb, "Content-hash groups:   %d\n", r.ContentHashGroups)
	}

	if len(r.BySourcePair) > 0 {
		sb.WriteString("\nBreakdown by source pair:\n")
		pairs := make([]string, 0, len(r.BySourcePair))
		for k := range r.BySourcePair {
			pairs = append(pairs, k)
		}
		sort.Strings(pairs)
		for _, pair := range pairs {
			fmt.Fprintf(&sb, "  %-20s %d groups\n",
				pair, r.BySourcePair[pair])
		}
	}

	sentGroups := 0
	for _, g := range r.Groups {
		for _, m := range g.Messages {
			if m.IsSentCopy() {
				sentGroups++
				break
			}
		}
	}
	if sentGroups > 0 {
		fmt.Fprintf(&sb,
			"\nSent-copy groups detected: %d "+
				"(survivor forced to a sent copy)\n",
			sentGroups)
	}

	if len(r.SampleGroups) > 0 {
		sb.WriteString("\nSample duplicate groups:\n")
		for i, g := range r.SampleGroups {
			label := g.Key
			if g.KeyType != "" && g.KeyType != "message-id" {
				label = fmt.Sprintf("%s (%s)", g.Key, g.KeyType)
			}
			fmt.Fprintf(&sb, "\n  Group %d: %s\n", i+1, label)
			for j, m := range g.Messages {
				marker := "  "
				if j == g.Survivor {
					marker = "* "
				}
				sent := ""
				if m.IsSentCopy() {
					sent = " [sent]"
				}
				fmt.Fprintf(&sb,
					"    %s[%s:%s]%s %s "+
						"(labels: %d, raw: %v)\n",
					marker, m.SourceType, m.SourceIdentifier,
					sent, m.Subject, m.LabelCount, m.HasRawMIME,
				)
			}
		}
	}

	return sb.String()
}

// FormatMethodology returns a detailed explanation of how dedup works.
func (e *Engine) FormatMethodology() string {
	var sb strings.Builder
	sb.WriteString("\n=== Deduplication Methodology ===\n\n")

	sb.WriteString("Scope:\n")
	if e.config.Account != "" {
		fmt.Fprintf(&sb,
			"  Scoped to account: %s (%d source(s)). "+
				"Cross-source dedup is enabled\n"+
				"  within this account.\n",
			e.config.Account, len(e.config.AccountSourceIDs))
	} else if len(e.config.AccountSourceIDs) > 0 {
		fmt.Fprintf(&sb,
			"  Scoped to %d source(s). Cross-source dedup is "+
				"enabled within that set.\n",
			len(e.config.AccountSourceIDs))
	} else {
		sb.WriteString(
			"  No account specified — only messages that appear " +
				"twice in the\n" +
				"  SAME source are eligible. Rerun with " +
				"--account <email> to dedup\n" +
				"  across sources that belong to one mailbox " +
				"(e.g. Gmail sync +\n" +
				"  mbox import of the same account).\n",
		)
	}
	sb.WriteString("\n")

	sb.WriteString("Detection:\n")
	sb.WriteString("  Messages are grouped by the RFC822 Message-ID " +
		"header.\n")
	sb.WriteString("  Messages missing that header are backfilled " +
		"from stored MIME\n")
	sb.WriteString("  before the scan runs.")
	if e.config.ContentHashFallback {
		sb.WriteString(" Messages still without an ID are then compared via\n")
		sb.WriteString("  a normalized raw-MIME hash that strips transport " +
			"headers such as\n")
		sb.WriteString("  Received, Delivered-To, X-Gmail-Labels, and " +
			"DKIM/ARC traces.\n\n")
	} else {
		sb.WriteString(" Messages still without an ID are ignored.\n\n")
	}

	sb.WriteString("Survivor selection:\n")
	for i, st := range e.config.SourcePreference {
		fmt.Fprintf(&sb, "  %d. %s\n", i+1, st)
	}
	sb.WriteString("  Tiebreakers: has raw MIME > more labels > " +
		"earlier archived_at > lower id.\n\n")

	sb.WriteString("Sent messages:\n")
	sb.WriteString(
		"  Dedup NEVER merges messages across different " +
			"accounts. A message that\n" +
			"  alice sent to bob is two distinct mailbox " +
			"copies — one in alice's\n" +
			"  Sent folder and one in bob's Inbox. Both are " +
			"preserved independently\n" +
			"  because deleting either would alter the other " +
			"user's archive.\n\n",
	)

	sb.WriteString("Merge behaviour:\n")
	sb.WriteString("  - Labels from every copy are unioned onto " +
		"the survivor.\n")
	sb.WriteString("  - Raw MIME is backfilled onto the survivor " +
		"if it lacks it.\n")
	sb.WriteString("  - Pruned duplicates are hidden in the msgvault " +
		"database (reversible via --undo).\n")
	sb.WriteString("  - Remote mailboxes (Gmail, IMAP) are NEVER " +
		"modified by default.\n")

	return sb.String()
}

func sourcePairKey(msgs []DuplicateMessage) string {
	types := make(map[string]bool)
	for _, m := range msgs {
		types[m.SourceType] = true
	}
	sorted := make([]string, 0, len(types))
	for t := range types {
		sorted = append(sorted, t)
	}
	sort.Strings(sorted)
	return strings.Join(sorted, "+")
}
