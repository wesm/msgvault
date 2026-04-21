package cmd

import (
	"errors"
	"fmt"

	"github.com/wesm/msgvault/internal/store"
)

// AccountScope is the result of resolving a user-supplied --account
// flag against the store.
type AccountScope struct {
	Input      string
	Source     *store.Source
	Collection *store.CollectionWithSources
}

// IsEmpty reports whether the scope resolved to nothing.
func (s AccountScope) IsEmpty() bool {
	return s.Source == nil && s.Collection == nil
}

// IsCollection reports whether the scope refers to a collection.
func (s AccountScope) IsCollection() bool {
	return s.Collection != nil
}

// SourceIDs returns the source IDs that this scope expands to.
func (s AccountScope) SourceIDs() []int64 {
	switch {
	case s.Collection != nil:
		return append([]int64(nil), s.Collection.SourceIDs...)
	case s.Source != nil:
		return []int64{s.Source.ID}
	}
	return nil
}

// DisplayName returns a human-readable label for the scope.
func (s AccountScope) DisplayName() string {
	switch {
	case s.Collection != nil:
		return s.Collection.Name
	case s.Source != nil:
		return s.Source.Identifier
	}
	return ""
}

// ResolveAccount resolves a user-supplied --account string against
// the store. Collections are checked first, then sources.
func ResolveAccount(
	st *store.Store, input string,
) (AccountScope, error) {
	scope := AccountScope{Input: input}
	if input == "" {
		return scope, nil
	}

	// Try collection first.
	coll, err := st.GetCollectionByName(input)
	switch {
	case err == nil:
		scope.Collection = coll
		return scope, nil
	case errors.Is(err, store.ErrCollectionNotFound):
		// Fall through to source lookup.
	default:
		return scope, fmt.Errorf(
			"look up collection %q: %w", input, err,
		)
	}

	// Source lookup.
	sources, err := st.GetSourcesByIdentifierOrDisplayName(input)
	if err != nil {
		return scope, fmt.Errorf(
			"look up source for %q: %w", input, err,
		)
	}
	if len(sources) == 0 {
		return scope, fmt.Errorf(
			"no collection or source found for %q "+
				"(try 'msgvault collections list' or "+
				"'msgvault list-accounts')",
			input,
		)
	}
	if len(sources) > 1 {
		names := make([]string, 0, len(sources))
		for _, s := range sources {
			names = append(names, fmt.Sprintf(
				"%s (%s, id=%d)",
				s.Identifier, s.SourceType, s.ID,
			))
		}
		return scope, fmt.Errorf(
			"ambiguous account %q matches multiple sources: %v",
			input, names,
		)
	}
	scope.Source = sources[0]
	return scope, nil
}
