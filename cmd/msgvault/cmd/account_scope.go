package cmd

import (
	"fmt"

	"github.com/wesm/msgvault/internal/store"
)

// AccountScope is the result of resolving a user-supplied --account
// flag against the store.
type AccountScope struct {
	Input  string
	Source *store.Source
}

// IsEmpty reports whether the scope resolved to nothing.
func (s AccountScope) IsEmpty() bool {
	return s.Source == nil
}

// SourceIDs returns the source IDs that this scope expands to.
func (s AccountScope) SourceIDs() []int64 {
	if s.Source != nil {
		return []int64{s.Source.ID}
	}
	return nil
}

// DisplayName returns a human-readable label for the scope.
func (s AccountScope) DisplayName() string {
	if s.Source != nil {
		return s.Source.Identifier
	}
	return ""
}

// ResolveAccount resolves a user-supplied --account string against
// the store. Returns an empty scope if input is empty. Currently
// looks up sources by identifier or display name; collection lookup
// will be added when collections are implemented.
func ResolveAccount(
	st *store.Store, input string,
) (AccountScope, error) {
	scope := AccountScope{Input: input}
	if input == "" {
		return scope, nil
	}

	sources, err := st.GetSourcesByIdentifierOrDisplayName(input)
	if err != nil {
		return scope, fmt.Errorf(
			"look up source for %q: %w", input, err,
		)
	}
	if len(sources) == 0 {
		return scope, fmt.Errorf(
			"no account or source found for %q "+
				"(try 'msgvault list-accounts')",
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
