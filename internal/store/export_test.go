package store

// ParseDBTime is exported for testing unexported timestamp parsing behavior.
var ParseDBTime = parseDBTime

// SetFTS5AvailableForTest flips the cached availability flag. Tests use this
// to exercise the guarantee that RebuildFTS works even when FTS5 looks
// unavailable — the symptom that motivates a rebuild in the first place.
func SetFTS5AvailableForTest(s *Store, v bool) {
	s.fts5Available = v
}
