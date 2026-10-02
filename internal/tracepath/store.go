package tracepath

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// State is the complete database. It is small enough to live in memory and to be
// rewritten atomically as one JSON document after every mutation, which is what
// replaces a real database driver in this standard-library-only build.
type State struct {
	// Spans is trace id -> span id -> span, giving both lookups in one place.
	Spans       map[string]map[string]*SpanInput `json:"spans"`
	Idempotency map[string]*IdempotencyRecord    `json:"idempotency"`
}

func newState() *State {
	return &State{
		Spans:       map[string]map[string]*SpanInput{},
		Idempotency: map[string]*IdempotencyRecord{},
	}
}

func (s *State) repair() {
	if s.Spans == nil {
		s.Spans = map[string]map[string]*SpanInput{}
	}
	for traceID, spans := range s.Spans {
		if spans == nil {
			s.Spans[traceID] = map[string]*SpanInput{}
		}
	}
	if s.Idempotency == nil {
		s.Idempotency = map[string]*IdempotencyRecord{}
	}
}

// traceIDs returns the stored trace ids in lexicographic order.
func (s *State) traceIDs() []string {
	ids := make([]string, 0, len(s.Spans))
	for id := range s.Spans {
		ids = append(ids, id)
	}
	sortStrings(ids)
	return ids
}

// Store owns the in-memory state and its file. Every mutation runs against a
// copy that is persisted first, so a failed write never changes the process
// state and no reader ever sees a half-applied transaction.
type Store struct {
	path  string
	mutex sync.Mutex
	state *State
}

// OpenStore loads the database at path, creating it on first write.
func OpenStore(path string) (*Store, error) {
	if path == "" {
		path = "tracepath.db"
	}
	state := newState()
	contents, err := os.ReadFile(path)
	switch {
	case err == nil:
		if err := json.Unmarshal(contents, state); err != nil {
			return nil, InternalError("database %s is not readable JSON: %s", path, err)
		}
		state.repair()
	case os.IsNotExist(err):
	default:
		return nil, InternalError("database %s could not be read: %s", path, err)
	}
	return &Store{path: path, state: state}, nil
}

// Update runs mutate against a copy of the state and commits it on success.
func (s *Store) Update(mutate func(state *State) error) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	clone, err := cloneState(s.state)
	if err != nil {
		return err
	}
	if err := mutate(clone); err != nil {
		return err
	}
	if err := writeState(s.path, clone); err != nil {
		return err
	}
	s.state = clone
	return nil
}

// View runs read against the committed state under the store lock.
func (s *Store) View(read func(state *State) (any, error)) (any, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return read(s.state)
}

func cloneState(state *State) (*State, error) {
	encoded, err := json.Marshal(state)
	if err != nil {
		return nil, InternalError("database could not be serialised: %s", err)
	}
	clone := newState()
	if err := json.Unmarshal(encoded, clone); err != nil {
		return nil, InternalError("database could not be restored: %s", err)
	}
	clone.repair()
	return clone, nil
}

func writeState(path string, state *State) error {
	encoded, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return InternalError("database could not be serialised: %s", err)
	}
	encoded = append(encoded, '\n')
	if directory := filepath.Dir(path); directory != "" && directory != "." {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return InternalError("database directory could not be created: %s", err)
		}
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, encoded, 0o644); err != nil {
		return InternalError("database could not be written: %s", err)
	}
	if err := os.Rename(temporary, path); err != nil {
		return InternalError("database could not be committed: %s", err)
	}
	return nil
}
