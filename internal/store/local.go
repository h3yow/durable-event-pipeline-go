package store

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// FileStore is a directory-backed Store implementation.
//
// Layout:
//
//	<dir>/state.json              atomic consumer state (tmp + rename)
//	<dir>/dead.log                append-only JSON-lines dead-letter log
//	<dir>/partitions/<name>.log   append-only JSON-lines partition logs
//
// Every append is fsynced before success is reported; state is written to
// a temp file, fsynced and atomically renamed, so a crash never leaves a
// half-written state file.
type FileStore struct {
	dir string
	mu  sync.Mutex

	// records[partition] holds every record ever appended (in offset order).
	records map[string][]Record
	// waters[partition] is the next offset to assign.
	waters map[string]int64

	dead []DeadLetter
}

// NewFileStore opens (or creates) the store rooted at dir and recovers all
// persisted logs.
func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(filepath.Join(dir, "partitions"), 0o755); err != nil {
		return nil, fmt.Errorf("store: mkdir: %w", err)
	}
	fs := &FileStore{
		dir:     dir,
		records: map[string][]Record{},
		waters:  map[string]int64{},
	}
	if err := fs.recoverLogs(); err != nil {
		return nil, err
	}
	if err := fs.recoverDead(); err != nil {
		return nil, err
	}
	return fs, nil
}

func (fs *FileStore) partPath(partition string) string {
	return filepath.Join(fs.dir, "partitions", escapeName(partition)+".log")
}

// Append writes a record at the next offset and fsyncs before returning.
func (fs *FileStore) Append(_ context.Context, partition string, env json.RawMessage) (int64, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	offset := fs.waters[partition]
	rec := Record{Offset: offset, Envelope: append(json.RawMessage(nil), env...)}
	line, err := json.Marshal(rec)
	if err != nil {
		return 0, fmt.Errorf("store: marshal record: %w", err)
	}
	line = append(line, '\n')

	path := fs.partPath(partition)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return 0, fmt.Errorf("store: open log: %w", err)
	}
	if _, err := f.Write(line); err != nil {
		f.Close()
		return 0, fmt.Errorf("store: write log: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return 0, fmt.Errorf("store: fsync log: %w", err)
	}
	if err := f.Close(); err != nil {
		return 0, fmt.Errorf("store: close log: %w", err)
	}

	fs.records[partition] = append(fs.records[partition], rec)
	fs.waters[partition] = offset + 1
	return offset, nil
}

// Read returns records at offsets >= from in persisted write order.
func (fs *FileStore) Read(_ context.Context, partition string, from int64) ([]Record, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	all := fs.records[partition]
	idx := int(from)
	if idx < 0 {
		idx = 0
	}
	if idx > len(all) {
		idx = len(all)
	}
	out := make([]Record, len(all)-idx)
	copy(out, all[idx:])
	return out, nil
}

// HighWatermark returns the next offset to assign in the partition.
func (fs *FileStore) HighWatermark(_ context.Context, partition string) (int64, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.waters[partition], nil
}

// LoadState reads the checkpointed state. A missing file means empty state.
func (fs *FileStore) LoadState(_ context.Context) (*State, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	data, err := os.ReadFile(filepath.Join(fs.dir, "state.json"))
	if errors.Is(err, os.ErrNotExist) {
		return fs.emptyState(), nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: read state: %w", err)
	}
	var st State
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fmt.Errorf("store: corrupt state.json: %w", err)
	}
	fs.normalizeState(&st)
	return &st, nil
}

// SaveState atomically replaces the state file (tmp + fsync + rename).
func (fs *FileStore) SaveState(_ context.Context, st *State) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.normalizeState(st)

	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("store: marshal state: %w", err)
	}
	final := filepath.Join(fs.dir, "state.json")
	tmp := filepath.Join(fs.dir, "state.json.tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("store: open state tmp: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("store: write state tmp: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("store: fsync state tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("store: close state tmp: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("store: rename state: %w", err)
	}
	return syncDir(fs.dir)
}

// AppendDead appends a dead-letter record, fsynced.
func (fs *FileStore) AppendDead(_ context.Context, dl DeadLetter) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	line, err := json.Marshal(dl)
	if err != nil {
		return fmt.Errorf("store: marshal dead letter: %w", err)
	}
	line = append(line, '\n')
	path := filepath.Join(fs.dir, "dead.log")
	if err := appendFileSync(path, line); err != nil {
		return err
	}
	fs.dead = append(fs.dead, dl)
	return nil
}

// DeadLetters returns every dead-letter record in move order.
func (fs *FileStore) DeadLetters(_ context.Context) ([]DeadLetter, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	out := make([]DeadLetter, len(fs.dead))
	copy(out, fs.dead)
	return out, nil
}

// Close releases the store. All writes are already durable.
func (fs *FileStore) Close() error { return nil }

func (fs *FileStore) emptyState() *State {
	return &State{
		Committed: map[string]int64{},
		InFlight:  []InFlight{},
		Processed: map[string]int64{},
		Dead:      []DeadLetter{},
	}
}

func (fs *FileStore) normalizeState(st *State) {
	if st.Committed == nil {
		st.Committed = map[string]int64{}
	}
	if st.Processed == nil {
		st.Processed = map[string]int64{}
	}
	if st.InFlight == nil {
		st.InFlight = []InFlight{}
	}
	if st.Dead == nil {
		st.Dead = []DeadLetter{}
	}
	// State's dead list is informational; dead.log remains the source of truth.
	st.Dead = append([]DeadLetter(nil), fs.dead...)
}

func (fs *FileStore) recoverLogs() error {
	pdir := filepath.Join(fs.dir, "partitions")
	entries, err := os.ReadDir(pdir)
	if err != nil {
		return fmt.Errorf("store: read partitions dir: %w", err)
	}
	for _, ent := range entries {
		if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".log") {
			continue
		}
		partition, err := unescapeName(strings.TrimSuffix(ent.Name(), ".log"))
		if err != nil {
			return fmt.Errorf("store: partition file %q: %w", ent.Name(), err)
		}
		path := filepath.Join(pdir, ent.Name())
		recs, water, err := readLog[Record](path)
		if err != nil {
			return fmt.Errorf("store: recover %s: %w", partition, err)
		}
		fs.records[partition] = recs
		fs.waters[partition] = water
	}
	return nil
}

func (fs *FileStore) recoverDead() error {
	recs, _, err := readLog[DeadLetter](filepath.Join(fs.dir, "dead.log"))
	if err != nil {
		return fmt.Errorf("store: recover dead log: %w", err)
	}
	fs.dead = recs
	return nil
}

// readLog reads a JSON-lines file. A torn trailing line from a crash
// mid-append is truncated deterministically so recovery always succeeds.
func readLog[T any](path string) ([]T, int64, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}

	var out []T
	var validLen int
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			validLen += len(line) + 1
			continue
		}
		var v T
		if err := json.Unmarshal(line, &v); err != nil {
			// Torn final write: stop and truncate the file back to good bytes.
			if cerr := os.Truncate(path, int64(validLen)); cerr != nil {
				return nil, 0, fmt.Errorf("truncate torn tail: %w (parse: %v)", cerr, err)
			}
			break
		}
		out = append(out, v)
		validLen += len(line) + 1 // scanner strips the trailing '\n'
	}
	if err := sc.Err(); err != nil {
		return nil, 0, err
	}

	var water int64
	switch r := any(out).(type) {
	case []Record:
		for _, rec := range r {
			if rec.Offset >= water {
				water = rec.Offset + 1
			}
		}
	}
	return out, water, nil
}

func appendFileSync(path string, line []byte) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("store: open %s: %w", path, err)
	}
	defer f.Close()
	if _, err := f.Write(line); err != nil {
		return fmt.Errorf("store: write %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("store: fsync %s: %w", path, err)
	}
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// escapeName makes an arbitrary partition name safe as a single filename.
func escapeName(name string) string {
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '.', r == '_', r == '-':
			b.WriteRune(r)
		default:
			fmt.Fprintf(&b, "%%%02X", r)
		}
	}
	return b.String()
}

func unescapeName(s string) (string, error) {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '%' {
			b.WriteByte(s[i])
			continue
		}
		if i+2 >= len(s) {
			return "", fmt.Errorf("bad escape in %q", s)
		}
		var hi, lo byte
		var err error
		if hi, err = hexVal(s[i+1]); err != nil {
			return "", err
		}
		if lo, err = hexVal(s[i+2]); err != nil {
			return "", err
		}
		b.WriteByte(hi<<4 | lo)
		i += 2
	}
	return b.String(), nil
}

func hexVal(c byte) (byte, error) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', nil
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, nil
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, nil
	}
	return 0, fmt.Errorf("bad hex byte %q", c)
}

// Partitions returns the names of all partition logs currently on disk.
func (fs *FileStore) Partitions() []string {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	out := make([]string, 0, len(fs.records))
	for p := range fs.records {
		out = append(out, p)
	}
	return out
}
