package store_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/h3yow/durable-event-pipeline-go/internal/store"
)

func TestAppendReadOrderAndHighWatermark(t *testing.T) {
	dir := t.TempDir()
	st, err := store.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		off, err := st.Append(ctx, "p1", json.RawMessage(`{"x":1}`))
		if err != nil {
			t.Fatal(err)
		}
		if off != int64(i) {
			t.Fatalf("offset=%d want %d", off, i)
		}
	}
	hw, _ := st.HighWatermark(ctx, "p1")
	if hw != 5 {
		t.Fatalf("watermark=%d want 5", hw)
	}
	recs, err := st.Read(ctx, "p1", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 || recs[0].Offset != 3 || recs[1].Offset != 4 {
		t.Fatalf("read from offset 3 wrong: %+v", recs)
	}
}

func TestStateRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st, _ := store.NewFileStore(dir)
	ctx := context.Background()
	want := &store.State{
		Committed: map[string]int64{"p1": 7},
		InFlight:  []store.InFlight{{Partition: "p1", Offset: 7, Attempts: 2, VisibleAt: "now"}},
		Processed: map[string]int64{"id:a": 0},
	}
	if err := st.SaveState(ctx, want); err != nil {
		t.Fatal(err)
	}
	got, err := st.LoadState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Committed["p1"] != 7 || got.Processed["id:a"] != 0 {
		t.Fatalf("state mismatch: %+v", got)
	}
	if len(got.InFlight) != 1 || got.InFlight[0].Attempts != 2 {
		t.Fatalf("inflight mismatch: %+v", got.InFlight)
	}
	if _, err := os.Stat(filepath.Join(dir, "state.json.tmp")); !os.IsNotExist(err) {
		t.Fatalf("temp state file must not remain: %v", err)
	}
}

func TestDeadLetterPersistence(t *testing.T) {
	dir := t.TempDir()
	st, _ := store.NewFileStore(dir)
	ctx := context.Background()
	err := st.AppendDead(ctx, store.DeadLetter{
		Partition: "p1", Offset: 2, Record: json.RawMessage(`{}`),
		Reason: "invalid-payload", Attempts: 1, MovedAt: "t",
	})
	if err != nil {
		t.Fatal(err)
	}
	st2, err := store.NewFileStore(dir) // reopen
	if err != nil {
		t.Fatal(err)
	}
	dls, err := st2.DeadLetters(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(dls) != 1 || dls[0].Reason != "invalid-payload" || dls[0].Offset != 2 {
		t.Fatalf("dead letter recovery mismatch: %+v", dls)
	}
}

// TestTornTailTruncation simulates a crash mid-append: a partial JSON line
// at the end of the partition log must be truncated deterministically on
// recovery instead of failing the whole broker.
func TestTornTailTruncation(t *testing.T) {
	dir := t.TempDir()
	st, _ := store.NewFileStore(dir)
	ctx := context.Background()
	if _, err := st.Append(ctx, "p1", json.RawMessage(`{"id":"good"}`)); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "partitions", "p1.log"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"offset":1,"envel`); err != nil { // torn
		t.Fatal(err)
	}
	f.Close()

	st2, err := store.NewFileStore(dir)
	if err != nil {
		t.Fatalf("recovery with torn tail must succeed: %v", err)
	}
	hw, _ := st2.HighWatermark(ctx, "p1")
	if hw != 1 {
		t.Fatalf("watermark after torn tail=%d, want 1", hw)
	}
	// Subsequent appends must continue cleanly from offset 1.
	off, err := st2.Append(ctx, "p1", json.RawMessage(`{"id":"after"}`))
	if err != nil {
		t.Fatal(err)
	}
	if off != 1 {
		t.Fatalf("offset after recovery=%d, want 1", off)
	}
}
