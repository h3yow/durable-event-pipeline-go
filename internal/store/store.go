// Package store provides the local durable backing storage: per-partition
// append-only logs, a dead-letter log and a periodically checkpointed,
// atomically replaced state file (offsets, in-flight records, dedup table).
package store

import (
	"context"
	"encoding/json"
)

// Record is one persisted event record inside a partition log.
type Record struct {
	Offset   int64           `json:"offset"`
	Envelope json.RawMessage `json:"envelope"`
}

// InFlight describes a delivered-but-not-yet-acknowledged record.
type InFlight struct {
	Partition   string `json:"partition"`
	Offset      int64  `json:"offset"`
	Attempts    int    `json:"attempts"`
	DeliveredAt string `json:"delivered_at"`
	VisibleAt   string `json:"visible_at"` // earliest next delivery time (backoff)
}

// DeadLetter is a message that exhausted retries or was non-retryable.
type DeadLetter struct {
	Partition string          `json:"partition"`
	Offset    int64           `json:"offset"`
	Record    json.RawMessage `json:"record"`
	Reason    string          `json:"reason"`
	Attempts  int             `json:"attempts"`
	MovedAt   string          `json:"moved_at"`
}

// State is the full recoverable consumer state, persisted atomically.
type State struct {
	Committed map[string]int64 `json:"committed"` // partition -> highest acked offset
	InFlight  []InFlight       `json:"in_flight"`
	Processed map[string]int64 `json:"processed"` // message ID -> offset (dedup)
	Dead      []DeadLetter     `json:"dead"`
}

// Store is the durable local storage abstraction.
type Store interface {
	// Append writes a record at the next offset of the partition log.
	Append(ctx context.Context, partition string, env json.RawMessage) (int64, error)
	// Read returns records at offsets >= from in write order.
	Read(ctx context.Context, partition string, from int64) ([]Record, error)
	// HighWatermark returns the next offset to be assigned.
	HighWatermark(ctx context.Context, partition string) (int64, error)

	LoadState(ctx context.Context) (*State, error)
	SaveState(ctx context.Context, st *State) error

	AppendDead(ctx context.Context, dl DeadLetter) error
	DeadLetters(ctx context.Context) ([]DeadLetter, error)

	Close() error
}
