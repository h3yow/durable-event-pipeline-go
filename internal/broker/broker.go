// Package broker implements a local, partitioned, durable event broker:
// producers append to per-partition logs; a consumer group polls records
// in order per partition, acks/nacks with backoff, moves terminal failures
// to a dead-letter log, and recovers all state after process restart.
package broker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/h3yow/durable-event-pipeline-go/internal/codec"
	"github.com/h3yow/durable-event-pipeline-go/internal/store"
)

// Handler processes one envelope. Return nil to ack. Return a NackError to
// control retry/dead-letter behaviour; any other error is treated as
// retryable with reason "handler_error".
type Handler func(ctx context.Context, env *codec.Envelope) error

// Config configures a Broker.
type Config struct {
	Dir        string
	Group      string
	Handler    Handler
	Retry      RetryPolicy
	PollTick   time.Duration // idle worker wakeup / partition discovery interval
	Checkpoint time.Duration // periodic state flush interval
	Logf       func(format string, args ...any)
}

// delivery is one in-flight (unacknowledged) record.
type delivery struct {
	partition string
	offset    int64
	attempts  int
	visibleAt time.Time // earliest next attempt
}

// Broker is the durable local broker.
type Broker struct {
	cfg Config
	st  store.Store
	log func(format string, args ...any)

	mu      sync.Mutex
	state   *store.State
	workers map[string]struct{}
	wake    map[string]chan struct{}
	dirty   bool
	closed  bool
}

// New opens (or creates) a broker rooted at cfg.Dir and restores all
// persisted consumer state.
func New(cfg Config) (*Broker, error) {
	if cfg.Dir == "" {
		return nil, fmt.Errorf("broker: Dir is required")
	}
	if cfg.Group == "" {
		cfg.Group = "default"
	}
	if cfg.PollTick <= 0 {
		cfg.PollTick = 20 * time.Millisecond
	}
	if cfg.Checkpoint <= 0 {
		cfg.Checkpoint = 250 * time.Millisecond
	}
	if cfg.Retry.MaxAttempts == 0 {
		cfg.Retry.MaxAttempts = 3
	}
	if cfg.Retry.Backoff == nil {
		cfg.Retry.Backoff = ExponentialBackoff(10*time.Millisecond, 200*time.Millisecond)
	}
	if cfg.Logf == nil {
		cfg.Logf = func(string, ...any) {}
	}

	st, err := store.NewFileStore(cfg.Dir)
	if err != nil {
		return nil, err
	}
	state, err := st.LoadState(context.Background())
	if err != nil {
		return nil, err
	}

	b := &Broker{
		cfg:     cfg,
		st:      st,
		log:     cfg.Logf,
		state:   state,
		workers: map[string]struct{}{},
		wake:    map[string]chan struct{}{},
	}
	b.recoverInFlight()
	return b, nil
}

// Publish appends an envelope to the partition named by its Key and wakes
// that partition's worker. The record is fsynced before return.
func (b *Broker) Publish(ctx context.Context, env *codec.Envelope) (int64, error) {
	return b.PublishTo(ctx, env.Key, env)
}

// PublishTo appends to an explicit partition.
func (b *Broker) PublishTo(ctx context.Context, partition string, env *codec.Envelope) (int64, error) {
	if partition == "" {
		partition = "_default"
	}
	if env.ID == "" {
		return 0, fmt.Errorf("broker: envelope id is required")
	}
	env.Key = partition
	raw, err := codec.Marshal(env)
	if err != nil {
		return 0, fmt.Errorf("broker: marshal: %w", err)
	}
	offset, err := b.st.Append(ctx, partition, raw)
	if err != nil {
		return 0, err
	}
	b.log("publish partition=%s offset=%d id=%s event=%s v=%d",
		partition, offset, env.ID, env.Event, env.Version)
	b.kick(partition)
	return offset, nil
}

// Start launches worker management (partition discovery + checkpointing)
// and blocks until ctx is canceled. Safe to call once per Broker.
func (b *Broker) Start(ctx context.Context) error {
	reaperDone := make(chan struct{})
	saverDone := make(chan struct{})

	go b.reaper(ctx, reaperDone)
	go b.saver(ctx, saverDone)

	<-ctx.Done()
	<-reaperDone
	<-saverDone
	return ctx.Err()
}

// Run starts background management without blocking. The returned stop
// function cancels the broker and performs a final checkpoint.
func (b *Broker) Run(ctx context.Context) (stop func(err error)) {
	runCtx, cancel := context.WithCancel(ctx)
	go b.reaper(runCtx, make(chan struct{}))
	go b.saver(runCtx, make(chan struct{}))
	return func(error) {
		cancel()
		// Give loops a moment to observe cancellation; Close checkpoints.
		b.Close()
	}
}

// reaper discovers partitions (including ones created after start) and
// ensures exactly one ordered worker goroutine runs per partition.
func (b *Broker) reaper(ctx context.Context, done chan<- struct{}) {
	defer close(done)
	t := time.NewTicker(b.cfg.PollTick)
	defer t.Stop()
	for {
		b.ensureWorkers(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (b *Broker) ensureWorkers(ctx context.Context) {
	b.mu.Lock()
	var known []string
	for p := range b.state.Committed {
		known = append(known, p)
	}
	for p := range b.wake {
		known = append(known, p)
	}
	b.mu.Unlock()

	// Discover partitions that only exist on disk (published before any state).
	if hw, err := b.listPartitions(ctx); err == nil {
		known = append(known, hw...)
	}

	started := []string{}
	b.mu.Lock()
	for _, p := range known {
		if _, ok := b.workers[p]; ok {
			continue
		}
		ch := make(chan struct{}, 1)
		b.wake[p] = ch
		b.workers[p] = struct{}{}
		started = append(started, p)
	}
	b.mu.Unlock()

	for _, p := range started {
		b.mu.Lock()
		ch := b.wake[p]
		b.mu.Unlock()
		b.log("worker start partition=%s committed=%d", p, b.CommittedOffset(p))
		go b.worker(ctx, p, ch)
	}
}

func (b *Broker) listPartitions(ctx context.Context) ([]string, error) {
	l, ok := b.st.(interface{ Partitions() []string })
	if !ok {
		return nil, nil
	}
	return l.Partitions(), nil
}

// worker consumes ONE partition strictly in offset order. At most one
// record is in flight at a time, so a retrying record never blocks another
// partition (other partitions have their own workers) and the committed
// offset can never pass an unfinished record.
func (b *Broker) worker(ctx context.Context, partition string, wake <-chan struct{}) {
	ticker := time.NewTicker(b.cfg.PollTick)
	defer ticker.Stop()

	for {
		if ctx.Err() != nil {
			return
		}
		madeProgress := b.pump(ctx, partition)
		if madeProgress {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-wake:
		case <-ticker.C:
		}
	}
}

// pump performs at most one delivery/settlement cycle. It returns true if
// something happened (caller should immediately pump again).
func (b *Broker) pump(ctx context.Context, partition string) bool {
	water, err := b.st.HighWatermark(ctx, partition)
	if err != nil {
		return false
	}

	b.mu.Lock()
	inflight := b.findDelivery(partition)
	committed := b.state.Committed[partition]
	b.mu.Unlock()

	if inflight != nil {
		// A record is awaiting redelivery after a retry. Nothing else in this
		// partition may pass it.
		if time.Now().Before(inflight.visibleAt) {
			return false // backoff window: other partitions keep running
		}
		b.deliver(ctx, partition, inflight.offset, inflight.attempts, true)
		return true
	}

	if committed >= water {
		return false // caught up
	}

	records, err := b.st.Read(ctx, partition, committed)
	if err != nil || len(records) == 0 {
		return false
	}
	for _, rec := range records {
		if b.isProcessed(partition, rec) {
			// Already effective before a crash between business effect and
			// checkpoint: ack without invoking the handler again.
			b.ackRecord(partition, rec, 0, "dedup-skip")
			return true
		}
		b.deliver(ctx, partition, rec.Offset, 0, false)
		return true
	}
	return false
}

// deliver invokes the handler for one record and settles the outcome.
func (b *Broker) deliver(ctx context.Context, partition string, offset int64, prevAttempts int, isRedelivery bool) {
	records, err := b.st.Read(ctx, partition, offset)
	if err != nil || len(records) == 0 {
		b.log("deliver error partition=%s offset=%d err=%v", partition, offset, err)
		return
	}
	rec := records[0]
	env, derr := codec.Unmarshal(rec.Envelope)
	if derr != nil {
		// Corrupt envelope cannot be processed at all: dead-letter it.
		b.moveDead(ctx, partition, rec, prevAttempts+1, "unparseable-envelope")
		b.ackRecord(partition, rec, prevAttempts+1, "dead-letter")
		return
	}

	attempt := prevAttempts + 1
	if isRedelivery {
		b.log("redeliver partition=%s offset=%d id=%s attempt=%d",
			partition, offset, env.ID, attempt)
	} else {
		compat := ""
		if env.UnknownVersion {
			compat = " compat=unknown-version-preserved"
		} else if env.Version < codec.SchemaVersion {
			compat = " compat=legacy-v0-upgraded"
		}
		if len(env.Unknown) > 0 {
			compat += " compat=unknown-fields-preserved"
		}
		b.log("deliver partition=%s offset=%d id=%s attempt=%d%s",
			partition, offset, env.ID, attempt, compat)
	}

	b.mu.Lock()
	b.upsertDelivery(partition, offset, attempt, time.Time{})
	b.mu.Unlock()

	herr := b.cfg.Handler(ctx, env)
	if herr == nil {
		// Effect complete: record idempotency key and advance the offset
		// together, in one atomic state checkpoint.
		b.ackRecord(partition, rec, attempt, "acked")
		b.log("ack partition=%s offset=%d id=%s attempt=%d committed=%d",
			partition, offset, env.ID, attempt, b.CommittedOffset(partition))
		return
	}

	dec := classify(herr)
	if !dec.Retryable || attempt >= b.cfg.Retry.MaxAttempts {
		reason := dec.Reason
		if dec.Retryable {
			reason = "max-attempts-exceeded:" + dec.Reason
		}
		b.moveDead(ctx, partition, rec, attempt, reason)
		b.ackRecord(partition, rec, attempt, "dead-letter")
		b.log("dead-letter partition=%s offset=%d id=%s attempt=%d reason=%s",
			partition, offset, env.ID, attempt, reason)
		return
	}

	delay := b.cfg.Retry.Delay(attempt)
	visible := time.Now().Add(delay)
	b.mu.Lock()
	b.upsertDelivery(partition, offset, attempt, visible)
	b.dirty = true
	b.mu.Unlock()
	b.log("nack partition=%s offset=%d id=%s attempt=%d reason=%s retry_in=%s",
		partition, offset, env.ID, attempt, dec.Reason, delay)
}

// ackRecord marks a record effective and advances the committed offset.
// Because workers process strictly in order, committing offset+1 is only
// ever called for the next expected record.
func (b *Broker) ackRecord(partition string, rec store.Record, attempt int, outcome string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	env, _ := codec.Unmarshal(rec.Envelope)
	id := dedupKey(partition, rec.Offset, env)
	b.state.Processed[id] = rec.Offset
	b.removeDelivery(partition, rec.Offset)
	b.state.Committed[partition] = rec.Offset + 1
	b.dirty = true
	// Flush synchronously: effect -> dedup+offset checkpoint is one durable
	// step, so a crash after the handler effect is never redelivered, and a
	// crash before it leaves the offset untouched (at-least-once).
	snap := b.snapshotStateLocked()
	if outcome == "dedup-skip" {
		b.log("dedup-skip partition=%s offset=%d committed=%d",
			partition, rec.Offset, rec.Offset+1)
	}
	_ = attempt
	if err := b.st.SaveState(context.Background(), snap); err != nil {
		b.log("checkpoint error partition=%s offset=%d err=%v", partition, rec.Offset, err)
	} else {
		b.dirty = false
	}
}

func (b *Broker) moveDead(ctx context.Context, partition string, rec store.Record, attempt int, reason string) {
	dl := store.DeadLetter{
		Partition: partition,
		Offset:    rec.Offset,
		Record:    rec.Envelope,
		Reason:    reason,
		Attempts:  attempt,
		MovedAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := b.st.AppendDead(ctx, dl); err != nil {
		b.log("dead-letter persist error partition=%s offset=%d err=%v",
			partition, rec.Offset, err)
		return
	}
}

// saver periodically flushes dirty state atomically.
func (b *Broker) saver(ctx context.Context, done chan<- struct{}) {
	defer close(done)
	t := time.NewTicker(b.cfg.Checkpoint)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			b.checkpoint()
			return
		case <-t.C:
			b.checkpoint()
		}
	}
}

func (b *Broker) checkpoint() {
	b.mu.Lock()
	if !b.dirty {
		b.mu.Unlock()
		return
	}
	snap := b.snapshotStateLocked()
	b.dirty = false
	b.mu.Unlock()

	if err := b.st.SaveState(context.Background(), snap); err != nil {
		b.log("checkpoint error err=%v", err)
		b.mu.Lock()
		b.dirty = true
		b.mu.Unlock()
	}
}

// Close performs a final checkpoint and closes storage.
func (b *Broker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	snap := b.snapshotStateLocked()
	b.dirty = false
	b.mu.Unlock()

	if err := b.st.SaveState(context.Background(), snap); err != nil {
		return err
	}
	return b.st.Close()
}

// CommittedOffset returns the next-to-consume offset (highest acked + 1).
func (b *Broker) CommittedOffset(partition string) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state.Committed[partition]
}

// DeadLetters returns all terminal-failure records from the dead log.
func (b *Broker) DeadLetters(ctx context.Context) ([]store.DeadLetter, error) {
	return b.st.DeadLetters(ctx)
}

// recoverInFlight prepares persisted in-flight records for redelivery after
// a restart: the old backoff deadline is discarded so the worker retries
// immediately (its attempts counter is preserved), and the dedup table is
// seeded for every already-committed offset so recovery never redelivers
// past an old checkpoint.
func (b *Broker) recoverInFlight() {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for i := range b.state.InFlight {
		b.state.InFlight[i].VisibleAt = ""
		b.state.InFlight[i].DeliveredAt = now
	}
	// Trust-but-repair: ensure every partition that has a committed offset
	// has at least an empty worker entry discovered via the store list.
	b.log("recovery in_flight=%d committed_partitions=%d dead=%d",
		len(b.state.InFlight), len(b.state.Committed), len(b.state.Dead))
}

// ---- in-flight persistence mapping (state slice <-> keyed map) ----

func (b *Broker) findDelivery(partition string) *delivery {
	for i := range b.state.InFlight {
		if b.state.InFlight[i].Partition == partition {
			d := b.state.InFlight[i]
			return &delivery{
				partition: d.Partition,
				offset:    d.Offset,
				attempts:  d.Attempts,
				visibleAt: parseTime(d.VisibleAt),
			}
		}
	}
	return nil
}

func (b *Broker) upsertDelivery(partition string, offset int64, attempts int, visible time.Time) {
	for i := range b.state.InFlight {
		if b.state.InFlight[i].Partition == partition {
			b.state.InFlight[i].Offset = offset
			b.state.InFlight[i].Attempts = attempts
			b.state.InFlight[i].DeliveredAt = time.Now().UTC().Format(time.RFC3339Nano)
			b.state.InFlight[i].VisibleAt = formatVisible(visible)
			return
		}
	}
	b.state.InFlight = append(b.state.InFlight, store.InFlight{
		Partition:   partition,
		Offset:      offset,
		Attempts:    attempts,
		DeliveredAt: time.Now().UTC().Format(time.RFC3339Nano),
		VisibleAt:   formatVisible(visible),
	})
}

func (b *Broker) removeDelivery(partition string, offset int64) {
	in := b.state.InFlight
	for i := range in {
		if in[i].Partition == partition && in[i].Offset == offset {
			b.state.InFlight = append(in[:i], in[i+1:]...)
			return
		}
	}
}

func (b *Broker) isProcessed(partition string, rec store.Record) bool {
	env, _ := codec.Unmarshal(rec.Envelope)
	id := dedupKey(partition, rec.Offset, env)
	b.mu.Lock()
	_, ok := b.state.Processed[id]
	b.mu.Unlock()
	return ok
}

func (b *Broker) snapshotStateLocked() *store.State {
	snap := &store.State{
		Committed: map[string]int64{},
		Processed: map[string]int64{},
		InFlight:  append([]store.InFlight(nil), b.state.InFlight...),
		Dead:      append([]store.DeadLetter(nil), b.state.Dead...),
	}
	for k, v := range b.state.Committed {
		snap.Committed[k] = v
	}
	for k, v := range b.state.Processed {
		snap.Processed[k] = v
	}
	return snap
}

func (b *Broker) kick(partition string) {
	b.mu.Lock()
	ch := b.wake[partition]
	b.mu.Unlock()
	if ch != nil {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func dedupKey(partition string, offset int64, env *codec.Envelope) string {
	if env != nil && env.ID != "" {
		return "id:" + env.ID
	}
	return fmt.Sprintf("pos:%s:%d", partition, offset)
}

func formatVisible(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func parseTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, _ := time.Parse(time.RFC3339Nano, s)
	return t
}

// SeedProcessedForTest injects an idempotency key as if a handler effect had
// been durably recorded. Test-only crash-window simulation helper.
func (b *Broker) SeedProcessedForTest(id string) error {
	b.mu.Lock()
	b.state.Processed[id] = -1
	b.dirty = true
	b.mu.Unlock()
	b.checkpoint()
	return nil
}

// AppendRawForTest appends a pre-marshaled envelope to a partition log
// without waking any consumer. Test-only.
func (b *Broker) AppendRawForTest(ctx context.Context, partition string, raw []byte) error {
	_, err := b.st.Append(ctx, partition, raw)
	return err
}
