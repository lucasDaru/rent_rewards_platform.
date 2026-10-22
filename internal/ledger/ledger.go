// Package ledger stores points entries.
//
// In production the Store is backed by Postgres (table ledger_entries).
// MemoryStore is the stand-in used for local runs and tests.
package ledger

import (
	"context"
	"errors"
	"sync"
	"time"
)

type Kind string

const (
	KindAward    Kind = "award"
	KindReversal Kind = "reversal"
)

// Entry is one line of a member's points ledger.
type Entry struct {
	MemberID  string
	PaymentID string
	Kind      Kind
	Points    int64  // positive for awards, negative for reversals
	Month     string // settlement month of the payment, "2006-01" in UTC
}

// ErrUnavailable is returned when the ledger cannot be reached. Retrying may succeed.
var ErrUnavailable = errors.New("ledger: unavailable")

type Tx interface {
	HasEntry(paymentID string, kind Kind) bool
	Find(paymentID string, kind Kind) (Entry, bool)
	AwardedInMonth(month string) int64
	Append(e Entry)
}

type Store interface {
	HasEntry(ctx context.Context, paymentID string, kind Kind) (bool, error)
	AwardedInMonth(ctx context.Context, memberID, month string) (int64, error)
	Append(ctx context.Context, e Entry) error
	Balance(ctx context.Context, memberID string) (int64, error)
	Transact(ctx context.Context, memberID string, fn func(Tx) error) error
}

type Option func(*MemoryStore)

// WithLatency delays every call, approximating a database round trip.
func WithLatency(d time.Duration) Option {
	return func(s *MemoryStore) { s.latency = d }
}

// WithFaults installs a hook run at the start of every call (after the
// round trip). A non-nil error is returned to the caller and the call has no effect.
func WithFaults(f func(op string) error) Option {
	return func(s *MemoryStore) { s.faults = f }
}

type memberState struct {
	mu      sync.Mutex
	entries []Entry
}

type MemoryStore struct {
	latency time.Duration
	faults  func(op string) error

	mu      sync.Mutex
	members map[string]*memberState
}

func NewMemoryStore(opts ...Option) *MemoryStore {
	s := &MemoryStore{members: make(map[string]*memberState)}
	for _, o := range opts {
		o(s)
	}
	return s
}

func (s *MemoryStore) roundTrip(ctx context.Context, op string) error {
	if s.latency > 0 {
		select {
		case <-time.After(s.latency):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if s.faults != nil {
		return s.faults(op)
	}
	return nil
}

func (s *MemoryStore) member(id string) *memberState {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.members[id]
	if !ok {
		st = &memberState{}
		s.members[id] = st
	}
	return st
}

func (s *MemoryStore) HasEntry(ctx context.Context, paymentID string, kind Kind) (bool, error) {
	if err := s.roundTrip(ctx, "has_entry"); err != nil {
		return false, err
	}
	s.mu.Lock()
	states := make([]*memberState, 0, len(s.members))
	for _, st := range s.members {
		states = append(states, st)
	}
	s.mu.Unlock()
	for _, st := range states {
		st.mu.Lock()
		found := findEntry(st.entries, paymentID, kind)
		st.mu.Unlock()
		if found {
			return true, nil
		}
	}
	return false, nil
}

func (s *MemoryStore) AwardedInMonth(ctx context.Context, memberID, month string) (int64, error) {
	if err := s.roundTrip(ctx, "awarded_in_month"); err != nil {
		return 0, err
	}
	st := s.member(memberID)
	st.mu.Lock()
	defer st.mu.Unlock()
	return awardedInMonth(st.entries, month), nil
}

func (s *MemoryStore) Append(ctx context.Context, e Entry) error {
	if err := s.roundTrip(ctx, "append"); err != nil {
		return err
	}
	st := s.member(e.MemberID)
	st.mu.Lock()
	defer st.mu.Unlock()
	st.entries = append(st.entries, e)
	return nil
}

func (s *MemoryStore) Balance(ctx context.Context, memberID string) (int64, error) {
	if err := s.roundTrip(ctx, "balance"); err != nil {
		return 0, err
	}
	st := s.member(memberID)
	st.mu.Lock()
	defer st.mu.Unlock()
	var total int64
	for _, e := range st.entries {
		total += e.Points
	}
	return total, nil
}

func (s *MemoryStore) Transact(ctx context.Context, memberID string, fn func(Tx) error) error {
	if err := s.roundTrip(ctx, "transact"); err != nil {
		return err
	}
	st := s.member(memberID)
	st.mu.Lock()
	defer st.mu.Unlock()
	tx := &memTx{entries: append([]Entry(nil), st.entries...)}
	if err := fn(tx); err != nil {
		return err
	}
	st.entries = tx.entries
	return nil
}

type memTx struct {
	entries []Entry
}

func (t *memTx) HasEntry(paymentID string, kind Kind) bool {
	return findEntry(t.entries, paymentID, kind)
}

func (t *memTx) Find(paymentID string, kind Kind) (Entry, bool) {
	for _, e := range t.entries {
		if e.PaymentID == paymentID && e.Kind == kind {
			return e, true
		}
	}
	return Entry{}, false
}

func (t *memTx) AwardedInMonth(month string) int64 {
	return awardedInMonth(t.entries, month)
}

func (t *memTx) Append(e Entry) {
	t.entries = append(t.entries, e)
}

func findEntry(entries []Entry, paymentID string, kind Kind) bool {
	for _, e := range entries {
		if e.PaymentID == paymentID && e.Kind == kind {
			return true
		}
	}
	return false
}

func awardedInMonth(entries []Entry, month string) int64 {
	var total int64
	for _, e := range entries {
		if e.Month == month && e.Kind == KindAward {
			total += e.Points
		}
	}
	return total
}
