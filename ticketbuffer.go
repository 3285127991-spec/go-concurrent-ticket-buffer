// Package ticketbuffer provides a concurrency-safe, strictly ordered ticket
// buffer. Tickets are reserved as monotonically increasing uint64 values
// starting at 1, completed exactly once by Commit or Cancel, and consumed
// strictly in ticket order by Next.
package ticketbuffer

import (
	"context"
	"errors"
	"sync"
)

var (
	// ErrClosed is returned by operations rejected because the buffer is
	// closed, and by Next once the buffer is closed and fully drained.
	ErrClosed = errors.New("ticketbuffer: closed")
	// ErrInvalidTicket is returned for ticket 0.
	ErrInvalidTicket = errors.New("ticketbuffer: ticket must be >= 1")
	// ErrUnknownTicket is returned for a ticket that was never reserved.
	ErrUnknownTicket = errors.New("ticketbuffer: unknown ticket")
	// ErrAlreadyDone is returned when completing a ticket that is already
	// committed or cancelled.
	ErrAlreadyDone = errors.New("ticketbuffer: ticket already committed or cancelled")
	// ErrTicketConsumed is returned when completing a ticket that was
	// already consumed or skipped.
	ErrTicketConsumed = errors.New("ticketbuffer: ticket already consumed or skipped")
)

type state uint8

const (
	pending state = iota
	committed
	cancelled
)

type entry struct {
	state state
	value any
}

// Snapshot is an independent value copy of the buffer's counters.
type Snapshot struct {
	NextReserve uint64 // next ticket Reserve will hand out
	NextConsume uint64 // smallest ticket not yet consumed or skipped
	Pending     int    // tickets neither committed nor cancelled
	Ready       int    // tickets committed but not yet consumed
	Closed      bool
}

// Buffer is a concurrency-safe ordered ticket buffer. The zero value is not
// usable; construct it with New.
type Buffer struct {
	mu          sync.Mutex
	entries     map[uint64]*entry // exactly the tickets in [nextConsume, nextReserve)
	nextReserve uint64
	nextConsume uint64
	pending     int
	ready       int
	closed      bool
	changed     chan struct{} // closed and replaced to wake Next waiters
}

// New returns an empty, open Buffer.
func New() *Buffer {
	return &Buffer{
		entries:     make(map[uint64]*entry),
		nextReserve: 1,
		nextConsume: 1,
		changed:     make(chan struct{}),
	}
}

// Reserve allocates the next ticket. Tickets start at 1, increase strictly,
// and are never reused. Each new ticket is pending until Commit or Cancel.
func (b *Buffer) Reserve() (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, ErrClosed
	}
	ticket := b.nextReserve
	b.nextReserve++
	b.entries[ticket] = &entry{state: pending}
	b.pending++
	return ticket, nil
}

// Commit stores value for a pending ticket, making it consumable by Next
// once all lower tickets are consumed or skipped.
func (b *Buffer) Commit(ticket uint64, value any) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	e, err := b.pendingLocked(ticket)
	if err != nil {
		return err
	}
	e.state = committed
	e.value = value
	b.pending--
	b.ready++
	b.broadcastLocked()
	return nil
}

// Cancel drops a pending ticket. Next skips cancelled tickets automatically.
func (b *Buffer) Cancel(ticket uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	e, err := b.pendingLocked(ticket)
	if err != nil {
		return err
	}
	e.state = cancelled
	b.pending--
	b.advanceLocked()
	b.broadcastLocked()
	return nil
}

// pendingLocked validates that ticket names a live pending ticket.
func (b *Buffer) pendingLocked(ticket uint64) (*entry, error) {
	if ticket == 0 {
		return nil, ErrInvalidTicket
	}
	if b.closed {
		return nil, ErrClosed
	}
	if ticket < b.nextConsume {
		return nil, ErrTicketConsumed
	}
	e, ok := b.entries[ticket]
	if !ok {
		return nil, ErrUnknownTicket
	}
	if e.state != pending {
		return nil, ErrAlreadyDone
	}
	return e, nil
}

// Next returns the smallest unconsumed ticket and its value, consuming it
// permanently. It blocks while the head ticket is still pending or not yet
// reserved, skips cancelled tickets, and returns ErrClosed once the buffer
// is closed and drained. Cancelling ctx unblocks only this call; the ticket
// remains consumable by others.
func (b *Buffer) Next(ctx context.Context) (uint64, any, error) {
	for {
		b.mu.Lock()
		b.advanceLocked()
		e, ok := b.entries[b.nextConsume]
		if ok && e.state == committed {
			delete(b.entries, b.nextConsume)
			ticket := b.nextConsume
			b.nextConsume++
			b.ready--
			b.advanceLocked()
			b.broadcastLocked()
			b.mu.Unlock()
			return ticket, e.value, nil
		}
		if b.closed && !ok {
			b.mu.Unlock()
			return 0, nil, ErrClosed
		}
		changed := b.changed
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		case <-changed:
		}
	}
}

// Snapshot returns an independent copy of the buffer's current counters.
func (b *Buffer) Snapshot() Snapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	return Snapshot{
		NextReserve: b.nextReserve,
		NextConsume: b.nextConsume,
		Pending:     b.pending,
		Ready:       b.ready,
		Closed:      b.closed,
	}
}

// Len reports the number of committed but not yet consumed tickets.
func (b *Buffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ready
}

// Close rejects further Reserve, Commit, and Cancel calls and atomically
// cancels every pending ticket. Committed values remain consumable in order;
// once drained, Next returns ErrClosed. Close is idempotent.
func (b *Buffer) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	for _, e := range b.entries {
		if e.state == pending {
			e.state = cancelled
		}
	}
	b.pending = 0
	b.advanceLocked()
	b.broadcastLocked()
	return nil
}

// advanceLocked skips cancelled tickets at the consumption head.
func (b *Buffer) advanceLocked() {
	for {
		e, ok := b.entries[b.nextConsume]
		if !ok || e.state != cancelled {
			return
		}
		delete(b.entries, b.nextConsume)
		b.nextConsume++
	}
}

func (b *Buffer) broadcastLocked() {
	close(b.changed)
	b.changed = make(chan struct{})
}
