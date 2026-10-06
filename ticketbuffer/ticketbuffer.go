// Package ticketbuffer provides a concurrency-safe buffer that hands out
// strictly increasing tickets and lets callers commit values out of order,
// while consumers always read values back in exact ticket order.
package ticketbuffer

import (
	"context"
	"errors"
	"sync"
)

var (
	// ErrClosed is returned by operations rejected because the buffer is
	// closed, and by Next once the buffer is closed and fully drained.
	ErrClosed = errors.New("ticketbuffer: buffer is closed")
	// ErrTicketZero is returned when ticket 0 is passed to Commit or Cancel.
	ErrTicketZero = errors.New("ticketbuffer: ticket must be >= 1")
	// ErrUnknownTicket is returned for tickets that were never reserved or
	// have already been consumed.
	ErrUnknownTicket = errors.New("ticketbuffer: unknown ticket")
	// ErrTicketFinalized is returned when a ticket was already committed or
	// cancelled; every ticket can be finalized exactly once.
	ErrTicketFinalized = errors.New("ticketbuffer: ticket already committed or cancelled")
)

type entry struct {
	committed bool
	value     any
}

// Snapshot is an independent copy of the buffer's counters at one instant.
type Snapshot struct {
	NextReserve uint64 // next ticket Reserve will hand out
	NextConsume uint64 // next ticket Next will try to consume
	Pending     int    // reserved but not yet committed or cancelled
	Ready       int    // committed but not yet consumed
	Closed      bool
}

// Buffer is a concurrent ordered ticket buffer. The zero value is not
// usable; create one with New.
type Buffer struct {
	mu      sync.Mutex
	changed chan struct{} // closed and replaced on every state change
	next    uint64        // next ticket to reserve
	consume uint64        // next ticket to consume
	entries map[uint64]*entry
	pending int
	ready   int
	closed  bool
}

// New returns an empty Buffer. The first reserved ticket is 1.
func New() *Buffer {
	return &Buffer{
		changed: make(chan struct{}),
		next:    1,
		consume: 1,
		entries: make(map[uint64]*entry),
	}
}

// broadcast wakes every waiter. Callers must hold b.mu.
func (b *Buffer) broadcast() {
	close(b.changed)
	b.changed = make(chan struct{})
}

// Reserve allocates the next ticket and returns it. Tickets start at 1,
// increase strictly and are never reused. It fails once the buffer is closed.
func (b *Buffer) Reserve() (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, ErrClosed
	}
	ticket := b.next
	b.next++
	b.entries[ticket] = &entry{}
	b.pending++
	return ticket, nil
}

// finalize validates a Commit/Cancel transition. Callers must hold b.mu.
func (b *Buffer) finalize(ticket uint64) (*entry, error) {
	if ticket == 0 {
		return nil, ErrTicketZero
	}
	if b.closed {
		return nil, ErrClosed
	}
	e, ok := b.entries[ticket]
	if !ok {
		return nil, ErrUnknownTicket
	}
	if e.committed {
		return nil, ErrTicketFinalized
	}
	return e, nil
}

// Commit attaches a value to a pending ticket. Commits may happen in any
// order; consumption order is unaffected.
func (b *Buffer) Commit(ticket uint64, value any) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	e, err := b.finalize(ticket)
	if err != nil {
		return err
	}
	e.committed = true
	e.value = value
	b.pending--
	b.ready++
	b.broadcast()
	return nil
}

// Cancel drops a pending ticket; Next will skip over it.
func (b *Buffer) Cancel(ticket uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, err := b.finalize(ticket)
	if err != nil {
		return err
	}
	delete(b.entries, ticket)
	b.pending--
	b.broadcast()
	return nil
}

// Next returns the lowest unconsumed ticket that is committed, consuming it
// permanently. Cancelled tickets are skipped silently. Next blocks until a
// ticket becomes consumable, ctx is cancelled, or the buffer is closed and
// drained. A cancelled ctx only affects this call: if ctx is already done,
// Next returns ctx.Err() without consuming, even if a ticket is ready.
func (b *Buffer) Next(ctx context.Context) (uint64, any, error) {
	b.mu.Lock()
	for {
		if err := ctx.Err(); err != nil {
			b.mu.Unlock()
			return 0, nil, err
		}
		// Skip cancelled tickets: entries are deleted on cancel, and every
		// ticket below b.next was reserved, so a gap below b.next can only
		// be a cancelled ticket.
		for b.consume < b.next {
			e, ok := b.entries[b.consume]
			if !ok {
				b.consume++
				continue
			}
			if !e.committed {
				break // head ticket still pending: must wait
			}
			ticket := b.consume
			b.consume++
			delete(b.entries, ticket)
			b.ready--
			value := e.value
			b.mu.Unlock()
			return ticket, value, nil
		}
		if b.closed {
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
		b.mu.Lock()
	}
}

// Snapshot returns an independent copy of the buffer's current counters.
func (b *Buffer) Snapshot() Snapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	return Snapshot{
		NextReserve: b.next,
		NextConsume: b.consume,
		Pending:     b.pending,
		Ready:       b.ready,
		Closed:      b.closed,
	}
}

// Len reports how many tickets are committed but not yet consumed.
func (b *Buffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ready
}

// Close rejects further Reserve, Commit and Cancel calls and atomically
// cancels every still-pending ticket. Values committed before Close remain
// consumable in ticket order; once drained, Next returns ErrClosed.
// Close is idempotent.
func (b *Buffer) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	for ticket, e := range b.entries {
		if !e.committed {
			delete(b.entries, ticket)
			b.pending--
		}
	}
	b.broadcast()
}
