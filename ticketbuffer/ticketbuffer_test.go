package ticketbuffer

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func mustReserve(t *testing.T, b *Buffer) uint64 {
	t.Helper()
	ticket, err := b.Reserve()
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	return ticket
}

func TestReserveAllocatesSequentialTickets(t *testing.T) {
	b := New()
	defer b.Close()
	for want := uint64(1); want <= 3; want++ {
		if got := mustReserve(t, b); got != want {
			t.Fatalf("Reserve = %d, want %d", got, want)
		}
	}
	// Consumed or cancelled tickets are never reused.
	if err := b.Commit(1, "a"); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := b.Cancel(2); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if _, _, err := b.Next(context.Background()); err != nil {
		t.Fatalf("Next: %v", err)
	}
	if got := mustReserve(t, b); got != 4 {
		t.Fatalf("Reserve after consume = %d, want 4", got)
	}
}

func TestNextBlocksUntilHeadCommitted(t *testing.T) {
	b := New()
	defer b.Close()
	mustReserve(t, b) // 1
	mustReserve(t, b) // 2
	if err := b.Commit(2, "b"); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	type result struct {
		ticket uint64
		value  any
	}
	done := make(chan result, 1)
	go func() {
		ticket, value, err := b.Next(context.Background())
		if err != nil {
			t.Errorf("Next: %v", err)
		}
		done <- result{ticket, value}
	}()

	// Ticket 1 is still pending, so Next must not return ticket 2.
	select {
	case r := <-done:
		t.Fatalf("Next returned %+v while head ticket pending", r)
	default:
	}
	if snap := b.Snapshot(); snap.NextConsume != 1 || snap.Ready != 1 {
		t.Fatalf("Snapshot = %+v, want NextConsume=1 Ready=1", snap)
	}

	if err := b.Commit(1, "a"); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if r := <-done; r.ticket != 1 || r.value != "a" {
		t.Fatalf("Next = %+v, want ticket 1 value a", r)
	}
	ticket, value, err := b.Next(context.Background())
	if err != nil || ticket != 2 || value != "b" {
		t.Fatalf("Next = (%d, %v, %v), want (2, b, nil)", ticket, value, err)
	}
}

func TestCancelSkipsTicket(t *testing.T) {
	b := New()
	defer b.Close()
	mustReserve(t, b) // 1
	mustReserve(t, b) // 2
	if err := b.Commit(2, "b"); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	done := make(chan uint64, 1)
	go func() {
		ticket, _, err := b.Next(context.Background())
		if err != nil {
			t.Errorf("Next: %v", err)
		}
		done <- ticket
	}()

	// Cancelling the pending head unblocks Next, which skips to ticket 2.
	if err := b.Cancel(1); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if ticket := <-done; ticket != 2 {
		t.Fatalf("Next = %d, want 2", ticket)
	}
	if snap := b.Snapshot(); snap.NextConsume != 3 || snap.Pending != 0 {
		t.Fatalf("Snapshot = %+v, want NextConsume=3 Pending=0", snap)
	}
}

func TestConcurrentNextConsumesEachTicketOnce(t *testing.T) {
	b := New()
	defer b.Close()
	const total = 32
	for i := 0; i < total; i++ {
		mustReserve(t, b)
	}
	// Commit out of order to prove consumption stays ordered.
	for i := total; i >= 1; i-- {
		if err := b.Commit(uint64(i), i); err != nil {
			t.Fatalf("Commit(%d): %v", i, err)
		}
	}

	results := make(chan uint64, total)
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				ticket, _, err := b.Next(context.Background())
				if errors.Is(err, ErrClosed) {
					return
				}
				if err != nil {
					t.Errorf("Next: %v", err)
					return
				}
				results <- ticket
			}
		}()
	}
	seen := make(map[uint64]int)
	for i := 0; i < total; i++ {
		seen[<-results]++
	}
	b.Close()
	wg.Wait()

	var max uint64
	for ticket := range seen {
		if ticket > max {
			max = ticket
		}
	}
	if len(seen) != total || max != total {
		t.Fatalf("consumed %d distinct tickets (max %d), want %d", len(seen), max, total)
	}
	for ticket := uint64(1); ticket <= total; ticket++ {
		if seen[ticket] != 1 {
			t.Fatalf("ticket %d consumed %d times, want 1", ticket, seen[ticket])
		}
	}
}

func TestInvalidStateTransitions(t *testing.T) {
	b := New()
	defer b.Close()
	one := mustReserve(t, b)
	two := mustReserve(t, b)
	three := mustReserve(t, b)

	if !errors.Is(b.Commit(0, "x"), ErrTicketZero) || !errors.Is(b.Cancel(0), ErrTicketZero) {
		t.Fatal("ticket 0 must return ErrTicketZero")
	}
	if !errors.Is(b.Commit(99, "x"), ErrUnknownTicket) || !errors.Is(b.Cancel(99), ErrUnknownTicket) {
		t.Fatal("unknown ticket must return ErrUnknownTicket")
	}

	if err := b.Commit(one, "a"); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := b.Cancel(two); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	for _, op := range []struct {
		name string
		err  error
	}{
		{"double commit", b.Commit(one, "again")},
		{"commit after commit", b.Commit(one, "again")},
		{"cancel after commit", b.Cancel(one)},
		{"commit after cancel", b.Commit(two, "x")},
		{"double cancel", b.Cancel(two)},
	} {
		if !errors.Is(op.err, ErrTicketFinalized) && !errors.Is(op.err, ErrUnknownTicket) {
			t.Fatalf("%s: got %v, want ErrTicketFinalized or ErrUnknownTicket", op.name, op.err)
		}
	}
	// A pending ticket is unaffected by other tickets' transitions.
	if err := b.Commit(three, "c"); err != nil {
		t.Fatalf("Commit pending ticket: %v", err)
	}
}

func TestNextContextCancel(t *testing.T) {
	b := New()
	defer b.Close()
	mustReserve(t, b) // stays pending

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, err := b.Next(ctx)
		done <- err
	}()
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Next error = %v, want context.Canceled", err)
	}

	// The cancelled wait must not disturb the ticket itself.
	if err := b.Commit(1, "a"); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	ticket, value, err := b.Next(context.Background())
	if err != nil || ticket != 1 || value != "a" {
		t.Fatalf("Next = (%d, %v, %v), want (1, a, nil)", ticket, value, err)
	}

	// An already-cancelled ctx never consumes a ready ticket.
	if _, err := b.Reserve(); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if err := b.Commit(2, "b"); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if _, _, err := b.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Next with cancelled ctx = %v, want context.Canceled", err)
	}
	if got := b.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1 (ticket 2 untouched)", got)
	}
}

func TestCloseDrainsCommittedThenBlocks(t *testing.T) {
	b := New()
	for i := 1; i <= 4; i++ {
		mustReserve(t, b)
	}
	if err := b.Commit(2, "b"); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := b.Commit(4, "d"); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	b.Close()

	// After Close: no new reservations or finalizations.
	if _, err := b.Reserve(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Reserve after Close = %v, want ErrClosed", err)
	}
	if !errors.Is(b.Commit(1, "x"), ErrClosed) || !errors.Is(b.Cancel(1), ErrClosed) {
		t.Fatal("Commit/Cancel after Close must return ErrClosed")
	}

	// Pending tickets 1 and 3 were atomically cancelled; 2 and 4 drain in order.
	for _, want := range []struct {
		ticket uint64
		value  string
	}{{2, "b"}, {4, "d"}} {
		ticket, value, err := b.Next(context.Background())
		if err != nil || ticket != want.ticket || value != want.value {
			t.Fatalf("Next = (%d, %v, %v), want (%d, %s, nil)", ticket, value, err, want.ticket, want.value)
		}
	}
	if _, _, err := b.Next(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Next after drain = %v, want ErrClosed", err)
	}
	if got := b.Len(); got != 0 {
		t.Fatalf("Len = %d, want 0", got)
	}
	// Close is idempotent.
	b.Close()
}

func TestSnapshotAndLen(t *testing.T) {
	b := New()
	defer b.Close()
	for i := 1; i <= 4; i++ {
		mustReserve(t, b)
	}
	if err := b.Commit(1, "a"); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := b.Commit(3, "c"); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := b.Cancel(4); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	snap := b.Snapshot()
	want := Snapshot{NextReserve: 5, NextConsume: 1, Pending: 1, Ready: 2, Closed: false}
	if snap != want {
		t.Fatalf("Snapshot = %+v, want %+v", snap, want)
	}
	if got := b.Len(); got != 2 {
		t.Fatalf("Len = %d, want 2", got)
	}

	ticket, _, err := b.Next(context.Background())
	if err != nil || ticket != 1 {
		t.Fatalf("Next = (%d, %v), want (1, nil)", ticket, err)
	}
	snap = b.Snapshot()
	if snap.NextConsume != 2 || snap.Ready != 1 || b.Len() != 1 {
		t.Fatalf("after consume: Snapshot = %+v Len = %d, want NextConsume=2 Ready=1 Len=1", snap, b.Len())
	}

	b.Close()
	if snap := b.Snapshot(); !snap.Closed || snap.Pending != 0 {
		t.Fatalf("Snapshot after Close = %+v, want Closed=true Pending=0", snap)
	}
}
