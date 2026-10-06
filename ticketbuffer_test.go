package ticketbuffer_test

import (
	"context"
	"errors"
	"runtime"
	"sort"
	"sync"
	"testing"

	"ticketbuffer"
)

func mustReserve(t *testing.T, b *ticketbuffer.Buffer) uint64 {
	t.Helper()
	ticket, err := b.Reserve()
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	return ticket
}

func mustNext(t *testing.T, b *ticketbuffer.Buffer) (uint64, any) {
	t.Helper()
	ticket, value, err := b.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	return ticket, value
}

func TestReserveSequential(t *testing.T) {
	b := ticketbuffer.New()
	defer b.Close()
	for want := uint64(1); want <= 3; want++ {
		if got := mustReserve(t, b); got != want {
			t.Fatalf("Reserve = %d, want %d", got, want)
		}
	}
	if snap := b.Snapshot(); snap.NextReserve != 4 {
		t.Fatalf("NextReserve = %d, want 4", snap.NextReserve)
	}
}

func TestNextWaitsForInOrderCommit(t *testing.T) {
	b := ticketbuffer.New()
	defer b.Close()
	t1 := mustReserve(t, b)
	t2 := mustReserve(t, b)
	if err := b.Commit(t2, "second"); err != nil {
		t.Fatalf("Commit t2: %v", err)
	}

	type result struct {
		ticket uint64
		value  any
		err    error
	}
	done := make(chan result, 1)
	go func() {
		ticket, value, err := b.Next(context.Background())
		done <- result{ticket, value, err}
	}()
	runtime.Gosched()
	select {
	case r := <-done:
		t.Fatalf("Next returned %+v while head ticket still pending", r)
	default:
	}

	if err := b.Commit(t1, "first"); err != nil {
		t.Fatalf("Commit t1: %v", err)
	}
	r := <-done
	if r.err != nil || r.ticket != t1 || r.value != "first" {
		t.Fatalf("Next = %+v, want ticket %d value first", r, t1)
	}
	if ticket, value := mustNext(t, b); ticket != t2 || value != "second" {
		t.Fatalf("Next = (%d, %v), want (%d, second)", ticket, value, t2)
	}
}

func TestCancelSkipsTicket(t *testing.T) {
	b := ticketbuffer.New()
	defer b.Close()
	t1 := mustReserve(t, b)
	t2 := mustReserve(t, b)
	t3 := mustReserve(t, b)
	if err := b.Cancel(t2); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if err := b.Commit(t1, "a"); err != nil {
		t.Fatalf("Commit t1: %v", err)
	}
	if err := b.Commit(t3, "c"); err != nil {
		t.Fatalf("Commit t3: %v", err)
	}
	if ticket, value := mustNext(t, b); ticket != t1 || value != "a" {
		t.Fatalf("Next = (%d, %v), want (%d, a)", ticket, value, t1)
	}
	if ticket, value := mustNext(t, b); ticket != t3 || value != "c" {
		t.Fatalf("Next = (%d, %v), want (%d, c); cancelled %d not skipped", ticket, value, t3, t2)
	}
}

func TestConcurrentNextDistinct(t *testing.T) {
	b := ticketbuffer.New()
	defer b.Close()
	const n = 4
	tickets := make([]uint64, 0, n)
	for i := 0; i < n; i++ {
		tickets = append(tickets, mustReserve(t, b))
	}
	for i := n - 1; i >= 0; i-- {
		if err := b.Commit(tickets[i], i); err != nil {
			t.Fatalf("Commit %d: %v", tickets[i], err)
		}
	}

	var wg sync.WaitGroup
	got := make(chan uint64, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticket, _, err := b.Next(context.Background())
			if err != nil {
				t.Errorf("Next: %v", err)
				return
			}
			got <- ticket
		}()
	}
	wg.Wait()
	close(got)

	seen := make([]uint64, 0, n)
	for ticket := range got {
		seen = append(seen, ticket)
	}
	sort.Slice(seen, func(i, j int) bool { return seen[i] < seen[j] })
	if len(seen) != n {
		t.Fatalf("consumed %v, want %d distinct tickets", seen, n)
	}
	for i, ticket := range seen {
		if ticket != uint64(i+1) {
			t.Fatalf("consumed %v, want each of 1..%d exactly once", seen, n)
		}
	}
	if b.Len() != 0 {
		t.Fatalf("Len = %d, want 0", b.Len())
	}
}

func TestInvalidTransitions(t *testing.T) {
	b := ticketbuffer.New()
	defer b.Close()

	for _, tc := range []struct {
		name string
		err  error
		want error
	}{
		{"commit zero", b.Commit(0, "x"), ticketbuffer.ErrInvalidTicket},
		{"cancel zero", b.Cancel(0), ticketbuffer.ErrInvalidTicket},
		{"commit unknown", b.Commit(42, "x"), ticketbuffer.ErrUnknownTicket},
		{"cancel unknown", b.Cancel(42), ticketbuffer.ErrUnknownTicket},
	} {
		if !errors.Is(tc.err, tc.want) {
			t.Errorf("%s: got %v, want %v", tc.name, tc.err, tc.want)
		}
	}

	t1 := mustReserve(t, b)
	if err := b.Commit(t1, "v"); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := b.Commit(t1, "v"); !errors.Is(err, ticketbuffer.ErrAlreadyDone) {
		t.Errorf("double Commit: got %v, want ErrAlreadyDone", err)
	}
	if err := b.Cancel(t1); !errors.Is(err, ticketbuffer.ErrAlreadyDone) {
		t.Errorf("Cancel after Commit: got %v, want ErrAlreadyDone", err)
	}

	t2 := mustReserve(t, b)
	if err := b.Cancel(t2); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if err := b.Cancel(t2); !errors.Is(err, ticketbuffer.ErrAlreadyDone) {
		t.Errorf("double Cancel: got %v, want ErrAlreadyDone", err)
	}
	if err := b.Commit(t2, "v"); !errors.Is(err, ticketbuffer.ErrAlreadyDone) {
		t.Errorf("Commit after Cancel: got %v, want ErrAlreadyDone", err)
	}

	if ticket, _ := mustNext(t, b); ticket != t1 {
		t.Fatalf("Next = %d, want %d", ticket, t1)
	}
	if err := b.Commit(t1, "v"); !errors.Is(err, ticketbuffer.ErrTicketConsumed) {
		t.Errorf("Commit consumed: got %v, want ErrTicketConsumed", err)
	}
	if err := b.Cancel(t1); !errors.Is(err, ticketbuffer.ErrTicketConsumed) {
		t.Errorf("Cancel consumed: got %v, want ErrTicketConsumed", err)
	}
}

func TestNextContextCancel(t *testing.T) {
	b := ticketbuffer.New()
	defer b.Close()
	t1 := mustReserve(t, b)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := b.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Next with cancelled ctx: got %v, want context.Canceled", err)
	}

	done := make(chan error, 1)
	ctx2, cancel2 := context.WithCancel(context.Background())
	go func() {
		_, _, err := b.Next(ctx2)
		done <- err
	}()
	runtime.Gosched()
	cancel2()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked Next after cancel: got %v, want context.Canceled", err)
	}

	if err := b.Commit(t1, "v"); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if ticket, value := mustNext(t, b); ticket != t1 || value != "v" {
		t.Fatalf("Next = (%d, %v), want (%d, v); cancelled wait consumed the ticket", ticket, value, t1)
	}
}

func TestCloseDrains(t *testing.T) {
	b := ticketbuffer.New()
	t1 := mustReserve(t, b)
	t2 := mustReserve(t, b)
	t3 := mustReserve(t, b)
	if err := b.Commit(t3, "c"); err != nil {
		t.Fatalf("Commit t3: %v", err)
	}
	if err := b.Commit(t1, "a"); err != nil {
		t.Fatalf("Commit t1: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	if _, err := b.Reserve(); !errors.Is(err, ticketbuffer.ErrClosed) {
		t.Errorf("Reserve after Close: got %v, want ErrClosed", err)
	}
	if err := b.Commit(t2, "b"); !errors.Is(err, ticketbuffer.ErrClosed) {
		t.Errorf("Commit after Close: got %v, want ErrClosed", err)
	}
	if err := b.Cancel(t2); !errors.Is(err, ticketbuffer.ErrClosed) {
		t.Errorf("Cancel after Close: got %v, want ErrClosed", err)
	}
	if n := b.Len(); n != 2 {
		t.Fatalf("Len after Close = %d, want 2", n)
	}

	if ticket, value := mustNext(t, b); ticket != t1 || value != "a" {
		t.Fatalf("Next = (%d, %v), want (%d, a)", ticket, value, t1)
	}
	if ticket, value := mustNext(t, b); ticket != t3 || value != "c" {
		t.Fatalf("Next = (%d, %v), want (%d, c); pending %d not skipped", ticket, value, t3, t2)
	}
	if _, _, err := b.Next(context.Background()); !errors.Is(err, ticketbuffer.ErrClosed) {
		t.Fatalf("Next after drain: got %v, want ErrClosed", err)
	}
}

func TestSnapshot(t *testing.T) {
	b := ticketbuffer.New()
	t1 := mustReserve(t, b)
	t2 := mustReserve(t, b)
	mustReserve(t, b)
	if err := b.Commit(t2, "b"); err != nil {
		t.Fatalf("Commit t2: %v", err)
	}

	snap := b.Snapshot()
	want := ticketbuffer.Snapshot{NextReserve: 4, NextConsume: 1, Pending: 2, Ready: 1}
	if snap != want {
		t.Fatalf("Snapshot = %+v, want %+v", snap, want)
	}
	if n := b.Len(); n != 1 {
		t.Fatalf("Len = %d, want 1", n)
	}

	if err := b.Cancel(t1); err != nil {
		t.Fatalf("Cancel t1: %v", err)
	}
	if snap := b.Snapshot(); snap.NextConsume != 2 || snap.Pending != 1 {
		t.Fatalf("after head Cancel: Snapshot = %+v, want NextConsume 2 Pending 1", snap)
	}

	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	snap = b.Snapshot()
	want = ticketbuffer.Snapshot{NextReserve: 4, NextConsume: 2, Pending: 0, Ready: 1, Closed: true}
	if snap != want {
		t.Fatalf("after Close: Snapshot = %+v, want %+v", snap, want)
	}

	if ticket, value := mustNext(t, b); ticket != t2 || value != "b" {
		t.Fatalf("Next = (%d, %v), want (%d, b)", ticket, value, t2)
	}
	if snap := b.Snapshot(); snap.NextConsume != 4 || snap.Ready != 0 {
		t.Fatalf("after drain: Snapshot = %+v, want NextConsume 4 Ready 0", snap)
	}
}
