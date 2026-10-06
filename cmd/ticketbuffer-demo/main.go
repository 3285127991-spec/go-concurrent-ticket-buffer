// Command ticketbuffer-demo shows the ticketbuffer library in action:
// out-of-order commits, a cancellation, strictly ordered consumption and
// draining the buffer after Close.
package main

import (
	"context"
	"errors"
	"fmt"

	"ticketbuffer/ticketbuffer"
)

func main() {
	buf := ticketbuffer.New()

	var tickets [4]uint64
	for i := range tickets {
		ticket, err := buf.Reserve()
		if err != nil {
			panic(err)
		}
		tickets[i] = ticket
	}
	fmt.Println("reserved tickets:", tickets)

	// Commit three tickets out of order and cancel one.
	must(buf.Commit(tickets[2], "third"))
	must(buf.Commit(tickets[0], "first"))
	must(buf.Cancel(tickets[1]))
	must(buf.Commit(tickets[3], "fourth"))
	fmt.Println("committed 3, 1; cancelled 2; committed 4")

	// Next always consumes in ticket order, skipping the cancelled ticket.
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		ticket, value, err := buf.Next(ctx)
		must(err)
		fmt.Printf("next -> ticket=%d value=%s\n", ticket, value)
	}

	// Close cancels anything still pending; committed values drain first.
	buf.Close()
	if _, _, err := buf.Next(ctx); errors.Is(err, ticketbuffer.ErrClosed) {
		fmt.Println("buffer closed and drained")
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
