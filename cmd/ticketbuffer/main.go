// Command ticketbuffer demonstrates the ticketbuffer library: out-of-order
// commits, a cancel, in-order consumption, and draining after Close.
package main

import (
	"context"
	"fmt"
	"log"

	"ticketbuffer"
)

func main() {
	buf := ticketbuffer.New()
	ctx := context.Background()

	t1, err := buf.Reserve()
	check(err)
	t2, err := buf.Reserve()
	check(err)
	t3, err := buf.Reserve()
	check(err)
	t4, err := buf.Reserve()
	check(err)
	fmt.Printf("reserved: %d %d %d %d\n", t1, t2, t3, t4)

	check(buf.Commit(t3, "third"))
	check(buf.Commit(t1, "first"))
	check(buf.Commit(t4, "fourth"))
	check(buf.Cancel(t2))
	fmt.Println("committed 3,1,4 out of order; cancelled 2")

	next(ctx, buf)
	next(ctx, buf)

	check(buf.Close())
	fmt.Println("closed; draining")

	next(ctx, buf)

	_, _, err = buf.Next(ctx)
	fmt.Printf("next after drain: %v\n", err)
	_, err = buf.Reserve()
	fmt.Printf("reserve after close: %v\n", err)
}

func next(ctx context.Context, buf *ticketbuffer.Buffer) {
	ticket, value, err := buf.Next(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("next: ticket=%d value=%v\n", ticket, value)
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
