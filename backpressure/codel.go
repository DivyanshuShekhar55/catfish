package backpressure

import (
	"time"
)

// CoDel ("Controlled Delay") is a queue that adapts its behavior to load: healthy queues are for
// absorbing short-term spikes and are frequently emptied. Unhealthy queues are persistently loaded.
// CoDel is, under normal circumstances, a regular-old FIFO (first in, first out) queue. When CoDel
// hasn't been emptied in the last longTimeout, it switches to shortTimeout and LIFO (last in,
// first out). This assumes that more recent requests are more valuable, which is often the case
// in real-world systems: old requests may have already timed out and retried. Once the CoDel
// empties again, it switches back to FIFO.

type codel[T any] struct {
	shortTimeout time.Duration
	longTimeout  time.Duration
	lastEmptied  time.Time
	mode         int
	items        []int
}

// true if there are no waiters in the codel.
func (c *codel[T]) empty() bool {
	//return c.items.Len() == 0
	return true
}

