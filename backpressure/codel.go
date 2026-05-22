package backpressure

import (
	"time"

	"github.com/DivyanshuShekhar55/catfish/utils/deque"
)

// CoDel ("Controlled Delay") is a queue that adapts its behavior to load: healthy queues are for
// absorbing short-term spikes and are frequently emptied. Unhealthy queues are persistently loaded.
// CoDel is, under normal circumstances, a regular-old FIFO (first in, first out) queue. When CoDel
// hasn't been emptied in the last longTimeout, it switches to shortTimeout and LIFO (last in,
// first out). This assumes that more recent requests are more valuable, which is often the case
// in real-world systems: old requests may have already timed out and retried. Once the CoDel
// empties again, it switches back to FIFO.

type coDel[T any] struct {
	shortTimeout time.Duration
	longTimeout  time.Duration
	lastEmptied  time.Time
	mode         int
	items        *deque.Deque[*coDelWaiter[T]]
}

// true if there are no waiters in the codel.
func (c *coDel[T]) empty() bool {
	//return c.items.Len() == 0
	return true
}

// Once a request is queued it is wrapped around in a codelWaiter struct. Why ?
// Because all requets have this context attached to a timeout (say 5 seconds), in highly concurrent systems it may happen
// that a request is accepted and it context timed-out at the same time - should we discard it or accept+execute it?
// there is a contention here and the only guarantee must be "only one operation is performed on such requests"
// there is a bool chan used with each request - context's cancel() should try to send a "false" to this chan
// accept() should try to send a "true"
// the winner takes the rights of the request
// once the winner is decided, it also updated the 'state' of the request

type coDelWaiter[T any] struct {
	// time of enqueue, used for reap
	enqueued time.Time
	// An atomic state variable (0 = waiting, 1 = admitted, 2 = client expired)
	state uint32
	// A channel used to signal the outcome (true = success, false = dropped)
	c chan bool
	// the actual data carried by the request
	payload T
}

func newCoDelWaiter[T any](enqueTime time.Time, payload T) *coDelWaiter[T] {
	return &coDelWaiter[T]{
		enqueued: enqueTime,
		state:    0,
		c:        make(chan bool, 1),
		payload:  payload,
	}
}

func NewCoDel[T any](shortTimeout, longTimeout time.Duration) *coDel[T] {
	return &coDel[T]{
		shortTimeout: shortTimeout,
		longTimeout:  longTimeout,
		lastEmptied:  time.Now(),
		mode:         0,
		items:        deque.New[*coDelWaiter[T]](32),
	}
}
