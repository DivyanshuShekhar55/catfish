package backpressure

import (
	"context"
	"sync/atomic"
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
	mode         coDelMode
	items        *deque.Deque[*coDelWaiter[T]]
}

type coDelMode int

const (
	coDelModeFIFO coDelMode = iota
	coDelModeLIFO
)

// true if there are no waiters in the codel.
func (c *coDel[T]) empty() bool {
	return c.items.Len() == 0
}

// Once a request is queued it is wrapped around in a codelWaiter struct. Why ?
// Because all requets have this context attached to a timeout (say 5 seconds), in highly concurrent systems it may happen
// that a request is accepted and it context timed-out (<-ctx.Done()) at the same time - should we discard it or accept+execute it?
// there is a contention here and the only guarantee must be "only one of these operations is performed on such requests"
// there is a atomic state used with each request - context's close should try to make it = 2
// accept() should try to make it = 1
// the winner of the atomic swap operation takes the rights of the request
// so if state=1 wins, it is accepted, else rejected.

// Note that there is no contention between context timeout and drop() because both reject the request, no harm if both run
// the drop function is called by the reap()er function
// the admit() and drop() put the reject or accept signal into the 'c' channel
// accept() sends a true while drop() sends a false to this boolean channel

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

func NewCoDel[T any](shortTimeout, longTimeout time.Duration, queueSize int) *coDel[T] {
	return &coDel[T]{
		shortTimeout: shortTimeout,
		longTimeout:  longTimeout,
		lastEmptied:  time.Now(),
		mode:         0,
		items:        deque.New[*coDelWaiter[T]](queueSize),
	}
}

// next we want to implement wait, admit and drop functions
// wait will keep the request waiting until context timeouts or admit or the reap "wakes" it up
// which means these action(s) try to take the rights of that request and change its 'state'
// the context is passed all the way from request to codel to here
func (cw *coDelWaiter[T]) wait(ctx context.Context) error {

	// the select is responsible for keeping the request blocked, smthing must happen (the cases) to wake it up
	select {
	// case 1: context timed out / cancelled
	case <-ctx.Done():
		// context cancel
		// try to update the state atomically
		// if state was = 0 (waiting), set to 2 (client expired) now
		didSwap := atomic.CompareAndSwapUint32(&cw.state, 0, 2)
		if !didSwap {
			// failed to swap, state value must already have been = 1
			// i.e., admitted => no error
			return nil
		}
		// ctx cancelled succesfully, return error = context error to let user know
		return ctx.Err()

	case v := <-cw.c:
		if v {
			// v is true, request accepted by codel
			return nil
		} else {
			// reap rejected the request, send error
			return ErrRejected
		}
	}

}

// admit will attempt to admit the request, returns true if admission successfull
func (wc *coDelWaiter[T]) admit() bool {
	isAdmitted := atomic.CompareAndSwapUint32(&wc.state, 0, 1)
	wc.c <- true
	return isAdmitted
}

// drop unsuccessfully-wakes the waiter, it will return ErrRejected from wait.
// we don't need any state swaps as there is no contention between drop with admit or with context timeout
// simply send a false
func (w *coDelWaiter[T]) drop() {
	w.c <- false
}

// Next try implementing push, pop and reap methods
func (c *coDel[T]) Push(wc *coDelWaiter[T], now time.Time) {
	// because we push, it might be a trigger to cause mode change
	c.setMode(now)
	c.items.PushBack(wc)
}

// setMode appropriately sets the codelMode. Called internally.
// called after pop, before doing a push and before and after both reap
func (c *coDel[T]) setMode(now time.Time) {
	if c.items.Len() == 0 {
		c.mode = coDelModeFIFO
		c.lastEmptied = now
	} else if now.Sub(c.lastEmptied) > c.longTimeout {
		c.mode = coDelModeLIFO
	}
}

// returns the next item that would be removed by pop, if there is one. calling reap, setMode, or
// push invalidates this value.
func (c *coDel[T]) peek() (T, bool) {
	if c.items.Len() == 0 {
		var zero T
		return zero, false
	}
	switch c.mode {
	case coDelModeFIFO:
		return c.items.Front().payload, true
	case coDelModeLIFO:
		return c.items.Back().payload, true
	default:
		panic("catfish/backpressure : unreachable")
	}
}

// returns the element popped, popped based on current mode
// then tries to admit the element
// it returns a bool representing success or failure and a value
// if failed to admit send a zero value (can NOT return nil as not supported for all types in Go) along with false
func (c *coDel[T]) pop(now time.Time) (T, bool) {
	var zero T
	if c.items.Len() == 0 {
		return zero, false
	}
	var wc *coDelWaiter[T]
	switch c.mode {
	case coDelModeFIFO:
		wc = c.items.PopFront()
	case coDelModeLIFO:
		wc = c.items.PopBack()
	default:
		panic("unreachable")
	}
	// its also a trigger to check if mode can change after pop
	c.setMode(now)

	// check if the popped item can be admiited
	ok := wc.admit()
	if !ok {
		// if not admitted, was rejected by reap
		return zero, false
	}
	// admitted
	return wc.payload, true
}

// reap removes and wakes-unsuccessfully waiters that have timed out based on the current codelMode.
// codel assumes this will be called at least once every longTimeout
// this assumption leads to change the timeout in the reap function itself
func (c *coDel[T]) reap(now time.Time) {
	c.setMode(now)
	timeout := c.longTimeout
	if c.mode == coDelModeLIFO {
		timeout = c.shortTimeout
	}
	for c.items.Len() > 0 && now.Sub(c.items.Front().enqueued) > timeout {
		item := c.items.PopFront()
		item.drop()
	}
	c.setMode(now)
}
