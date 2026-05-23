package backpressure

import (
	"sync"
	"time"
)

// Semaphore is a way to bound concurrency and similar to golang.org/x/sync/semaphore. Conceptually,
// it is a bucket of some number of tokens. Callers can take tokens out of this bucket using
// Acquire, do whatever operation needs concurrency bounding, and then return the tokens with
// Release. If the bucket does not have enough tokens in it to Acquire, it will block for some time
// in case another user of tokens Releases.
//
// It has two major differences from golang.org/x/sync/semaphore:
//
// 1. It is prioritized, preferring to accept higher priority requests first.
//
// 2. Each queue of waiters is a CoDel, which is not fair but can behave better in a real-time
// system under load.
//
// In order to minimize wait times for high-priority requests, it self balances using "debt." Debt
// is tracked per priority and is the number of tickets that must be left in the semaphore before a
// given request may be admitted.
//
// Debt is self-adjusting: whenever a high-priority `Acquire()` cannot immediately be accepted, the
// debt for all lower priorities is increased. Intuitively, this request would not have had to wait
// if this debt already existed, so the semaphore self-corrects by adding it. Whenever a
// high-priority `Acquire()` can be admitted without waiting, then any existing debt may not have
// been necessary and so some of it is forgiven. Additionally, debt decays over time, since anything
// the semaphore has learned about a load pattern may become out-of-date as load changes.
//
//
// MIND MAP (too many things gonna happen here)
/* Acquire()
  ├── can admit immediately? → take slot, forgive debt, return
  └── cannot admit → penalize lower priorities (add debt)
                   → join codel queue
                   → sleep in w.wait(ctx)

Release()
  └── free slot → call admit() → wake up next eligible waiter

admit()
  ├── reap stale waiters (drop them)
  └── for each priority (high→low):
        admit as many as capacity+debt allows
        stop if this priority still has waiters (don't serve lower)

background goroutine
  └── fires every longTimeout → calls admit() → reap catches stale waiters
*/

type Semaphore struct {
	m           sync.Mutex    // the single lock protecting everything
	capacity    int           // max concurrent operations
	outstanding int           // how many slots are currently taken
	queues      []coDel[int]  // one waiting queue per priority level
	debt        []linearDecay // one debt tracker per priority level
	hasWaiters  bool          // are any queues non-empty right now?
	reapTicker  *time.Ticker  // the stale waiter killer
	longTimeout time.Duration // passed down to reap()
	bgDone      chan struct{} // used to inform server/semaphore close
	isClosed    bool          // tells if semaphore has closed
}

// Additional options for the Semaphore type. These options do not frequently need to be tuned as
// the defaults work in a majority of cases, but they're included for completeness.
type SemaphoreOption struct{ f func(*semaphoreOptions) }

type semaphoreOptions struct {
	shortTimeout          time.Duration
	longTimeout           time.Duration
	debtDecayInterval     time.Duration
	debtForgivePerSuccess float64
}

// The short timeout for the internal CoDels. See the README for more on CoDel.
func SemaphoreShortTimeout(d time.Duration) SemaphoreOption {
	return SemaphoreOption{func(opts *semaphoreOptions) {
		opts.shortTimeout = d
	}}
}

// The long timeout for the internal CoDels. See the README for more on CoDel.
func SemaphoreLongTimeout(d time.Duration) SemaphoreOption {
	return SemaphoreOption{func(opts *semaphoreOptions) {
		opts.longTimeout = d
	}}
}

// The time it takes for 100% debt to be completely forgiven. Debt decays linearly over time since
// load patterns change and a previously learned debt amount may no longer be relevant.
func SemaphoreDebtDecayInterval(x time.Duration) SemaphoreOption {
	return SemaphoreOption{func(opts *semaphoreOptions) {
		opts.debtDecayInterval = x
	}}
}

// The proportion of debt that is forgiven for lower priorities whenever a higher-priority request
// succeeds, in [0, 1].
func SemaphoreDebtForgivePerSuccess(x float64) SemaphoreOption {
	return SemaphoreOption{func(opts *semaphoreOptions) {
		opts.debtForgivePerSuccess = x
	}}
}
