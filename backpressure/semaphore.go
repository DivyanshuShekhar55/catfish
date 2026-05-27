package backpressure

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/DivyanshuShekhar55/catfish/config"
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

var (
	ErrCapacityNil            error = errors.New("catfish/backpressure : semaphore has 0 capacity")
	ErrCapacityNegative       error = errors.New("catfish/backpressure : semaphore has negative capacity")
	ErrSemaphoreAlreadyCLosed error = errors.New("catfish/backpressure : semaphore has already closed")
	ErrSemaphoreUnbalance     error = errors.New("catfish/backpressure : unbalanced acquire and release in semaphores")

	infinity time.Duration = 365 * 24 * time.Hour
)

type Semaphore struct {
	m                     sync.Mutex    // the single lock protecting everything
	capacity              int           // max concurrent operations
	outstanding           int           // how many slots are currently taken
	queues                []*coDel[int] // one waiting queue per priority level
	debt                  []linearDecay // one debt tracker per priority level
	hasWaiters            bool          // are any queues non-empty right now?
	reapTicker            *time.Ticker  // the stale waiter killer
	bgDone                chan struct{} // used to inform server/semaphore close
	isClosed              bool          // tells if semaphore has closed
	longTimeout           time.Duration // passed down to reap(), how often to reap
	debtForgivePerSuccess float64       // debt forgiveness amount (explanation later), if it is 0.1 means forgive 10% of current debt
	debtDecayInterval     time.Duration // in how much time debt will completely decay/reset/forgiven/erase
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

// NewSemaphore returns a semaphore with the given number of priorities, and will allow at most
// capacity concurrency.
// The other options do not frequently need to be modified.
// capacity is the max available token
func NewSemaphore(tiers []config.Tier, capacity int, options ...SemaphoreOption) *Semaphore {
	if capacity < 0 {
		panic(ErrCapacityNegative)
	}
	opts := semaphoreOptions{
		shortTimeout:          5 * time.Millisecond,
		longTimeout:           100 * time.Millisecond,
		debtDecayInterval:     10 * time.Second,
		debtForgivePerSuccess: 0.1,
	}
	for _, option := range options {
		option.f(&opts)
	}

	now := time.Now()

	// from the config get num of queues and size per queue
	// pass the queueSizes[i] to make the new deque
	prioritiesCount := len(tiers)

	// we don't init a mutex as by default it gets a 0 value
	s := &Semaphore{
		capacity:              capacity,
		outstanding:           0,
		queues:                make([]*coDel[int], prioritiesCount),
		debt:                  make([]linearDecay, prioritiesCount),
		hasWaiters:            false,
		reapTicker:            time.NewTicker(infinity),
		bgDone:                make(chan struct{}),
		longTimeout:           opts.longTimeout,
		debtForgivePerSuccess: opts.debtForgivePerSuccess,
		debtDecayInterval:     opts.debtDecayInterval,
	}

	// start the background listener on a separate goroutine
	// in original implementation, this is done in the Acquire function

	// init the codels and debt struct values (currently all set as default zero values)
	for i, tier := range tiers {
		s.queues[i] = NewCoDel[int](opts.shortTimeout, opts.longTimeout, tier.QueueSize)
		s.debt[i] = linearDecay{
			last: now,
			max:  float64(capacity), // can't have more seats reserved than there is capacity
			// e.g., if capacity is 10 and we wish to decay debt in 10 seconds, then per second 1 unit debt should reduce
			decayPerSec: float64(capacity) / opts.debtDecayInterval.Seconds(),
		}
	}

	go s.background()
	return s
}

// Acquire attempts to acquire some number of tokens from the semaphore on behalf of the given
// priority. If Acquire succedes it returns nil, and the acquired tokens should be returned to the semaphore when the
// caller is finished with them by using Release. Acquire returns non-nil if the given context
// expires before the tokens can be acquired, or if the request is rejected for timing out with the
// semaphore's own timeout.
func (s *Semaphore) Acquire(ctx context.Context, tokenDemand int, targetPriority int) error {
	// take the lock, so multiple workers(tcp listeners) can't acquire at once
	s.m.Lock()

	if s.isClosed {
		s.m.Unlock()
		panic(ErrSemaphoreAlreadyCLosed)
	}

	if s.capacity == 0 {
		s.m.Unlock()
		panic(ErrCapacityNil)
	}

	// Guard clause against dirty input indices
	if targetPriority < 0 || targetPriority >= len(s.queues) {
		s.m.Unlock()
		return fmt.Errorf("invalid priority level %d", targetPriority)
	}

	if tokenDemand > s.capacity {
		s.m.Unlock()
		return fmt.Errorf(
			"tried to Acquire %d tokens, semaphore only has capacity for %d",
			tokenDemand,
			s.capacity,
		)
	}

	now := time.Now()

	// check if there is a higher priority request already available
	// e.g., if tokenDemand is 2 tokens from targetPriority = 3, then run a loop from priority levels - 0 till 3
	// if they are not empty, it means a higher priority is available
	isHigherPriorityPresent := false
	for i := 0; i <= targetPriority; i++ {
		if !s.queues[i].empty() {
			isHigherPriorityPresent = true
			break
		}
	}

	// if req can be admitted straight
	if !isHigherPriorityPresent && s.canAdmit(now, tokenDemand, targetPriority) {
		s.outstanding += tokenDemand

		// do a safety check before moving on
		if s.outstanding > s.capacity {
			s.m.Unlock()
			panic(ErrSemaphoreUnbalance)
		}

		// decrease the debt for lower priorities, be a little generous
		for i := targetPriority + 1; i < len(s.debt); i++ {
			s.debt[i].add(now, -(s.debtForgivePerSuccess * float64(tokenDemand)))
			// Careful : Don't be to generous to everyone
			// Make sure that we don't accidentally make lower debt for any lower priority. e.g. if
			// p=0 waits and increases debt for p=1 and p=2, then a p=1 succeeds, p=2 would end with
			// lower debt than p=1 which makes no sense.
			s.debt[i].floor(now, s.debt[i-1].get(now))
		}

		s.m.Unlock()
		return nil

	}

	// a request couldn't pass through
	// be a little stricter now
	for i := int(targetPriority) + 1; i < len(s.debt); i++ {
		s.debt[i].add(now, float64(tokenDemand))

		// safety check
		s.debt[i].setMax(now, float64(s.capacity))
	}

	// wrap req into a codelWaiter and put into queue
	cw := newCoDelWaiter(now, tokenDemand)
	s.queues[targetPriority].Push(cw, now)

	// check if reap timer is running
	if !s.hasWaiters {
		s.reapTicker.Reset(s.longTimeout)
		s.hasWaiters = true
	}

	// original implementation has a check for nil s.bgDone but we did that in New itself
	// THINK : can it casue troubles?
	s.admit(now)
	s.m.Unlock()

	// since req was not admitted make it wait
	return cw.wait(ctx)

}

func (s *Semaphore) background() {
	// keeps listening for events forever
	for {
		select {
		case <-s.bgDone:
			return
		case <-s.reapTicker.C:
			s.m.Lock()
			if s.reapTicker == nil {
				s.m.Unlock()
				return
			}
			now := time.Now()
			// admit calls the reaper, so just call admit
			s.admit(now)
			s.m.Unlock()

		}
	}
}

// This is called whenever something changes (a slot freed, a new waiter joined)
// It tries to push as many waiters through as possible
func (s *Semaphore) admit(now time.Time) {
	// drop stale ones
	for i := range s.queues {
		s.queues[i].reap(now)
	}
	for i := range s.queues {
		queue := s.queues[i]
		for {
			nextTokenDemand, ok := queue.peek()
			if !ok {
				// Queue is empty, move to next lowest priority
				break
			}

			// TODO
			// I SHOULD BE REPLACED BY PRIORITY DATA TYPE
			if !s.canAdmit(now, nextTokenDemand, i) {
				// Not enough tokens or blocked by priority debt! Stop evaluating this queue.
				break
			}

			// coDel.pop() -> returns the payload and a success flag bool
			// if success also sends a true to the coDelWaiter's channel
			// this removes the wait blockage
			_, ok = queue.pop(now)
			if ok {
				s.outstanding += nextTokenDemand
			}
		}

		if !queue.empty() {
			return
		}
	}
	// All queues are empty.
	if s.hasWaiters {
		s.reapTicker.Reset(infinity)
		s.hasWaiters = false
	}
}

func (s *Semaphore) canAdmit(now time.Time, tokenDemand int, targetPriority int) bool {
	available := float64(s.capacity) - float64(s.outstanding)
	debt := s.debt[targetPriority].get(now)
	return available >= debt+float64(tokenDemand)
}

// Release returns the given number of tokens to the semaphore. It should only be called if these
// tokens are known to be acquired from the semaphore with a corresponding Acquire.
func (s *Semaphore) Release(tokens int) {
	s.m.Lock()
	s.outstanding -= tokens
	if s.outstanding < 0 {
		s.m.Unlock()
		panic(ErrSemaphoreUnbalance)
	}
	s.admit(time.Now())
	s.m.Unlock()
}

// Close frees background resources used by the semaphore.
func (s *Semaphore) Close() {
	s.m.Lock()
	defer s.m.Unlock()
	if s.isClosed {
		return
	}
	s.isClosed = true
	s.reapTicker.Stop()
	if s.bgDone != nil {
		close(s.bgDone)
	}
}
