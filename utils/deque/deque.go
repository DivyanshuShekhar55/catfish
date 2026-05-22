package deque

import "errors"

var errDequeEmpty = errors.New("catfish/deque: cannot pop from empty queue")
var errDequeFull = errors.New("catfish/deque: deque full, rejecting item")

// i guess a few 100 conns per app is usually setup in a connection pooler
// so a 1/3-rd of the baseline is okay in the queue
// anyway it's configurable by the yaml settings
const (
	CAPACITY = 32
)

// Deque is a double-ended queue, allowing push and pop to both the front and back of the queue.
// Pushes and pops are amortized O(1). The zero-value is ready to use. Deque should not be copied
// after first use.
// Instead of physically shifting items around when you add/remove from the front
// the front and back pointers just move around the circle
// This makes push/pop at both ends super cheap — no copying needed.

type Deque[T any] struct {
	a     []T // the actual storage
	front int // index of the first item
	back  int // index of the last item (-1 means empty)
	gen   int // version number — bumped on every change
}

func New[T any](capacity int) *Deque[T] {
	if capacity <= 0 {
		panic("catfish/deque : capacity must be greater than 0")
	}
	return &Deque[T]{
		a:    make([]T, capacity),
		back: -1,
	}
}

// Len returns the number of items in the deque.
func (d *Deque[T]) Len() int {
	if d.a == nil || d.back == -1 {
		return 0
	}

	if d.front <= d.back {
		return d.back - d.front + 1
	}
	return len(d.a) - d.front + d.back + 1
}

// an important edge case to consider : 
// go allows users to initialise structs as var s Deque[int]
// since it doesn't call the New() it inits a nil slice
// this can cause panic when checking for d.a[any_index] or when pushing or popping items to the slice
// so we apply a d.a == nil check in all the functionalities 

func (d *Deque[T]) IsFull() bool {
	if d.a == nil {
		return false
	}

	// d.Len() calculates length based on front and back pointer positions
	// wheeras len(d.a) calculates the actual len of the underlying slice
	if d.Len() == len(d.a) {
		return true
	}
	return false
}

func (d *Deque[T]) PushFront(x T) error {
	// Lazy allocate memory if struct was initialized as a zero-value
	if d.a == nil {
		d.a = make([]T, CAPACITY)
	}

	if d.IsFull() {
		return errDequeFull
	}
	// move front pointer one behind
	d.front = positiveMod(d.front-1, len(d.a))
	d.a[d.front] = x

	// if deque was earlier empty, update the back pointer
	if d.back == -1 {
		d.back = d.front
	}
	d.gen++
	return nil

}

func (d *Deque[T]) PushBack(x T) error {
	// Lazy allocate memory if struct was initialized as a zero-value
	if d.a == nil {
		d.a = make([]T, CAPACITY)
	}

	if d.IsFull() {
		return errDequeFull
	}
	if d.back == -1 {
		d.back = d.front
	} else {
		d.back = (d.back + 1) % len(d.a)
	}
	d.a[d.back] = x
	d.gen++
	return nil
}

// PopFront removes and returns the item at the front of the deque. It panics if the deque is empty.
func (d *Deque[T]) PopFront() T {
	l := d.Len()
	if l == 0 {
		panic(errDequeEmpty)
	}
	item := d.a[d.front]
	var zero T
	if l == 1 {
		d.a[d.front] = zero
		d.front = 0
		d.back = -1
		return item
	}
	d.a[d.front] = zero
	d.front = (d.front + 1) % len(d.a)
	d.gen++
	return item
}

// PopBack removes and returns the item at the back of the deque. It panics if the deque is empty.
func (d *Deque[T]) PopBack() T {
	l := d.Len()
	if l == 0 {
		panic(errDequeEmpty)
	}
	item := d.a[d.back]
	var zero T
	if l == 1 {
		d.a[d.back] = zero
		d.front = 0
		d.back = -1
		return item
	}
	d.a[d.back] = zero
	d.back = positiveMod(d.back-1, len(d.a))
	d.gen++
	return item
}

// Front returns the item at the front of the deque. It panics if the deque is empty.
func (d *Deque[T]) Front() T {
	if d.back == -1 {
		panic("catfish/deque: index out of range")
	}
	return d.a[d.front]
}

// Back returns the item at the back of the deque. It panics if the deque is empty.
func (d *Deque[T]) Back() T {
	// Added empty check protection to prevent runtime d.a[-1] crash
	if d.back == -1 {
		panic("catfish/deque: index out of range")
	}

	return d.a[d.back]
}

// normal mod fails when we have negative numbers
// assume len = 9, amd front = 0, back = 2, now to pushFront(), front moves to 8 (one behind)
// but normal mod would calculate it as d.front = (0-1) % 9 = -1, which is wrong
// so for this edge case we do : -1 + 9 = 8
func positiveMod(l, d int) int {
	x := l % d
	if x < 0 {
		return x + d
	}
	return x
}
