package deque

import "errors"

var (
	errDequeEmpty           = errors.New("catfish/deque: cannot pop from empty queue")
	errDequeFull            = errors.New("catfish/deque: deque full, rejecting item")
	errDequeCapacityInvalid = errors.New("catfish/deque : capacity must be greater than 0")
)
