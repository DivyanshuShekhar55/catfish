package backpressure

import "errors"

var (
	ErrRejected               error = errors.New("catfish/backpressure : request rejected, too many requests")
	ErrCapacityNil            error = errors.New("catfish/backpressure : semaphore has 0 capacity")
	ErrCapacityNegative       error = errors.New("catfish/backpressure : semaphore has negative capacity")
	ErrSemaphoreAlreadyCLosed error = errors.New("catfish/backpressure : semaphore has already closed")
	ErrSemaphoreUnbalance     error = errors.New("catfish/backpressure : unbalanced acquire and release in semaphores")
	ErrInvalidPriorityLevel   error = errors.New("catfish/backpressure : invalid priority level")
	ErrTokenDemandTooLarge    error = errors.New("catfish/backpressure : token demand exceeds semaphore capacity")
)
