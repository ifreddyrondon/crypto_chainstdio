package workerpool

import (
	"sync"
)

type (
	// QueueNode is an element of a linked list.
	QueueNode struct {
		next, prev *QueueNode
		// Val stored with this element.
		Val func()
	}

	// Queue represents a FIFO queue implementation using linked list.
	// The zero value for List is an empty list ready to use.
	Queue struct {
		lock sync.Mutex
		head *QueueNode
		tail *QueueNode

		size uint64
	}
)

// NewQueue returns an initialized Queue.
func NewQueue() *Queue {
	node := &QueueNode{
		next: nil,
		prev: nil,
		Val:  nil,
	}
	return &Queue{
		head: node,
		tail: node,
		size: 0,
	}
}

// Push enqueues an element. It will wait until is unlocked
func (q *Queue) Push(val func()) {
	q.lock.Lock()
	defer q.lock.Unlock()
	node := &QueueNode{Val: val, next: q.head}
	if q.size == 0 {
		q.tail = node
	} else {
		q.head.prev = node
	}
	q.head = node
	q.size++
}

// Pop dequeues an element. Returns nil if queue is empty.
// It will wait until is unlocked.
func (q *Queue) Pop() func() {
	q.lock.Lock()
	defer q.lock.Unlock()
	if q.size == 0 {
		return nil
	}
	result := q.tail
	q.tail = q.tail.prev
	// remove reference from prev tail
	if q.tail != nil {
		q.tail.next = nil
	}
	q.size--
	// remove references when empty queue
	if q.size == 0 {
		q.head = nil
		q.tail = nil
	}
	return result.Val
}

// Len returns the number of elements currently stored in the queue. If q is
// nil, q.Len() is zero.
func (q *Queue) Len() uint64 {
	return q.size
}
