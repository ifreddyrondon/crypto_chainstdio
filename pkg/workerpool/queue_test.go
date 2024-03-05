package workerpool

import (
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueue(t *testing.T) {
	t.Parallel()

	t.Run("get a first inserted value from the queue", func(t *testing.T) {
		actual := new(int)
		q := NewQueue()
		q.Push(func() {
			assert.Equal(t, 1, *actual)
		})
		// head should not have prev
		assert.Nil(t, q.head.prev)
		// first value should be 1
		*actual = 1
		f := q.Pop()
		f()
		// size should be 0 when empty queue
		assert.Equal(t, uint64(0), q.size)
		// there should not be references to head or tail when empty
		assert.Nil(t, q.head)
		assert.Nil(t, q.tail)
	})

	t.Run("get a first inserted value from the queue when there are more than one value", func(t *testing.T) {
		actual := new(int)
		f1 := func() { assert.Equal(t, 1, *actual) }
		f2 := func() { assert.Equal(t, 2, *actual) }
		q := NewQueue()
		q.Push(f1)
		q.Push(f2)
		// head should not have prev
		assert.Nil(t, q.head.prev)
		// first value should be 1
		*actual = 1
		f := q.Pop()
		f()
		assert.Equal(t, uint64(1), q.size)
		// head and tail should be the same when queue size is 1
		assert.Equal(t, q.tail, q.head)
		// tail should not have next
		assert.Nil(t, q.tail.next)
	})

	t.Run("it should be safe inserting after empty queue", func(t *testing.T) {
		f1 := func() {}
		f2 := func() {}
		q := NewQueue()
		q.Push(f1)
		q.Pop()
		assert.Equal(t, uint64(0), q.size)
		q.Push(f2)
		assert.Equal(t, uint64(1), q.size)
	})

	t.Run("it should be safe removing when empty queue", func(t *testing.T) {
		q := NewQueue()
		f := q.Pop()
		assert.Equal(t, uint64(0), q.size)
		assert.Nil(t, f)
	})

	t.Run("pop order should be FIFO", func(t *testing.T) {
		actual := new(int)
		q := NewQueue()
		// generate random input
		min := 10
		max := 1000
		top := rand.Intn(max-min) + min
		// insertion
		for i := 0; i < top; i++ {
			i := i
			q.Push(func() {
				assert.Equal(t, i, *actual)
			})
		}
		// pop
		for i := 0; i < top; i++ {
			f := q.Pop()
			*actual = i
			f()
		}
		assert.Equal(t, uint64(0), q.size)
	})
}

func TestQueueConcurrency(t *testing.T) {
	t.Parallel()
	t.Run("push should be deterministic", func(t *testing.T) {
		// generate random input
		min := 10
		max := 1000
		concurrency := rand.Intn(max-min) + min

		var wg sync.WaitGroup
		wg.Add(concurrency)
		q := NewQueue()
		for i := 0; i < concurrency; i++ {
			go func() {
				defer wg.Done()
				q.Push(func() {})
			}()
		}
		wg.Wait()
		assert.Equal(t, uint64(concurrency), q.size)
	})
	t.Run("pop should be deterministic", func(t *testing.T) {
		// generate random input
		min := 10
		max := 1000
		concurrency := rand.Intn(max-min) + min
		q := NewQueue()
		// push
		for i := 0; i < concurrency; i++ {
			q.Push(func() {})
		}
		// pop
		var wg sync.WaitGroup
		// keep one node
		wg.Add(concurrency - 1)
		for i := 1; i < concurrency; i++ {
			go func() {
				defer wg.Done()
				q.Pop()
			}()
		}
		wg.Wait()
		assert.Equal(t, uint64(1), q.size)
	})
}
