package workerpool

import (
	"sync"
)

// WorkerPool is a collection of goroutines, where the number of concurrent
// goroutines processing requests does not exceed the specified maximum.
type WorkerPool struct {
	maxWorkers   int
	taskQueue    chan func()
	workerQueue  chan func()
	waitingQueue *Queue
}

// New creates and starts a pool of worker goroutines.
//
// The maxWorkers parameter specifies the maximum number of workers that can
// execute tasks concurrently. When there are no incoming tasks, workers are
// gradually stopped until there are no remaining workers.
func New(maxWorkers int) *WorkerPool {
	// There must be at least one worker.
	if maxWorkers < 1 {
		maxWorkers = 1
	}

	pool := &WorkerPool{
		maxWorkers:   maxWorkers,
		taskQueue:    make(chan func()),
		workerQueue:  make(chan func()),
		waitingQueue: NewQueue(),
	}

	// Start the task dispatcher.
	go pool.dispatch()

	return pool
}

// Submit enqueues a function for a worker to execute.
//
// Any external values needed by the task function must be captured in a
// closure. Any return values should be returned over a channel that is
// captured in the task function closure.
//
// Submit will not block regardless of the number of tasks submitted. Each task
// is immediately given to an available worker or to a newly started worker. If
// there are no available workers, and the maximum number of workers are
// already created, then the task is put onto a waiting queue.
//
// When there are tasks on the waiting queue, any additional new tasks are put
// on the waiting queue. Tasks are removed from the waiting queue as workers
// become available.
func (p *WorkerPool) Submit(task func()) {
	if task != nil {
		p.taskQueue <- task
	}
}

// dispatch sends the next queued task to an available worker.
func (p *WorkerPool) dispatch() {
	var workerCount int
	var idle bool
	var wg sync.WaitGroup

	for {
		// As long as tasks are in the waiting queue, incoming tasks are put
		// into the waiting queue and tasks to run are taken from the waiting
		// queue. Once the waiting queue is empty, then go back to submitting
		// incoming tasks directly to available workers.
		if p.waitingQueue.Len() > 0 {
			select {
			case task, ok := <-p.taskQueue:
				if !ok {
					break
				}
				p.waitingQueue.Push(task)
			default:
				if len(p.workerQueue) == cap(p.workerQueue) {
					// all workers are full capacity, wait to the next round
					continue
				}
				p.workerQueue <- p.waitingQueue.Pop()
			}
			continue
		}

		select {
		case task, ok := <-p.taskQueue:
			if !ok {
				break
			}
			if len(p.workerQueue) < cap(p.workerQueue) {
				// A worker is ready
				p.workerQueue <- p.waitingQueue.Pop()
				continue
			}
			// Create a new worker, if not at max.
			if workerCount < p.maxWorkers {
				wg.Add(1)
				go worker(task, p.workerQueue, &wg)
				workerCount++
				continue
			}
			// enqueue task to be executed by next available worker.
			p.waitingQueue.Push(task)
		}
	}
}

// worker executes tasks and stops when it receives a nil task.
func worker(task func(), workerQueue chan func(), wg *sync.WaitGroup) {
	for task != nil {
		task()
		task = <-workerQueue
	}
	wg.Done()
}
