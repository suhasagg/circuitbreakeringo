// This program demonstrates how to pause all running goroutines when an error
// occured in one of them. Consider this as a global circuit-breaker

package main

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"
)

func main() {
	numWorkers := 5
//Inter go routine coordination
	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var pause bool

	counter := 0
	start := time.Now()
	for i := range worker1(ctx, generator1(ctx, 100), numWorkers, cond, pause) {
		counter++
		log.Println(i)
	}

	log.Println(time.Since(start))
	log.Println("done", counter)
}

func generator1(ctx context.Context, limit int) <-chan interface{} {
	outStream := make(chan interface{})

	go func() {
		defer close(outStream)
		for i := 0; i < limit; i++ {
			select {
			// The sequence of context done, does it matters?
			case outStream <- i:
			case <-ctx.Done():
				return
			}
		}
	}()
	return outStream
}

func worker1(ctx context.Context, inStream <-chan interface{}, numWorkers int, cond *sync.Cond, pause bool) <-chan interface{} {

	outStream := make(chan interface{})

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	failIndex := rand.Intn(50)
	multiplex := func(index int, in <-chan interface{}) {
		log.Println("starting multiplex", index)
	    //Read data from generator which fans out multiple go routines which process value from the generator
		for i := range in {
			// Assuming each operation takes roughly 100 ms
			time.Sleep(time.Duration(rand.Intn(50)+50) * time.Millisecond)


			// Fake failures
			if i == failIndex {
				cond.L.Lock()
				// Set pause to true
				// Circuit breaker on state
				pause = i == failIndex
				log.Println("worker", index, "encountered error at", i)
				cond.L.Unlock()

				// Retry after 5 seconds
				go func() {
					log.Println("spending five seconds to recover")
					//Timer for circuit breaker ie 5 seconds in this case
					//System is choking so circuit breaker must be tripped 
					time.Sleep(5 * time.Second)
					//Half on state of circuit breaker is not illustrated to check service call resumption and then turning on the circuit breaker
					//Can be done using select case when service call is met continue further, case service call is not met sleep again to simulate on conidition
					//of circuit breaker
					//When circuit breaker is on no retries are occuring
					//When it is tripped off -> retry occurs in case of error gain after some time exponential backoff kicks in
					// Reset (do I need to lock here too?)
					pause = false
					// go routines which has failed signal a broadcast i.e it has recovered and rest of the go routines which are blocked on wait condition are freed
					cond.Broadcast()
				    //

					}()
			}

			//Need to get a lock on condition before calling wait on that condition
			cond.L.Lock()
			// While pause is true...
			// go routines start pausing one at a time
			// circuit breaker is tripped
			for pause {
				// This does not consume cpu cycle. It suspends the current goroutines.
				log.Println("worker", index, "is paused")
				cond.Wait()
			}
			cond.L.Unlock()

			select {
			//For clean exit, when main routine is terminating, ctx is cancelled, can use context with deadline also to set a deadline for all go routines to complete
			case <-ctx.Done():
				return
			case outStream <- i:
			}
		}
		wg.Done()
	}

    //5 workers will try to process generator stream	
	for i := 0; i < numWorkers; i++ {
		log.Println("executing workers", i)
		go multiplex(i, inStream)
	}

	go func() {
		wg.Wait()
		close(outStream)
	}()

	return outStream
}
