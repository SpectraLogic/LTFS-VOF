package utils

import (
	"log"
)

// simple resource manager so that not too many file hits are done at one time
type Resource struct {
	reserveChan chan chan int // callback channel will be payload
	releaseChan chan int      // signals to release
	signalChan  chan bool     // channel for pause, resume or kill
	inUse       []bool        // state of in used
	max         int           // maximum number of opens
}

func NewResource(concurrent int) *Resource {
	var resource Resource
	resource.max = concurrent
	for i := 0; i < concurrent; i++ {
		resource.inUse = append(resource.inUse, false)
	}
	resource.reserveChan = make(chan chan int)
	resource.releaseChan = make(chan int)
	resource.signalChan = make(chan bool)

	// start the manager for this instance
	go resource.manager()

	return &resource
}

// the manager gives us resources if available
func (r *Resource) manager() {
	for {
		for {
			// break to outer for loop if all in use
			// release events
			var allInUse bool = true
			for _, inuse := range r.inUse {
				if inuse == false {
					allInUse = false
					break
				}
			}
			if allInUse {
				break
			}
			// select the request or release channel
			// until full
			select {
			// release a resource
			case unit := <-r.releaseChan:
				// reduce the number of resources in use
				r.inUse[unit] = false

				// receive a reseve request
			case callback := <-r.reserveChan:
				var allInUse bool = true
				for i, inuse := range r.inUse {
					if inuse == false {
						allInUse = false
						r.inUse[i] = true
						callback <- i
						break
					}
				}
				if allInUse {
					log.Fatal("SNO: All resources in use")
				}

			// receive signal to exit maanger
			case <-r.signalChan:
				return
			}
		}
		// all used up, wait for a release
		unit := <-r.releaseChan
		r.inUse[unit] = false
	}
}

// request a resource
func (r *Resource) Reserve() int {
	// create a callback channel
	callback := make(chan int)

	// send request with callback as argument
	r.reserveChan <- callback

	// wait for callback
	return <-callback
}
func (r *Resource) Release(i int) {
	// send request with callback as argument
	r.releaseChan <- i
}

// release a resource
func (r *Resource) Stop() {
	// send request to releaseChan
	r.signalChan <- true
}
