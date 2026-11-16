package stream

import (
	"sync"
	"testing"
)

// A publisher sending while a DWP client disconnects must not panic with
// "send on closed channel". Run with -race.
func TestSubscriber_SendRacingCloseDoesNotPanic(t *testing.T) {
	for range 200 {
		s := NewSubscriber("sub", 64, 1<<20)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for range 50 {
					s.send(&Event{})
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			s.Close()
		}()
		close(start)
		wg.Wait()
		if s.send(&Event{}) {
			t.Fatal("send after Close delivered an event")
		}
	}
}
