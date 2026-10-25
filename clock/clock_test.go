package clock

import (
	"testing"
	"time"
)

func TestRealClock(t *testing.T) {
	c := NewReal()
	before := time.Now()
	if c.Now().Before(before) {
		t.Fatal("Now went backwards")
	}
	fired := make(chan struct{})
	c.AfterFunc(time.Millisecond, func() { close(fired) })
	select {
	case <-fired:
	case <-time.After(2 * time.Second):
		t.Fatal("timer did not fire")
	}
	tm := c.AfterFunc(time.Hour, func() { t.Error("stopped timer fired") })
	if !tm.Stop() {
		t.Fatal("Stop on a pending timer returned false")
	}
}
