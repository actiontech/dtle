package gou

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestThrottleer(t *testing.T) {
	th := NewThrottler(10, 10*time.Second)
	for i := 0; i < 10; i++ {
		thb, tc := th.Throttle()
		assert.True(t, thb == false, "Should not throttle %v", i)
		assert.True(t, tc < 10, "Throttle count should remain below 10 %v", tc)
		time.Sleep(time.Millisecond * 10)
	}

	throttled := 0
	th = NewThrottler(10, 1*time.Second)
	// We are going to loop 20 times, first 10 should make it, next 10 throttled
	for i := 0; i < 20; i++ {
		LogThrottleKey(WARN, 10, "throttle", "hello %v", i)
		thb, tc := th.Throttle()

		if thb {
			throttled += 1
			assert.True(t, int(tc) == i-9, "Throttle count should rise %v, i: %d", tc, i)
		}
	}
	assert.True(t, throttled == 10, "Should throttle 10 of 20 requests: %v", throttled)

	// Now sleep for 1 second so that we should
	// no longer be throttled
	time.Sleep(time.Second * 2)
	thb, _ := th.Throttle()
	assert.True(t, thb == false, "We should not have been throttled")
}

func TestThrottler2(t *testing.T) {

	th := NewThrottler(10, 1*time.Second)

	tkey := "throttle2"
	throttleMu.Lock()
	logThrottles[tkey] = th
	throttleMu.Unlock()

	th, ok := logThrottles[tkey]
	if !ok {
		t.Errorf("Throttle key %s not created!", tkey)
	}

	// We are going to loop 20 times, first 10 should make it, next 10 throttled
	for i := 0; i < 20; i++ {

		LogThrottleKey(WARN, 10, tkey, "hello %v", i)

	}

	throttleMu.Lock()
	th = logThrottles[tkey]
	tcount := th.ThrottleCount()
	assert.True(t, tcount == 10, "Should throttle 10 of 20 requests: %v", tcount)
	throttleMu.Unlock()

	// Now sleep for 1 second so that we should
	// no longer be throttled
	time.Sleep(time.Second * 1)
	LogThrottleKey(WARN, 10, tkey, "hello again %v", 20)

}
