package tracer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// withBucketSize sets bs as the new bucket size and returns a function to restore
// to the original value.
func withBucketSize(bs int64) func() {
	old := bucketSize
	bucketSize = bs
	return func() { bucketSize = old }
}

// waitForBuckets reports whether concentrator c contains n buckets within a 5ms
// period.
func waitForBuckets(c *concentrator, n int) bool {
	for i := 0; i < 5; i++ {
		time.Sleep(time.Millisecond)
		c.mu.Lock()
		if len(c.buckets) == n {
			return true
		}
		c.mu.Unlock()
	}
	return false
}

func TestAlignTs(t *testing.T) {
	now := time.Now().UnixNano()
	got := alignTs(now)
	want := now - now%((10 * time.Second).Nanoseconds())
	assert.Equal(t, got, want)
}

func TestConcentrator(t *testing.T) {
	key1 := aggregation{
		Name: "http.request",
		Env:  "prod",
	}
	ss1 := &aggregableSpan{
		key:      key1,
		Start:    time.Now().UnixNano() + 2*bucketSize,
		Duration: (2 * time.Second).Nanoseconds(),
	}
	key2 := aggregation{
		Name: "sql.query",
		Env:  "staging",
	}
	ss2 := &aggregableSpan{
		key:      key2,
		Start:    time.Now().UnixNano() + 3*bucketSize,
		Duration: (3 * time.Second).Nanoseconds(),
	}

	t.Run("new", func(t *testing.T) {
		assert := assert.New(t)
		cfg := &config{version: "1.2.3"}
		c := newConcentrator(cfg)
		assert.Equal(cap(c.In), 10000)
		assert.Equal(c.bufferLen, 2)
		assert.Nil(c.stop)
		assert.NotNil(c.buckets)
		assert.NotZero(c.oldestTs)
		assert.Equal(c.cfg, cfg)
		assert.EqualValues(c.stopped, 1)
	})

	t.Run("start-stop", func(t *testing.T) {
		assert := assert.New(t)
		c := newConcentrator(&config{})
		assert.EqualValues(c.stopped, 1)
		c.Start()
		assert.EqualValues(c.stopped, 0)
		c.Stop()
		c.Stop()
		assert.EqualValues(c.stopped, 1)
		c.Start()
		assert.EqualValues(c.stopped, 0)
		c.Start()
		c.Start()
		assert.EqualValues(c.stopped, 0)
		c.Stop()
		c.Stop()
		assert.EqualValues(c.stopped, 1)
	})

	t.Run("oldest", func(t *testing.T) {
		c := newConcentrator(&config{})
		c.add(&aggregableSpan{
			key:      key1,
			Start:    1,
			Duration: 2,
		})
		_, ok := c.buckets[c.oldestTs]
		assert.True(t, ok)
	})

	t.Run("valid", func(t *testing.T) {
		c := newConcentrator(&config{})
		btime := alignTs(ss1.Start + ss1.Duration)
		c.add(ss1)
		assert.Len(t, c.buckets, 1)
		b, ok := c.buckets[btime]
		assert.True(t, ok)
		assert.Equal(t, b.start, uint64(btime))
		assert.Equal(t, b.duration, uint64(bucketSize))
	})

	t.Run("grouping", func(t *testing.T) {
		c := newConcentrator(&config{})
		c.add(ss1)
		c.add(ss1)
		assert.Len(t, c.buckets, 1)
		_, ok := c.buckets[alignTs(ss1.Start+ss1.Duration)]
		assert.True(t, ok)
		c.add(ss2)
		assert.Len(t, c.buckets, 2)
		_, ok = c.buckets[alignTs(ss2.Start+ss2.Duration)]
		assert.True(t, ok)
	})

	t.Run("ingester", func(t *testing.T) {
		c := newConcentrator(&config{})
		c.Start()
		assert.Len(t, c.buckets, 0)
		c.In <- ss1
		if !waitForBuckets(c, 1) {
			t.Fatal("sending to channel did not work")
		}
		c.Stop()
	})

	t.Run("flusher", func(t *testing.T) {
		t.Run("old", func(t *testing.T) {
			transport := newDummyTransport()
			c := newConcentrator(&config{transport: transport})
			assert.Len(t, transport.Stats(), 0)
			defer withBucketSize(500000)()
			c.Start()
			c.In <- &aggregableSpan{
				key: key2,
				// Start must be older than latest 2 buckets to get flushed
				Start:    time.Now().UnixNano() - 3*bucketSize,
				Duration: 1,
			}
			c.In <- &aggregableSpan{
				key: key2,
				// Start must be older than latest 2 buckets to get flushed
				Start:    time.Now().UnixNano() - 4*bucketSize,
				Duration: 1,
			}
			time.Sleep(2 * time.Millisecond)
			c.Stop()
			assert.NotZero(t, transport.Stats())
		})

		t.Run("recent", func(t *testing.T) {
			transport := newDummyTransport()
			c := newConcentrator(&config{transport: transport})
			assert.Len(t, transport.Stats(), 0)
			defer withBucketSize(500000)()
			c.Start()
			c.In <- &aggregableSpan{
				key:      key2,
				Start:    time.Now().UnixNano() + 3*bucketSize,
				Duration: 1,
			}
			c.In <- &aggregableSpan{
				key:      key1,
				Start:    time.Now().UnixNano() + 4*bucketSize,
				Duration: 1,
			}
			time.Sleep(2 * time.Millisecond)
			c.Stop()
			assert.Zero(t, transport.Stats())
		})
	})
}
