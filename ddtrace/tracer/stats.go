// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

//go:generate msgp -unexported -marshal=false -o=stats_msgp.go -tests=false

package tracer

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"
	"gopkg.in/DataDog/dd-trace-go.v1/internal/log"

	"github.com/DataDog/sketches-go/ddsketch"
)

// aggregableSpan holds necessary information about a span that can be used to
// aggregate statistics in a bucket.
type aggregableSpan struct {
	// key specifies the aggregation key under which this span can be placed into
	// grouped inside a bucket.
	key aggregation

	Start, Duration int64
	Error           int32
	TopLevel        bool
}

// bucketSize specifies the size of a stats bucket.
var bucketSize = (10 * time.Second).Nanoseconds()

// concentrator aggregates and stores statistics on incoming spans in time buckets,
// flushing them occasionally to the underlying transport located in the given
// tracer config.
type concentrator struct {
	// In specifies the channel to be used for feeding data to the concentrator.
	// In order for In to have a consumer, the concentrator must be started using
	// a call to Start.
	In chan *aggregableSpan

	// mu guards below fields
	mu sync.Mutex

	// buckets maintains a set of buckets, where the map key represents
	// the starting point in time of that bucket, in nanoseconds.
	buckets map[int64]*rawBucket

	// stopped reports whether the concentrator is stopped (when non-zero)
	stopped uint64

	wg        sync.WaitGroup // waits for any active goroutines
	bufferLen int            // maximum number of buckets buffered
	oldestTs  int64          // any entry older than this will go into this bucket
	stop      chan struct{}  // closing this channel triggers shutdown
	cfg       *config        // tracer startup configuration
}

// defaultBufferLen represents the default buffer length; the number of bucket size
// units used by the concentrator.
const defaultBufferLen = 2

// newConcentrator creates a new concentrator using the given tracer configuration c.
func newConcentrator(c *config) *concentrator {
	return &concentrator{
		In:        make(chan *aggregableSpan, 10000),
		bufferLen: defaultBufferLen,
		stopped:   1,
		oldestTs:  alignTs(time.Now().UnixNano()),
		buckets:   make(map[int64]*rawBucket),
		cfg:       c,
	}
}

// alignTs returns the provided timestamp truncated to the bucket size.
// It gives us the start time of the time bucket in which such timestamp falls.
func alignTs(ts int64) int64 { return ts - ts%bucketSize }

// Start starts the concentrator. A started concentrator needs to be stopped
// in order to gracefully shut down, using Stop.
func (c *concentrator) Start() {
	if atomic.SwapUint64(&c.stopped, 0) == 0 {
		// already running
		return
	}
	c.stop = make(chan struct{})
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		tick := time.NewTicker(time.Duration(bucketSize) * time.Nanosecond)
		defer tick.Stop()
		c.runFlusher(tick.C)
	}()
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.runIngester()
	}()
}

// runFlusher runs the flushing loop which sends stats to the underlying transport.
func (c *concentrator) runFlusher(tick <-chan time.Time) {
	for {
		select {
		case now := <-tick:
			p := c.flush(now)
			if len(p.Stats) == 0 {
				// nothing to flush
				continue
			}
			if err := c.cfg.transport.sendStats(&p); err != nil {
				log.Error("Error sending stats payload: %v", err)
			}
		case <-c.stop:
			return
		}
	}
}

// runIngester runs the loop which accepts incoming data on the concentrator's In
// channel.
func (c *concentrator) runIngester() {
	for {
		select {
		case ss := <-c.In:
			c.add(ss)
		case <-c.stop:
			return
		}
	}
}

// add adds ss into the concentrators internal stats buckets.
func (c *concentrator) add(ss *aggregableSpan) {
	c.mu.Lock()
	defer c.mu.Unlock()

	btime := alignTs(ss.Start + ss.Duration)
	if btime < c.oldestTs {
		btime = c.oldestTs
	}
	b, ok := c.buckets[btime]
	if !ok {
		b = newRawBucket(uint64(btime))
		c.buckets[btime] = b
	}
	b.handleSpan(ss)
}

// Stop stops the concentrator and blocks until the operation completes.
func (c *concentrator) Stop() {
	if atomic.SwapUint64(&c.stopped, 1) > 0 {
		return
	}
	close(c.stop)
	c.wg.Wait()
}

func (c *concentrator) flush(timenow time.Time) statsPayload {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := timenow.UnixNano()
	sp := statsPayload{
		Hostname: c.cfg.hostname,
		Env:      c.cfg.env,
		Version:  c.cfg.version,
		Stats:    make([]statsBucket, 0, len(c.buckets)),
	}
	for ts, srb := range c.buckets {
		if ts > now-int64(c.bufferLen)*bucketSize {
			// Always make sure that we keep the most recent bufferLen number of buckets.
			// This is a trade-off: we accept slightly late traces (clock skew and stuff)
			// but we delay flushing by at most bufferLen buckets.
			continue
		}
		log.Debug("Flushing bucket %d", ts)
		sp.Stats = append(sp.Stats, srb.Export())
		delete(c.buckets, ts)
	}
	// After flushing, update the oldest timestamp allowed to prevent having stats for
	// an already-flushed bucket.
	newOldestTs := alignTs(now) - int64(c.bufferLen-1)*bucketSize
	if newOldestTs > c.oldestTs {
		log.Debug("Update oldestTs to %d", newOldestTs)
		c.oldestTs = newOldestTs
	}
	return sp
}

// aggregation specifies a uniquely identifiable key under which a certain set
// of stats are grouped inside a bucket.
type aggregation struct {
	Name       string
	Env        string
	Type       string
	Resource   string
	Service    string
	Hostname   string
	StatusCode uint32
	Version    string
	Synthetics bool
}

type rawBucket struct {
	start, duration uint64
	data            map[aggregation]*rawGroupedStats
}

func newRawBucket(btime uint64) *rawBucket {
	return &rawBucket{
		start:    btime,
		duration: uint64(bucketSize),
		data:     make(map[aggregation]*rawGroupedStats),
	}
}

func (sb *rawBucket) handleSpan(ss *aggregableSpan) {
	gs, ok := sb.data[ss.key]
	if !ok {
		gs = newRawGroupedStats()
		sb.data[ss.key] = gs
	}
	if ss.TopLevel {
		gs.topLevelHits++
	}
	gs.hits++
	if ss.Error != 0 {
		gs.errors++
	}
	gs.duration += float64(ss.Duration)
	// alter resolution of duration distro
	trundur := nsTimestampToFloat(ss.Duration)
	if ss.Error != 0 {
		gs.errDistribution.Add(trundur)
	} else {
		gs.okDistribution.Add(trundur)
	}
}

// Export transforms a RawBucket into a statsBucket, typically used
// before communicating data to the API, as RawBucket is the internal
// type while statsBucket is the public, shared one.
func (sb *rawBucket) Export() statsBucket {
	csb := statsBucket{
		Start:    sb.start,
		Duration: sb.duration,
		Stats:    make([]groupedStats, len(sb.data)),
	}
	for k, v := range sb.data {
		b, err := v.export(k)
		if err != nil {
			log.Error("Could not export stats bucket: %v.", err)
			continue
		}
		csb.Stats = append(csb.Stats, b)
	}
	return csb
}

type rawGroupedStats struct {
	// using float64 here to avoid the accumulation of rounding issues.
	hits            float64
	topLevelHits    float64
	errors          float64
	duration        float64
	okDistribution  *ddsketch.DDSketch
	errDistribution *ddsketch.DDSketch
}

func newRawGroupedStats() *rawGroupedStats {
	const (
		// relativeAccuracy is the value accuracy we have on the percentiles. For example, we can
		// say that p99 is 100ms +- 1ms
		relativeAccuracy = 0.01
		// maxNumBins is the maximum number of bins of the ddSketch we use to store percentiles.
		// It can affect relative accuracy, but in practice, 2048 bins is enough to have 1% relative accuracy from
		// 80 micro second to 1 year: http://www.vldb.org/pvldb/vol12/p2195-masson.pdf
		maxNumBins = 2048
	)
	okSketch, err := ddsketch.LogCollapsingLowestDenseDDSketch(relativeAccuracy, maxNumBins)
	if err != nil {
		log.Error("Error when creating ddsketch: %v", err)
	}
	errSketch, err := ddsketch.LogCollapsingLowestDenseDDSketch(relativeAccuracy, maxNumBins)
	if err != nil {
		log.Error("Error when creating ddsketch: %v", err)
	}
	return &rawGroupedStats{
		okDistribution:  okSketch,
		errDistribution: errSketch,
	}
}

func (s *rawGroupedStats) export(k aggregation) (groupedStats, error) {
	msg := s.okDistribution.ToProto()
	okSummary, err := proto.Marshal(msg)
	if err != nil {
		return groupedStats{}, err
	}
	msg = s.errDistribution.ToProto()
	errSummary, err := proto.Marshal(msg)
	if err != nil {
		return groupedStats{}, err
	}
	// round a float to an int, uniformly choosing
	// between the lower and upper approximations.
	round := func(f float64) uint64 {
		i := uint64(f)
		if rand.Float64() < f-float64(i) {
			i++
		}
		return i
	}
	return groupedStats{
		Service:        k.Service,
		Name:           k.Name,
		Resource:       k.Resource,
		HTTPStatusCode: k.StatusCode,
		Type:           k.Type,
		Hits:           round(s.hits),
		Errors:         round(s.errors),
		Duration:       round(s.duration),
		TopLevelHits:   round(s.topLevelHits),
		OkSummary:      okSummary,
		ErrorSummary:   errSummary,
		Synthetics:     k.Synthetics,
	}, nil
}

// nsTimestampToFloat converts a nanosec timestamp into a float nanosecond timestamp truncated to a fixed precision
func nsTimestampToFloat(ns int64) float64 {
	// 10 bits precision (any value will be +/- 1/1024)
	const roundMask int64 = 1 << 10
	var shift uint
	for ns > roundMask {
		ns = ns >> 1
		shift++
	}
	return float64(ns << shift)
}
