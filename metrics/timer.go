/*
Copyright 2009-2016 Weibo, Inc.

All files licensed under the Apache License, Version 2.0 (the "License");
you may not use these files except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package metrics

import (
	"sync"
	"time"

	"github.com/rcrowley/go-metrics"
)

type timer struct {
	mutex sync.Mutex
	sum   int64
	prev  int64
	count int64
	mean  float64
}

// GetOrRegisterTimer returns an existing Timer or constructs and registers a
// new StandardTimer.
func getOrRegisterTimer(name string, r metrics.Registry) metrics.Timer {
	if nil == r {
		r = metrics.DefaultRegistry
	}
	return r.GetOrRegister(name, newTimer).(metrics.Timer)
}

func newTimer() metrics.Timer {
	t := &timer{}

	arbiter.Lock()
	defer arbiter.Unlock()
	arbiter.meters = append(arbiter.meters, t)
	if !arbiter.started {
		arbiter.started = true
		go arbiter.tick()
	}
	return t
}

// Count returns the number of events recorded.
func (t *timer) Count() int64 {
	t.mutex.Lock()
	count := t.count
	t.mutex.Unlock()
	return count
}

// Max returns the maximum value in the sample.
func (t *timer) Max() int64 {
	// Not implement
	return 0
}

// Min returns the minimum value in the sample.
func (t *timer) Min() int64 {
	// Not implement
	return 0
}

// Mean returns the mean of the values in the sample.
func (t *timer) Mean() float64 {
	return t.RateMean()
}

// Percentile returns an arbitrary percentile of the values in the sample.
func (t *timer) Percentile(p float64) float64 {
	// Not implement
	return 0.0
}

// Percentiles returns a slice of arbitrary percentiles of the values in the
// sample.
func (t *timer) Percentiles(ps []float64) []float64 {
	// Not implement
	return nil
}

// Rate1 returns the one-minute moving average rate of events per second.
func (t *timer) Rate1() float64 {
	return t.RateMean()
}

// Rate5 returns the five-minute moving average rate of events per second.
func (t *timer) Rate5() float64 {
	return t.RateMean()
}

// Rate15 returns the fifteen-minute moving average rate of events per second.
func (t *timer) Rate15() float64 {
	return t.RateMean()
}

// RateMean returns the meter's mean rate of events per second.
func (t *timer) RateMean() float64 {
	t.mutex.Lock()
	mean := t.mean
	t.mutex.Unlock()
	return mean
}

// Snapshot returns a read-only copy of the timer.
func (t *timer) Snapshot() metrics.Timer {
	t.mutex.Lock()
	snap := &timerSnapshot{
		sum:   t.sum,
		count: t.count,
		mean:  t.mean,
	}
	t.mutex.Unlock()
	return snap
}

// StdDev returns the standard deviation of the values in the sample.
func (t *timer) StdDev() float64 {
	// Not implement
	return 0.0
}

// Sum returns the sum in the sample.
func (t *timer) Sum() int64 {
	t.mutex.Lock()
	sum := t.sum
	t.mutex.Unlock()
	return sum
}

// Record the duration of the execution of the given function.
// record time as Millisecond
func (t *timer) Time(f func()) {
	ts := time.Now()
	f()
	t.Update(time.Since(ts) / time.Millisecond)
}

// Record the duration of an event. Millisecond
func (t *timer) Update(d time.Duration) {
	t.mutex.Lock()
	t.sum += int64(d)
	t.count += 1
	t.mutex.Unlock()
}

// Record the duration of an event that started at a time and ends now.
func (t *timer) UpdateSince(ts time.Time) {
	t.Update(time.Since(ts) / time.Millisecond)
}

// Variance returns the variance of the values in the sample.
func (t *timer) Variance() float64 {
	// Not implement
	return 0.0
}

func (t *timer) tick() {
	t.mutex.Lock()
	if t.count != 0 {
		sum := t.sum
		t.mean = float64(sum-t.prev) / float64(t.count)
		t.prev = sum
		t.count = 0
	}
	t.mutex.Unlock()
}

// TimerSnapshot is a read-only copy of another Timer.
type timerSnapshot struct {
	sum   int64
	count int64
	mean  float64
}

// Count returns the number of events recorded at the time the snapshot was
// taken.
func (t *timerSnapshot) Count() int64 { return t.count }

// Max returns the maximum value at the time the snapshot was taken.
func (t *timerSnapshot) Max() int64 { return 0 }

// Mean returns the mean value at the time the snapshot was taken.
func (t *timerSnapshot) Mean() float64 { return t.mean }

// Min returns the minimum value at the time the snapshot was taken.
func (t *timerSnapshot) Min() int64 { return 0 }

// Percentile returns an arbitrary percentile of sampled values at the time the
// snapshot was taken.
func (t *timerSnapshot) Percentile(p float64) float64 {
	return 0.0
}

// Percentiles returns a slice of arbitrary percentiles of sampled values at
// the time the snapshot was taken.
func (t *timerSnapshot) Percentiles(ps []float64) []float64 {
	return nil
}

// Rate1 returns the one-minute moving average rate of events per second at the
// time the snapshot was taken.
func (t *timerSnapshot) Rate1() float64 { return t.mean }

// Rate5 returns the five-minute moving average rate of events per second at
// the time the snapshot was taken.
func (t *timerSnapshot) Rate5() float64 { return t.mean }

// Rate15 returns the fifteen-minute moving average rate of events per second
// at the time the snapshot was taken.
func (t *timerSnapshot) Rate15() float64 { return t.mean }

// RateMean returns the meter's mean rate of events per second at the time the
// snapshot was taken.
func (t *timerSnapshot) RateMean() float64 { return t.mean }

// Snapshot returns the snapshot.
func (t *timerSnapshot) Snapshot() metrics.Timer { return t }

// StdDev returns the standard deviation of the values at the time the snapshot
// was taken.
func (t *timerSnapshot) StdDev() float64 { return 0.0 }

// Sum returns the sum at the time the snapshot was taken.
func (t *timerSnapshot) Sum() int64 { return t.sum }

// Time panics.
func (*timerSnapshot) Time(func()) {
	panic("Time called on a TimerSnapshot")
}

// Update panics.
func (*timerSnapshot) Update(time.Duration) {
	panic("Update called on a TimerSnapshot")
}

// UpdateSince panics.
func (*timerSnapshot) UpdateSince(time.Time) {
	panic("UpdateSince called on a TimerSnapshot")
}

// Variance returns the variance of the values at the time the snapshot was
// taken.
func (t *timerSnapshot) Variance() float64 { return 0.0 }
