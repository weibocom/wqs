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

package utils

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	latencyLt2ms int = iota
	latencyLt5ms
	latencyLt10ms
	latencyLt20ms
	latencyLt50ms
	latencyGt50ms
)

type TesterTask func(bt *BenchmarkTester, index int) error
type TesterClean func(bt *BenchmarkTester)

type BenchmarkTester struct {
	running         int64
	Count           int64
	failure         int64
	concurrentLevel int
	duration        int
	responseTime    []int64
	lock            sync.Mutex
	ready           sync.WaitGroup
	finish          sync.WaitGroup
	testStart       chan int
	task            TesterTask
	cleanHandler    TesterClean
	err             error
}

func (bt *BenchmarkTester) recardResponseTime(latency time.Duration) {
	switch {
	case latency < 2:
		atomic.AddInt64(&(bt.responseTime[latencyLt2ms]), 1)
	case latency >= 2 && latency < 5:
		atomic.AddInt64(&(bt.responseTime[latencyLt5ms]), 1)
	case latency >= 5 && latency < 10:
		atomic.AddInt64(&(bt.responseTime[latencyLt10ms]), 1)
	case latency >= 10 && latency < 20:
		atomic.AddInt64(&(bt.responseTime[latencyLt20ms]), 1)
	case latency >= 20 && latency < 50:
		atomic.AddInt64(&(bt.responseTime[latencyLt50ms]), 1)
	default:
		atomic.AddInt64(&(bt.responseTime[latencyGt50ms]), 1)
	}
}

func (bt *BenchmarkTester) result() {
	if bt.cleanHandler != nil {
		bt.cleanHandler(bt)
	}
	fmt.Printf("Benchmark Result:\n")
	fmt.Printf("\tTotal %d ops, %d%% ops failed.\n", bt.Count, bt.failure*100/bt.Count)
	if bt.err != nil {
		fmt.Printf("\tLast error is : %s\n", bt.err.Error())
	}
	fmt.Printf("\t%2d%% ops < 2ms\n", bt.responseTime[latencyLt2ms]*100/bt.Count)
	fmt.Printf("\t%2d%% ops 2-5ms\n", bt.responseTime[latencyLt5ms]*100/bt.Count)
	fmt.Printf("\t%2d%% ops 5-10ms\n", bt.responseTime[latencyLt10ms]*100/bt.Count)
	fmt.Printf("\t%2d%% ops 10-20ms\n", bt.responseTime[latencyLt20ms]*100/bt.Count)
	fmt.Printf("\t%2d%% ops 20-50ms\n", bt.responseTime[latencyLt50ms]*100/bt.Count)
	fmt.Printf("\t%2d%% ops >50ms\n", bt.responseTime[latencyGt50ms]*100/bt.Count)
}

func (bt *BenchmarkTester) taskLoop(index int) {
	bt.ready.Done()
	<-bt.testStart

	for atomic.LoadInt64(&bt.running) != 0 {
		start := time.Now()
		err := bt.task(bt, index)
		bt.recardResponseTime(time.Since(start) / time.Millisecond)
		atomic.AddInt64(&bt.Count, 1)
		if err != nil {
			atomic.AddInt64(&bt.failure, 1)
			bt.err = err
		}
	}

	bt.finish.Done()
}

func (bt *BenchmarkTester) Run() error {

	if bt.task == nil {
		panic("BenchmarkTester.testing must be assigned!!!")
	}

	for i := 0; i < bt.concurrentLevel; i++ {
		bt.ready.Add(1)
		bt.finish.Add(1)
		go bt.taskLoop(i)
	}
	bt.ready.Wait()
	close(bt.testStart)

	for i := 0; i < bt.duration; i++ {
		now := time.Now().UnixNano()
		count1 := atomic.LoadInt64(&bt.Count)
		time.Sleep(time.Second)
		dlt := time.Now().UnixNano() - now
		count2 := atomic.LoadInt64(&bt.Count)
		if dlt <= 0 {
			dlt = 1
		}
		fmt.Printf("At %dth second : %d ops/s\n", i+1, (count2-count1)*int64(time.Second)/dlt)
	}

	atomic.StoreInt64(&bt.running, 0) //Notify taskLoop break
	bt.finish.Wait()                  //Wait taskLoop break
	bt.result()
	return nil
}

func NewBenchmarkTester(concurrentLevel int, duration int, task TesterTask, clean TesterClean) *BenchmarkTester {
	return &BenchmarkTester{
		running:         1,
		concurrentLevel: concurrentLevel,
		duration:        duration,
		responseTime:    make([]int64, latencyGt50ms+1),
		testStart:       make(chan int, 0),
		task:            task,
		cleanHandler:    clean,
	}
}
