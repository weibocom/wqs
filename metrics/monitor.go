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
	"fmt"
	"strconv"
	"sync"
	"time"

	log "github.com/cihub/seelog"
	redis "gopkg.in/redis.v3"
)

const (
	Interval = 10 //10s
)

type MetricsObj map[string][]int64

type Monitor struct {
	redisClient  *redis.Client
	statisticMap map[string]int64 //key=$queue.$group.$action eg:remind.if.s remind.if.r
	stopChan     chan error
	mu           sync.Mutex
}

func NewMonitor(redisAddr string) *Monitor {
	return (&Monitor{
		redisClient: redis.NewClient(&redis.Options{
			Addr:     redisAddr,
			Password: "", // no password set
			DB:       0,  // use default DB
		}),
		statisticMap: make(map[string]int64),
	}).start()
}

func (m *Monitor) start() *Monitor {
	go func() {
		ticker := time.NewTicker(Interval * time.Second)
		for {
			select {
			case <-m.stopChan:
				ticker.Stop()
				m.storeStatistic()
			case <-ticker.C:
				m.storeStatistic()
			}
		}
	}()
	return m
}

func (m *Monitor) storeStatistic() {
	t := time.Now().Unix() / 10 * 10
	field := strconv.FormatInt(int64(t), 10)
	snapshot := make(map[string]int64)
	//reduce hoding mutex druation
	m.mu.Lock()
	for k, v := range m.statisticMap {
		snapshot[k] = v
		m.statisticMap[k] = 0
	}
	m.mu.Unlock()

	for k, v := range snapshot {
		_, err := m.redisClient.HIncrBy(k, field, v).Result()
		if err != nil {
			log.Warnf("storeStatistic HIncrBy err: %s", err)
		}
	}
}

func (m *Monitor) StatisticSend(queue string, group string, count int64) {
	key := fmt.Sprintf("%s.%s.s", queue, group)
	m.doStatistic(key, count)
}

func (m *Monitor) StatisticReceive(queue string, group string, count int64) {
	key := fmt.Sprintf("%s.%s.r", queue, group)
	m.doStatistic(key, count)
}

func (m *Monitor) doStatistic(key string, count int64) {
	m.mu.Lock()
	m.statisticMap[key] += count
	m.mu.Unlock()
}

func (m *Monitor) GetSendMetrics(queue string, group string,
	start int64, end int64, intervalnum int) (MetricsObj, error) {

	key := fmt.Sprintf("%s.%s.s", queue, group)
	return m.doGetMetrics(key, start, end, int64(intervalnum))
}

func (m *Monitor) GetReceiveMetrics(queue string, group string,
	start int64, end int64, intervalnum int) (MetricsObj, error) {

	key := fmt.Sprintf("%s.%s.r", queue, group)
	return m.doGetMetrics(key, start, end, int64(intervalnum))
}

func (m *Monitor) doGetMetrics(key string, start int64,
	end int64, intervalnum int64) (MetricsObj, error) {

	metricsMap := make(MetricsObj)
	time := make([]int64, 0)
	data := make([]int64, 0)
	field := make([]string, 0)

	start = start / 10 * 10
	end = end / 10 * 10
	for i := start; i <= end; i += Interval * intervalnum {
		time = append(time, i)
		field = append(field, strconv.FormatInt(i, 10))
	}

	result, err := m.redisClient.HMGet(key, field...).Result()
	if err != nil {
		return nil, err
	}

	for _, value := range result {
		if value != nil {
			s, ok := value.(string)
			if ok {
				count, _ := strconv.ParseInt(s, 10, 0)
				data = append(data, count)
			} else {
				data = append(data, int64(0))
			}
		} else {
			data = append(data, int64(0))
		}
	}
	metricsMap["time"] = time
	metricsMap["data"] = data
	return metricsMap, nil
}
