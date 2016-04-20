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
	"strconv"
	"time"

	"github.com/weibocom/wqs/config"
	redis "gopkg.in/redis.v3"
)

const (
	Interval = 10 //10s
)

type Monitor struct {
	redisClient  *redis.Client
	statisticMap map[string]*int64 //key=$queue.$group.$action eg:remind.if.s remind.if.r
}

func NewMonitor(config config.Config) *Monitor {
	monitor := Monitor{}
	monitor.redisClient = redis.NewClient(&redis.Options{
		Addr:     config.RedisAddr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	monitor.statisticMap = make(map[string]*int64)
	return &monitor
}

func (this *Monitor) Start() {
	go func() {
		for {
			this.storeStatistic()
			time.Sleep(Interval * time.Second)
		}
	}()
}

func (this *Monitor) storeStatistic() {
	time := time.Now().Unix() / 10 * 10
	for k, v := range this.statisticMap {
		go this.redisClient.HIncrBy(k, strconv.Itoa(int(time)), *v)
		*v = 0
	}
}

func (this *Monitor) StatisticSend(queue string, group string, count int64) {
	key := queue + "." + group + ".s"
	this.doStatistic(key, count)
}

func (this *Monitor) StatisticReceive(queue string, group string, count int64) {
	key := queue + "." + group + ".r"
	this.doStatistic(key, count)
}

func (this *Monitor) doStatistic(key string, count int64) {
	data, ok := this.statisticMap[key]
	if ok {
		*data++
	} else {
		data = new(int64)
		*data = 0
		this.statisticMap[key] = data
	}
}

func (this *Monitor) GetSendMetrics(queue string, group string, start int64, end int64, intervalnum int) map[string][]int64 {
	key := queue + "." + group + ".s"
	return this.doGetMetrics(key, start, end, intervalnum)
}

func (this *Monitor) GetReceiveMetrics(queue string, group string, start int64, end int64, intervalnum int) map[string][]int64 {
	key := queue + "." + group + ".r"
	return this.doGetMetrics(key, start, end, intervalnum)
}

func (this *Monitor) doGetMetrics(key string, start int64, end int64, intervalnum int) map[string][]int64 {
	metricsMap := make(map[string][]int64)
	time := make([]int64, 0)
	data := make([]int64, 0)
	field := make([]string, 0)
	start = start / 10 * 10
	end = end / 10 * 10
	for i := start; i <= end; i += Interval * int64(intervalnum) {
		time = append(time, i)
		field = append(field, strconv.Itoa(int(i)))
	}
	result, _ := this.redisClient.HMGet(key, field...).Result()
	for _, value := range result {
		if value != nil {
			s, ok := value.(string)
			if ok {
				count, _ := strconv.Atoi(s)
				data = append(data, int64(count))
			} else {
				data = append(data, int64(0))
			}
		} else {
			data = append(data, int64(0))
		}
	}
	metricsMap["time"] = time
	metricsMap["data"] = data
	return metricsMap
}
