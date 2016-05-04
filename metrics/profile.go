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

	"github.com/weibocom/wqs/log"
)

var (
	statisticMap map[string]*statisticItem //key=$queue.$group.$action eg:remind.if.s remind.if.r
	stopChan     chan error
	mu           sync.Mutex
)

func init() {
	statisticMap = make(map[string]*statisticItem)
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-stopChan:
				ticker.Stop()
				storeStatistic()
			case <-ticker.C:
				storeStatistic()
			}
		}
	}()
}

func storeStatistic() {
	for _, v := range statisticMap {
		log.Profile("%s", v.getStatistic())
		v.clear()
	}
}

func StatisticSend(queue, group string, cost int64) {
	doStatistic(queue, group, "send", cost)
}

func StatisticRecv(queue, group string, cost int64) {
	doStatistic(queue, group, "recv", cost)
}

func doStatistic(queue, group, action string, cost int64) {
	key := fmt.Sprintf("%s.%s.%s", queue, group, action)
	statisticItem, ok := statisticMap[key]
	if ok {
		statisticItem.statistic(cost)
	} else {
		mu.Lock()
		statisticItem = newStatisticItem(queue, group, action)
		statisticMap[key] = statisticItem
		mu.Unlock()
		statisticItem.statistic(cost)
	}
}

type statisticItem struct {
	queue  string
	group  string
	action string
	count  int64
	cost   int64
}

func newStatisticItem(queue, group, action string) *statisticItem {
	return &statisticItem{
		queue:  queue,
		group:  group,
		action: action,
		count:  int64(0),
		cost:   int64(0),
	}
}

func (si *statisticItem) clear() {
	si.count = 0
	si.cost = 0
}

func (si *statisticItem) statistic(cost int64) {
	mu.Lock()
	si.count++
	si.cost += cost
	mu.Unlock()
}

func (si *statisticItem) getStatistic() string {
	cost := si.cost
	count := si.count
	avg := float64(0)
	if count != 0 {
		avg = float64(cost) / float64(count)
	}
	return `{"type":"WQS","queue":"` + si.queue + `","group":"` + si.group + `","action":"` + si.action + `","total_count":` + strconv.FormatInt(count, 10) + `,"avg_time":` + strconv.FormatFloat(avg, 'f', 2, 64) + `}`
}
