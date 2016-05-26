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
	"strings"

	"github.com/weibocom/wqs/log"

	"github.com/rcrowley/go-metrics"
)

type MetricsStat struct {
	Queue string
	Group string
	Sent  *MetricsSt
	Recv  *MetricsSt
	Accum uint64
	TS    int64
}

type MetricsSt struct {
	Total   int64
	Cost    float64
	Latency float64
}

func snapShotMetricsSts(r metrics.Registry) []*MetricsStat {
	retMap := make(map[string]*MetricsStat)
	each := func(k string, _ interface{}) {
		c := metrics.GetOrRegisterCounter(k, r)
		ks := strings.Split(k, "#")
		if len(ks) != 4 {
			return
		}
		k = strings.Join(ks[:2], "#")
		if _, ok := retMap[k]; !ok {
			retMap[k] = &MetricsStat{
				Sent: &MetricsSt{},
				Recv: &MetricsSt{},
			}
		}
		st := retMap[k]
		st.Queue = ks[0]
		st.Group = ks[1]

		if ks[2] == SENT {
			switch ks[3] {
			case QPS:
				st.Sent.Total = c.Count()
			case COST:
				st.Sent.Cost = float64(c.Count())
			}
		} else if ks[2] == RECV {
			switch ks[3] {
			case QPS:
				st.Recv.Total = c.Count()
			case COST:
				st.Recv.Cost = float64(c.Count())
			case LATENCY:
				st.Recv.Latency = float64(c.Count())
			}
		}
	}
	r.Each(each)

	var list []*MetricsStat
	for k := range retMap {
		list = append(list, retMap[k])
		retMap[k].Accum = uint64(retMap[k].Sent.Total) - uint64(retMap[k].Recv.Total)
		retMap[k].Sent.Cost = retMap[k].Sent.Cost / float64(retMap[k].Sent.Total)
		retMap[k].Recv.Cost = retMap[k].Recv.Cost / float64(retMap[k].Recv.Total)
		retMap[k].Recv.Latency = retMap[k].Recv.Latency / float64(retMap[k].Recv.Total)

		log.Infof("%s %s %d sent:%+v recv:%+v",
			retMap[k].Queue, retMap[k].Group, retMap[k].Accum,
			retMap[k].Sent, retMap[k].Recv)
	}
	return list
}
