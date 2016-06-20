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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/rcrowley/go-metrics"
)

var LOCAL = "localhost"

//type metricsOverview struct {
//	Endpoint string  `json:"endpoint"`
//	Ts       int64   `json:"ts"`
//	Sent     int64   `json:"sent"`
//	Recv     int64   `json:"recv"`
//	Accum    int64   `json:"accum"`
//	Elapsed  float64 `json:"elapsed"`
//}

type metricsStat struct {
	Endpoint string         `json:"endpoint"`
	Queue    string         `json:"queue"`
	Group    string         `json:"group"`
	Sent     *metricsStruct `json:"sent"`
	Recv     *metricsStruct `json:"recv"`
	Accum    int64          `json:"accum"`
	TS       int64          `json:"ts"`
}

type metricsStruct struct {
	Total   int64   `json:"total"`
	Elapsed float64 `json:"cost"`
	Latency float64 `json:"latency"`

	Scale map[string]int64 `json:"scale"`
}

func snapshotMetricsStats(r metrics.Registry) (list []*metricsStat) {
	retMap := make(map[string]*metricsStat)
	each := func(k string, _ interface{}) {

		c := metrics.GetOrRegisterCounter(k, r)
		ks := strings.Split(k, "#")
		if len(ks) != 4 {
			return
		}
		k = strings.Join(ks[:2], "#")
		if _, ok := retMap[k]; !ok {
			retMap[k] = &metricsStat{
				Sent: &metricsStruct{
					Scale: make(map[string]int64),
				},
				Recv: &metricsStruct{
					Scale: make(map[string]int64),
				},
			}
		}
		st := retMap[k]
		st.Queue = ks[0]
		st.Group = ks[1]

		if ks[2] == KeySent {
			switch ks[3] {
			case KeyQps:
				st.Sent.Total = c.Count()
			case KeyElapsed:
				st.Sent.Elapsed = float64(c.Count())
			default:
				st.Sent.Scale[ks[3]] = c.Count()
			}
		} else if ks[2] == KeyRecv {
			switch ks[3] {
			case KeyQps:
				st.Recv.Total = c.Count()
			case KeyElapsed:
				st.Recv.Elapsed = float64(c.Count())
			case KeyLatency:
				st.Recv.Latency = float64(c.Count())
			default:
				st.Recv.Scale[ks[3]] = c.Count()
			}
		}
	}
	r.Each(each)

	for k := range retMap {
		list = append(list, retMap[k])
		retMap[k].Endpoint = LOCAL
		retMap[k].Accum = retMap[k].Sent.Total - retMap[k].Recv.Total
		if retMap[k].Sent.Total > 0 {
			retMap[k].Sent.Elapsed = truncateFloat64(retMap[k].Sent.Elapsed/float64(retMap[k].Sent.Total), 2)
		} else {
			retMap[k].Sent.Elapsed = 0.0
		}
		if retMap[k].Recv.Total > 0 {
			retMap[k].Recv.Elapsed = truncateFloat64(retMap[k].Recv.Elapsed/float64(retMap[k].Recv.Total), 2)
		} else {
			retMap[k].Recv.Elapsed = 0.0
		}
		if retMap[k].Recv.Total > 0 {
			retMap[k].Recv.Latency = truncateFloat64(retMap[k].Recv.Latency/float64(retMap[k].Recv.Total), 2)
		} else {
			retMap[k].Recv.Latency = 0.0
		}
	}
	return
}

func truncateFloat64(num float64, bit int) float64 {
	ret, _ := strconv.ParseFloat(fmt.Sprintf("%.[2]*[1]f", num, bit), 64)
	return ret
}

// metrics 模块对外输出数据时，序列化使用的数据结构
type metricsDataPoint struct {
	TimeStamp int64   `json:"t"`
	Value     float64 `json:"v"`
}

type metricsData struct {
	Points []metricsDataPoint `json:"points"`
}

func (m *metricsData) String() string {
	data, _ := json.Marshal(m)
	return string(data)
}

type metricsDataSet []*metricsData

func (m metricsDataSet) String() string {
	if m == nil {
		return "[]"
	}

	data, _ := json.Marshal(m)
	return string(data)
}
