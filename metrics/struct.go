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
)

var LOCAL = "localhost"

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
