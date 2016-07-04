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

import "encoding/json"

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

type dataSets []*metricsData

func (m dataSets) String() string {
	if m == nil {
		return "[]"
	}

	data, _ := json.Marshal(m)
	return string(data)
}
