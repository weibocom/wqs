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

import "time"

const (
	AllHost    = "*"
	AllMetrics = "*"

	defaultRWTimeout = time.Second * 1
)

type MetricsQueryParam struct {
	Host       string
	Group      string
	Queue      string
	ActionKey  string
	MetricsKey string
	StartTime  int64
	EndTime    int64
	Step       int64
}

type MetricsStatWriter interface {
	Send(uri string, snapshot []*MetricsStat) error
}

type MetricsStatReader interface {
	Overview(param *MetricsQueryParam) (data string, err error)
	GroupMetrics(param *MetricsQueryParam) (data string, err error)
}
