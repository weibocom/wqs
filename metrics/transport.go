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
	"net/url"
	"strings"
)

type MetricsQueryParam struct {
	Host        string
	Action      string
	Group       string
	Queue       string
	MetricsName string
	Merge       bool
}

func NewMetricsQueryParam(host, action, group, queue,
	metricsName string, merge bool) *MetricsQueryParam {

	return &MetricsQueryParam{
		Host:        host,
		Action:      action,
		Group:       group,
		Queue:       queue,
		MetricsName: metricsName,
		Merge:       merge,
	}
}

func NewMetricsQueryParamFormURL(args url.Values) *MetricsQueryParam {
	var merge bool
	switch strings.ToUpper(args.Get("merge")) {
	case "TRUE":
		merge = true
	case "FALSE":
		merge = false
	default:
		merge = false
	}

	return &MetricsQueryParam{
		Host:        args.Get("host"),
		Action:      args.Get("action"),
		Group:       args.Get("group"),
		Queue:       args.Get("queue"),
		MetricsName: args.Get("metrics_name"),
		Merge:       merge,
	}
}

type MetricsStatWriter interface {
	Send(uri string, snapshot []*MetricsStat) error
}

type MetricsStatReader interface {
	Overview(start, end, step int64, params *MetricsQueryParam) (ret string, err error)
	GroupMetrics(start, end, step int64, params *MetricsQueryParam) (ret string, err error)
}
