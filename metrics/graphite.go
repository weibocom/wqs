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
	"net"
	"net/http"
	"strings"

	"github.com/weibocom/wqs/log"
)

type graphiteType string

const (
	counterType graphiteType = "c"
	timerType   graphiteType = "ms"

	messageMaxLen = 65000
)

func (t graphiteType) String() string {
	return string(t)
}

type graphiteClient struct {
	root        string
	addr        string
	servicePool string
	cli         *http.Client
}

func newGraphiteClient(root, addr, servicePool string) *graphiteClient {
	return &graphiteClient{
		root:        root,
		addr:        addr,
		servicePool: servicePool,
		cli:         &http.Client{},
	}
}

func (g *graphiteClient) Send(_ string, snapshot []*metricsStat) error {
	if len(snapshot) == 0 {
		return nil
	}

	conn, err := net.Dial("udp", g.addr)
	if err != nil {
		return err
	}

	ip := strings.SplitN(conn.LocalAddr().String(), ":", 2)[0]
	ip = strings.Replace(ip, ".", "_", -1)
	items := transToGraphiteItems(snapshot)
	messages := transGraphiteItemsToMessages(ip, g.servicePool, items)

	for _, message := range messages {
		_, err = conn.Write([]byte(message))
		if err != nil {
			log.Errorf("graphite send message error: %v", err)
		}
		log.Debugf("graphite message: %s", message)
	}

	return conn.Close()
}

func (m *graphiteClient) genGraphiteRequestURL(startTime int64, endTime int64, target string) string {
	return fmt.Sprintf("http://%s/render?from=%d&until=%d&target=%s&format=json",
		m.root, startTime, endTime, target)
}

func (m *graphiteClient) genTarget(param *MetricsQueryParam) string {
	return fmt.Sprintf("stats_byhost.openapi_profile.%s.byhost.%s.%s.%s.%s.%s",
		m.servicePool, param.Host, param.Queue, param.Group, param.ActionKey, param.MetricsKey)
}

func (m *graphiteClient) Overview(param *MetricsQueryParam) (data string, err error) {
	url := m.genGraphiteRequestURL(param.StartTime, param.EndTime, param.Host)
	dataset, err := m.doRequest(url)
	if err != nil {
		return "", err
	}
	return dataset.String(), nil
}

func (m *graphiteClient) GroupMetrics(param *MetricsQueryParam) (data string, err error) {
	target := m.genTarget(param)
	url := m.genGraphiteRequestURL(param.StartTime, param.EndTime, target)
	dataset, err := m.doRequest(url)
	if err != nil {
		return "", err
	}

	if param.Host == AllHost {
		return mergeDataSet(dataset).String(), nil
	}
	return dataset.String(), nil
}

func (m *graphiteClient) doRequest(url string) (metricsDatas metricsDataSet, err error) {

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("graphite request %d error", resp.StatusCode)
	}

	var graphiteResponse []*graphiteDataSet
	err = json.NewDecoder(resp.Body).Decode(&graphiteResponse)
	if err != nil {
		return nil, err
	}

	for _, dataset := range graphiteResponse {
		data := new(metricsData)
		for _, point := range dataset.DataPoints {
			data.Points = append(data.Points, metricsDataPoint{
				TimeStamp: point.GetTimestamp(),
				Value:     point.GetValue(),
			})
		}
		metricsDatas = append(metricsDatas, data)
	}

	return metricsDatas, nil
}

type graphiteItem struct {
	key   string
	value interface{}
	typ   graphiteType
}

func transToGraphiteItems(states []*metricsStat) []*graphiteItem {
	var items []*graphiteItem
	for _, state := range states {
		items = append(items, &graphiteItem{
			key:   strings.Join([]string{state.Queue, state.Group, KeySent, KeyQps}, "."),
			value: state.Sent.Total,
			typ:   counterType,
		})
		items = append(items, &graphiteItem{
			key:   strings.Join([]string{state.Queue, state.Group, KeySent, KeyElapsed}, "."),
			value: state.Sent.Elapsed,
			typ:   timerType,
		})

		for k, v := range state.Sent.Scale {
			items = append(items, &graphiteItem{
				key:   strings.Join([]string{state.Queue, state.Group, KeySent, k}, "."),
				value: v,
				typ:   counterType,
			})
		}

		items = append(items, &graphiteItem{
			key:   strings.Join([]string{state.Queue, state.Group, KeyRecv, KeyQps}, "."),
			value: state.Recv.Total,
			typ:   counterType,
		})
		items = append(items, &graphiteItem{
			key:   strings.Join([]string{state.Queue, state.Group, KeyRecv, KeyElapsed}, "."),
			value: state.Recv.Elapsed,
			typ:   timerType,
		})
		items = append(items, &graphiteItem{
			key:   strings.Join([]string{state.Queue, state.Group, KeyRecv, KeyLatency}, "."),
			value: state.Recv.Latency,
			typ:   timerType,
		})

		for k, v := range state.Recv.Scale {
			items = append(items, &graphiteItem{
				key:   strings.Join([]string{state.Queue, state.Group, KeyRecv, k}, "."),
				value: v,
				typ:   counterType,
			})
		}
	}
	return items
}

func transGraphiteItemsToMessages(localIP string, servicePool string, items []*graphiteItem) []string {
	messages := make([]string, 0)
	segments := make([]string, 0)
	segmentsLength := 0

	for _, item := range items {
		var segment string
		switch item.value.(type) {
		case int64:
			segment = fmt.Sprintf("openapi_profile.%s.byhost.%s.%s:%d|%s",
				servicePool, localIP, item.key, item.value.(int64), item.typ)
		case float64:
			segment = fmt.Sprintf("openapi_profile.%s.byhost.%s.%s:%.2f|%s",
				servicePool, localIP, item.key, item.value.(float64), item.typ)
		default:
			continue
		}

		if segmentsLength+len(segment) > messageMaxLen {
			message := strings.Join(segments, "\n") + "\n"
			messages = append(messages, message)
			segments = make([]string, 0)
			segmentsLength = 0
		}
		segments = append(segments, segment)
		segmentsLength += len(segment)
	}
	message := strings.Join(segments, "\n") + "\n"
	messages = append(messages, message)

	return messages
}

type dataPoint [2]interface{}

func (c dataPoint) GetValue() float64 {
	switch c[0].(type) {
	case int64:
		return float64(c[0].(int64))
	case float64:
		return c[0].(float64)
	default:
		return 0.0
	}
}

func (c dataPoint) GetTimestamp() int64 {
	switch c[1].(type) {
	case int64:
		return c[1].(int64)
	case float64:
		return int64(c[1].(float64))
	default:
		return 0
	}
}

type graphiteDataSet struct {
	Target     string      `json:"target"`
	DataPoints []dataPoint `json:"datapoints"`
}

func mergeDataSet(datasets metricsDataSet) *metricsData {
	if datasets == nil || len(datasets) == 0 {
		return &metricsData{Points: make([]metricsDataPoint, 0)}
	}

	data := &metricsData{Points: make([]metricsDataPoint, len(datasets[0].Points))}
	for _, dataset := range datasets {
		if len(dataset.Points) != len(data.Points) {
			continue
		}
		for i, point := range dataset.Points {
			data.Points[i].TimeStamp = point.TimeStamp
			data.Points[i].Value += point.Value
		}
	}
	return data
}
