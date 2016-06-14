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
	"io/ioutil"
	"net"
	"net/http"
	"strings"

	"github.com/weibocom/wqs/log"
)

const (
	graphiteRequestTpl = "http://%s/render?from=%d&until=%d&target=%s&format=json"

	allMask    = "*"
	allHost    = allMask
	allMetrics = allMask

	messageMaxLen = 65000
)

type graphiteType string

const (
	counterType graphiteType = "c"
	timerType   graphiteType = "ms"
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

func (g *graphiteClient) Send(_ string, snapshot []*MetricsStat) error {
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

func (m *graphiteClient) Overview(start, end, step int64, params *MetricsQueryParam) (ret string, err error) {
	reqURL := fmt.Sprintf(graphiteRequestTpl, m.root, start, end, params.Host)
	res, err := m.doRequest(reqURL, false)
	_ = res
	return
}

func (m *graphiteClient) GroupMetrics(start, end, step int64, params *MetricsQueryParam) (ret string, err error) {
	target := m.factoryTarget(params.Host, params.Queue, params.Group, params.Action, params.MetricsName)
	reqURL := fmt.Sprintf(graphiteRequestTpl, m.root, start, end, target)
	return m.doRequest(reqURL, params.Merge)
}

func (m *graphiteClient) doRequest(reqURL string, merge bool) (ret string, err error) {
	req, err := http.NewRequest("GET", reqURL, nil)
	if err != nil {
		return "", err
	}
	resp, err := m.cli.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	res, err := parseGraphiteResponse(data)
	if err != nil {
		return "", err
	}
	if merge {
		data, err = mergeMetrics(res)
		if err != nil {
			return "", err
		}
	}
	return string(data), nil
}

func (m *graphiteClient) factoryTarget(host, queue, group, action, metrics string) string {
	return fmt.Sprintf("stats_byhost.openapi_profile.%s.byhost.%s.%s.%s.%s.%s",
		m.servicePool,
		host,
		queue,
		group,
		action,
		metrics,
	)
}

type graphiteItem struct {
	key   string
	value interface{}
	typ   graphiteType
}

func transToGraphiteItems(states []*MetricsStat) []*graphiteItem {
	var items []*graphiteItem
	for _, state := range states {
		items = append(items, &graphiteItem{
			key:   strings.Join([]string{state.Queue, state.Group, sentKey, qpsKey}, "."),
			value: state.Sent.Total,
			typ:   counterType,
		})
		items = append(items, &graphiteItem{
			key:   strings.Join([]string{state.Queue, state.Group, sentKey, elapsedKey}, "."),
			value: state.Sent.Elapsed,
			typ:   timerType,
		})

		for k, v := range state.Sent.Scale {
			items = append(items, &graphiteItem{
				key:   strings.Join([]string{state.Queue, state.Group, sentKey, k}, "."),
				value: v,
				typ:   counterType,
			})
		}

		items = append(items, &graphiteItem{
			key:   strings.Join([]string{state.Queue, state.Group, recvKey, qpsKey}, "."),
			value: state.Recv.Total,
			typ:   counterType,
		})
		items = append(items, &graphiteItem{
			key:   strings.Join([]string{state.Queue, state.Group, recvKey, elapsedKey}, "."),
			value: state.Recv.Elapsed,
			typ:   timerType,
		})
		items = append(items, &graphiteItem{
			key:   strings.Join([]string{state.Queue, state.Group, recvKey, latencyKey}, "."),
			value: state.Recv.Latency,
			typ:   timerType,
		})

		for k, v := range state.Recv.Scale {
			items = append(items, &graphiteItem{
				key:   strings.Join([]string{state.Queue, state.Group, recvKey, k}, "."),
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
		segment := fmt.Sprintf("openapi_profile.%s.byhost.%s.%s:%v|%s",
			servicePool, localIP, item.key, item.value, item.typ)

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

type Cell [2]interface{}

func AddCell(b, c Cell) Cell {
	if c.GetTimestamp() != b.GetTimestamp() {
		return b
	}
	d := Cell{}
	d[0] = c.GetValue() + b.GetValue()
	d[1] = b.GetTimestamp()
	return d
}

func (c Cell) GetValue() float64 {
	switch c[0].(type) {
	case int64:
		return float64(c[0].(int64))
	case float64:
		return c[0].(float64)
	default:
		return 0.0
	}
}

func (c Cell) GetTimestamp() int64 {
	switch c[1].(type) {
	case int64:
		return c[1].(int64)
	case float64:
		return int64(c[1].(float64))
	default:
		return 0
	}
}

type GraphiteSer struct {
	Target string `json:"target"`
	Data   []Cell `json:"datapoints"`
}

func (gs *GraphiteSer) Add(b *GraphiteSer) {
	if len(gs.Data) != len(b.Data) {
		return
	}
	for i := range gs.Data {
		gs.Data[i] = AddCell(gs.Data[i], b.Data[i])
	}
}

type GraphiteResponse []*GraphiteSer

func parseGraphiteResponse(data []byte) (resp GraphiteResponse, err error) {
	err = json.Unmarshal(data, &resp)

	// format
	for i := range resp {
		for j := range resp[i].Data {
			resp[i].Data[j][1] = resp[i].Data[j].GetTimestamp()
			resp[i].Data[j][0] = resp[i].Data[j].GetValue()
		}
	}
	return
}

func mergeMetrics(resp GraphiteResponse) ([]byte, error) {
	if len(resp) > 1 {
		for i := 1; i < len(resp); i++ {
			resp[0].Add(resp[i])
		}
		resp = resp[:1]
	}

	return json.Marshal(resp)
}
