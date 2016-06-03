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
	_OVERVIEW_URI_TPL = "http://%s/render?from=%d&until=%d&target=%s&format=json"
	_GROUP_URI_TPL    = "http://%s/render?from=%d&until=%d&target=%s&group=%s&queue=%s&action=%s&format=json"
	MESSAGE_MAX_LEN   = 65000
)

type graphiteType string

const (
	COUNTER graphiteType = "c"
	TIMER   graphiteType = "ms"
)

func (t graphiteType) String() string {
	return string(t)
}

type graphiteStruct struct {
	Type    string  `json:"type"`
	Queue   string  `json:"queue"`
	Group   string  `json:"group"`
	Action  string  `json:"action"`
	Total   int64   `json:"total_count"`
	AvgTime float64 `json:"avg_time"`
	Latency float64 `json:"latency"`
	Accum   int64   `json:"accum"`

	Scale map[string]int64 `json:"scale"`
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

func (m *graphiteClient) Overview(start, end, step int64, host string) (ret string, err error) {
	reqURL := fmt.Sprintf(_OVERVIEW_URI_TPL, m.root, start, end, host)
	res, err := m.doRequest(reqURL)
	_ = res
	return
}

func (m *graphiteClient) GroupMetrics(start, end, step int64, group, queue string) (ret string, err error) {
	reqURL := fmt.Sprintf(_GROUP_URI_TPL, m.root, start, end, "*", group, queue, "*")
	res, err := m.doRequest(reqURL)
	_ = res
	return
}

func (m *graphiteClient) doRequest(reqURL string) (ret string, err error) {
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
	_ = res
	return
}

func transToGraphiteStruct(results []*MetricsStat) (jsonStrs []string, err error) {
	action := func(st *MetricsStat) []*graphiteStruct {
		ret := make([]*graphiteStruct, 0, 2)
		actions := []string{SENT, RECV}
		for _, act := range actions {
			rst := &graphiteStruct{
				Type:   WQS,
				Queue:  st.Queue,
				Group:  st.Group,
				Action: act,
			}
			switch act {
			case SENT:
				rst.Total = st.Sent.Total
				rst.AvgTime = st.Sent.Elapsed
				rst.Accum = st.Accum
				rst.Latency = st.Sent.Latency
				rst.Scale = st.Sent.Scale
			case RECV:
				rst.Total = st.Recv.Total
				rst.AvgTime = st.Recv.Elapsed
				rst.Accum = st.Accum
				rst.Latency = st.Recv.Latency
				rst.Scale = st.Recv.Scale
			}
			ret = append(ret, rst)
		}
		return ret
	}

	for _, st := range results {
		rsts := action(st)
		for _, rst := range rsts {
			stData, err := json.Marshal(rst)
			if err != nil {
				log.Warnf("transToGraphiteStruct err : %v", err)
				continue
			}
			jsonStrs = append(jsonStrs, string(stData))
		}
	}
	return
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
			key:   strings.Join([]string{state.Queue, state.Group, SENT, QPS}, "."),
			value: state.Sent.Total,
			typ:   COUNTER,
		})
		items = append(items, &graphiteItem{
			key:   strings.Join([]string{state.Queue, state.Group, SENT, ELAPSED}, "."),
			value: state.Sent.Elapsed,
			typ:   TIMER,
		})

		for k, v := range state.Sent.Scale {
			items = append(items, &graphiteItem{
				key:   strings.Join([]string{state.Queue, state.Group, SENT, k}, "."),
				value: v,
				typ:   COUNTER,
			})
		}

		items = append(items, &graphiteItem{
			key:   strings.Join([]string{state.Queue, state.Group, RECV, QPS}, "."),
			value: state.Recv.Total,
			typ:   COUNTER,
		})
		items = append(items, &graphiteItem{
			key:   strings.Join([]string{state.Queue, state.Group, RECV, ELAPSED}, "."),
			value: state.Recv.Elapsed,
			typ:   TIMER,
		})
		items = append(items, &graphiteItem{
			key:   strings.Join([]string{state.Queue, state.Group, RECV, LATENCY}, "."),
			value: state.Recv.Latency,
			typ:   TIMER,
		})

		for k, v := range state.Recv.Scale {
			items = append(items, &graphiteItem{
				key:   strings.Join([]string{state.Queue, state.Group, RECV, k}, "."),
				value: v,
				typ:   COUNTER,
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

		if segmentsLength+len(segment) > MESSAGE_MAX_LEN {
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

type GraphiteSer struct {
	Target string `json:"target"`
	Data   []Cell `json:"datapoints"`
}

type GraphiteResponse []*GraphiteSer

func parseGraphiteResponse(data []byte) (resp GraphiteResponse, err error) {
	err = json.Unmarshal(data, &resp)
	return
}
