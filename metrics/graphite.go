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
	"net/http"

	"github.com/weibocom/wqs/log"
)

const (
	_OVERVIEW_URI_TPL = "http://%s/render?from=%d&until=%d&target=%s&format=json"
	_GROUP_URI_TPL    = "http://%s/render?from=%d&until=%d&target=%s&group=%s&queue=%s&action=%s&format=json"
)

type RoamStruct struct {
	Type    string  `json:"type"`
	Queue   string  `json:"queue"`
	Group   string  `json:"group"`
	Action  string  `json:"action"`
	Total   int64   `json:"total_count"`
	AvgTime float64 `json:"avg_time"`
}

type RoamClient struct {
	root string
	cli  *http.Client
}

func newRoamClient(root string) *RoamClient {
	return &RoamClient{
		cli: &http.Client{},
	}
}

func (m *RoamClient) Send(key string, data []byte) (err error) {
	if len(data) == 0 || string(data) == "[]" {
		return
	}
	sts, err := transToRoamStruct(data)
	if err != nil {
		log.Warnf("store profile log err : %v", err)
		return err
	}
	for i := range sts {
		log.Profile("%s", sts[i])
	}
	return
}

func (m *RoamClient) Overview(start, end, step int64, host string) (ret string, err error) {
	reqURL := fmt.Sprintf(_OVERVIEW_URI_TPL, m.root, start, end, host)
	res, err := m.doRequest(reqURL)
	_ = res
	return
}

func (m *RoamClient) GroupMetrics(start, end, step int64, group, queue string) (ret string, err error) {
	reqURL := fmt.Sprintf(_GROUP_URI_TPL, m.root, start, end, "*", group, queue, "*")
	res, err := m.doRequest(reqURL)
	_ = res
	return
}

func (m *RoamClient) doRequest(reqURL string) (ret string, err error) {
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

func transToRoamStruct(data []byte) (jsonStrs []string, err error) {
	var results []*MetricsStat
	err = json.Unmarshal(data, &results)
	if err != nil {
		return nil, err
	}

	action := func(st *MetricsStat) []*RoamStruct {
		ret := make([]*RoamStruct, 0, 2)
		actions := []string{SENT, RECV}
		for _, act := range actions {
			rst := &RoamStruct{
				Type:   WQS,
				Queue:  st.Queue,
				Group:  st.Group,
				Action: act,
			}
			switch act {
			case SENT:
				rst.Total = st.Sent.Total
				rst.AvgTime = st.Sent.Elapsed
			case RECV:
				rst.Total = st.Recv.Total
				rst.AvgTime = st.Recv.Elapsed
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
				log.Warnf("transToRoamStruct err : %v", err)
				continue
			}
			jsonStrs = append(jsonStrs, string(stData))
		}
	}
	return
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
