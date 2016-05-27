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
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/weibocom/wqs/log"

	redis "gopkg.in/redis.v3"
)

const (
	DEFAULT_RW_TIMEOUT = time.Second * 1

	ALL_MASK = "*"
)

type Transport interface {
	Send(uri string, data []byte) error
	Overview(start, end, step int64, host string) (ret string, err error)
	GroupMetrics(start, end, step int64, group, queue string) (ret string, err error)
}

type httpClient struct {
	cli *http.Client
}

func newHTTPClient() *httpClient {
	return &httpClient{
		cli: &http.Client{Timeout: DEFAULT_RW_TIMEOUT},
	}
}

func (c *httpClient) Send(uri string, data []byte) (err error) {
	log.Info(string(data))
	req, err := http.NewRequest("POST", uri, bytes.NewReader(data))
	if err != nil {
		log.Warnf("new http request err: %v", err)
		return
	}
	resp, err := c.cli.Do(req)
	if err != nil {
		log.Warnf("do http request err: %v", err)
		return
	}
	defer resp.Body.Close()

	respData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Warnf("do http request err: %v", err)
		return
	}
	// TODO check
	_ = respData
	return
}

func (c *httpClient) Overview(start, end, step int64, host string) (ret string, err error) {
	// TODO
	return
}

func (c *httpClient) GroupMetrics(start, end, step int64, group, queue string) (ret string, err error) {
	// TODO
	return
}

type redisClient struct {
	poolSize int
	addr     string
	conns    *redis.Client
}

func newRedisClient(addr string, auth string, poolSize int) (cli *redisClient) {
	cli = &redisClient{
		poolSize: poolSize,
		addr:     addr,
		conns: redis.NewClient(&redis.Options{
			Addr:         addr,
			Password:     auth,
			ReadTimeout:  DEFAULT_RW_TIMEOUT,
			WriteTimeout: DEFAULT_RW_TIMEOUT,
		}),
	}
	return
}

func (r *redisClient) Send(key string, data []byte) (err error) {
	res := r.conns.RPush(key, string(data))
	err = res.Err()
	if err != nil {
		log.Error(err)
	}
	// Lpush to q
	return
}

func (r *redisClient) Overview(start, end, step int64, host string) (ret string, err error) {
	// TODO
	return
}

func (c *redisClient) GroupMetrics(start, end, step int64, group, queue string) (ret string, err error) {
	// TODO
	return
}

type RoamSt struct {
	Type    string  `json:"type"`
	Queue   string  `json:"queue"`
	Group   string  `json:"group"`
	Action  string  `json:"action"`
	Total   int64   `json:"total_count"`
	AvgTime float64 `json:"avg_time"`
}

type RoamClient struct {
	cli *http.Client
}

func newRoamClient() *RoamClient {
	return &RoamClient{
		cli: &http.Client{},
	}
}

func (m *RoamClient) Send(key string, data []byte) (err error) {
	sts, err := transToRoamSt(data)
	if err != nil {
		log.Warnf("store profile log err : %v", err)
		return
	}
	for i := range sts {
		log.Profile("%s", sts[i])
	}
	return
}

func (m *RoamClient) Overview(start, end, step int64, host string) (ret string, err error) {
	// TODO
	req, err := http.NewRequest("GET", "http://127.0.0.1/dashboard/metrics/wqs", nil)
	if err != nil {
	}
	m.cli.Do(req)
	return
}

func (m *RoamClient) GroupMetrics(start, end, step int64, group, queue string) (ret string, err error) {
	// TODO
	req, err := http.NewRequest("GET", "http://127.0.0.1/detail/metrics/wqs", nil)
	if err != nil {
	}
	m.cli.Do(req)
	return
}

func transToRoamSt(data []byte) (jsonStrs []string, err error) {
	var results []*MetricsStat
	err = json.Unmarshal(data, &results)
	if err != nil {
		return
	}

	action := func(st *MetricsStat) []*RoamSt {
		ret := make([]*RoamSt, 0, 2)
		actions := []string{SENT, RECV}
		for _, act := range actions {
			rst := &RoamSt{
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
				log.Warnf("transToRoamSt err : %v", err)
				continue
			}
			jsonStrs = append(jsonStrs, string(stData))
		}
	}
	return
}

func cutFloat64(src float64, bit int) (ret float64) {
	ret, err := strconv.ParseFloat(fmt.Sprintf("%.2f", src), 64)
	if err != nil {
		log.Warn(err)
		return
	}
	return
}
