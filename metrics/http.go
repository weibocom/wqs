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
	"io/ioutil"
	"net/http"

	"github.com/weibocom/wqs/log"
)

type httpClient struct {
	cli *http.Client
}

func newHTTPClient() *httpClient {
	return &httpClient{
		cli: &http.Client{Timeout: defaultRWTimeout},
	}
}

func (c *httpClient) Send(uri string, results []*MetricsStat) (err error) {
	data, err := json.Marshal(results)
	if err != nil {
		log.Warnf("Encode results err: %v", err)
		return
	}
	req, err := http.NewRequest("POST", uri, bytes.NewReader(data))
	if err != nil {
		log.Warnf("new http request err: %v", err)
		return err
	}
	resp, err := c.cli.Do(req)
	if err != nil {
		log.Warnf("do http request err: %v", err)
		return err
	}
	defer resp.Body.Close()

	respData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Warnf("do http request err: %v", err)
		return err
	}
	// TODO check
	_ = respData
	return
}

func (c *httpClient) Overview(params *MetricsQueryParam) (data string, err error) {
	// TODO
	return
}

func (c *httpClient) GroupMetrics(params *MetricsQueryParam) (data string, err error) {
	// TODO
	return
}
