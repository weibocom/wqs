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
	"io/ioutil"
	"net/http"
	"time"

	"github.com/weibocom/wqs/log"
)

const (
	DEFAULT_RW_TIMEOUT = time.Second * 1
)

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

func (c *httpClient) Overview(start, end, step int64, host string) (ret string, err error) {
	// TODO
	return
}

func (c *httpClient) GroupMetrics(start, end, step int64, group, queue string) (ret string, err error) {
	// TODO
	return
}
