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

	redis "gopkg.in/redis.v3"
)

const (
	DEFAULT_RW_TIMEOUT = time.Second * 4
)

type Transport interface {
	Send(uri string, data []byte) error
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
