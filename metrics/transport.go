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
	"fmt"
	"strconv"

	"github.com/weibocom/wqs/log"

	redis "gopkg.in/redis.v3"
)

const (
	ALL_MASK = "*"
)

type Transport interface {
	Send(uri string, data []byte) error
	Overview(start, end, step int64, host string) (ret string, err error)
	GroupMetrics(start, end, step int64, group, queue string) (ret string, err error)
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

func cutFloat64(src float64, bit int) (ret float64) {
	ret, err := strconv.ParseFloat(fmt.Sprintf("%.2f", src), 64)
	if err != nil {
		log.Warn(err)
		return 0.0
	}
	return
}
