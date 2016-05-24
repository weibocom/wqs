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
	"os"
	"sync"
	"time"

	"github.com/weibocom/wqs/log"

	"github.com/rcrowley/go-metrics"
)

const (
	INCR = iota
	DECR
)

const (
	defaultChSize   = 1024 * 4
	defaultPrintTTL = time.Second * 30
)

var defaultClient *MetricsClient

func Init() (err error) {
	// TODO init with config
	hn, err := os.Hostname()
	if err != nil {
		hn = "unknown"
	}
	defaultClient = &MetricsClient{
		r:           metrics.NewRegistry(),
		in:          make(chan *Packet, defaultChSize),
		serviceName: "wqs",
		endpoint:    hn,
		printTTL:    defaultPrintTTL,
		stop:        make(chan struct{}),
	}
	go defaultClient.run()
	return
}

type Packet struct {
	Op  uint8
	Key string
	Val int64
}

type MetricsClient struct {
	in          chan *Packet
	r           metrics.Registry
	printTTL    time.Duration
	serviceName string
	endpoint    string
	wg          *sync.WaitGroup
	stop        chan struct{}

	// TODO Report interface
	// TODO QPS stat
	// send stat_info to remote server
	// count_err instead of writing
}

func (m *MetricsClient) run() {
	tk := time.NewTicker(m.printTTL)
	defer tk.Stop()
	var p *Packet

	for {
		select {
		case p = <-m.in:
			m.do(p)
		case <-tk.C:
			m.print()
		case <-m.stop:
			return
		}
	}
}

func (m *MetricsClient) do(p *Packet) {
	switch p.Op {
	case INCR:
		m.incr(p.Key, p.Val)
	case DECR:
		m.decr(p.Key, p.Val)
	}
}

func (m *MetricsClient) print() {
	var bf = &bytes.Buffer{}
	shot := map[string]interface{}{
		"endpoint": m.endpoint,
		"service":  m.serviceName,
		"data":     m.r,
	}
	json.NewEncoder(bf).Encode(shot)
	log.Info("[metrics] " + bf.String())

	// TODO print qps
}

func (m *MetricsClient) incr(k string, v int64) {
	c := metrics.GetOrRegisterCounter(k, m.r)
	c.Inc(v)
}

func (m *MetricsClient) decr(k string, v int64) {
	// TODO
}

func Add(key string, val int64) {
	defaultClient.in <- &Packet{INCR, key, val}
}
