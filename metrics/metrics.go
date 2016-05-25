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
	defaultChSize   = 1024 * 10
	defaultPrintTTL = time.Second * 30

	defaultReportURI = "http://127.0.0.1:10001/metrics/wqs"
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
		d:           metrics.NewRegistry(),
		in:          make(chan *Packet, defaultChSize),
		serviceName: "wqs",
		endpoint:    hn,
		printTTL:    defaultPrintTTL,
		stop:        make(chan struct{}),
		// transport:   newRedisClient("127.0.0.1:6379", "", 2),
		transport: newHTTPClient(),
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
	d           metrics.Registry
	printTTL    time.Duration
	serviceName string
	endpoint    string
	wg          *sync.WaitGroup
	stop        chan struct{}

	transport Transport
}

func (m *MetricsClient) run() {
	tk := time.NewTicker(m.printTTL)
	defer tk.Stop()

	reportTk := time.NewTicker(time.Second * 1)
	defer reportTk.Stop()
	var p *Packet

	for {
		select {
		case p = <-m.in:
			m.do(p)
		case <-tk.C:
			m.print()
		case <-reportTk.C:
			m.report()
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
	log.Info("[metrics] COUNTER: " + bf.String())
}

func (m *MetricsClient) report() {
	var bf = &bytes.Buffer{}
	shot := map[string]interface{}{
		"endpoint": m.endpoint,
		"service":  m.serviceName,
		"data":     m.d,
	}
	json.NewEncoder(bf).Encode(shot)
	m.d.UnregisterAll()
	log.Info("[metrics] QPS: " + bf.String())
	// m.transport.Send(defaultReportURI, bf.Bytes())
}

func (m *MetricsClient) incr(k string, v int64) {
	c := metrics.GetOrRegisterCounter(k, m.r)
	c.Inc(v)
	d := metrics.GetOrRegisterCounter(k, m.d)
	d.Inc(v)
}

func (m *MetricsClient) decr(k string, v int64) {
	// TODO
}

func Add(key string, val int64) {
	pkt := &Packet{INCR, key, val}
	select {
	case defaultClient.in <- pkt:
	default:
		println("metrics chan is full")
	}
}
