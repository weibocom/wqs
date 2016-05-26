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

	"github.com/weibocom/wqs/config"
	"github.com/weibocom/wqs/log"

	"github.com/rcrowley/go-metrics"
)

const (
	INCR = iota
	INCR_EX
	DECR
)

const (
	defaultChSize    = 1024 * 10
	defaultPrintTTL  = 30
	defaultReportURI = "http://127.0.0.1:10001/v1/metrics"
)

type Packet struct {
	Op   uint8
	Key  string
	Val  int64
	Cost int64
}

type MetricsClient struct {
	in       chan *Packet
	r        metrics.Registry
	d        metrics.Registry
	printTTL time.Duration

	centerAddr  string
	serviceName string
	endpoint    string
	wg          *sync.WaitGroup
	stop        chan struct{}

	transport Transport
}

var defaultClient *MetricsClient

func Init(cfg *config.Config) (err error) {
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
		stop:        make(chan struct{}),
		transport:   newHTTPClient(),
	}

	uri := cfg.GetSettingVal("metrics.center", defaultReportURI)
	uri = uri + "/" + defaultClient.serviceName
	defaultClient.centerAddr = uri

	ttl := cfg.GetSettingIntVal("metrics.print_ttl", defaultPrintTTL)
	defaultClient.printTTL = time.Second * time.Duration(ttl)

	go defaultClient.run()
	return
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
	case INCR_EX:
		m.incrEx(p.Key, p.Val, p.Cost)
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

	// TODO async ?
	m.transport.Send(m.centerAddr, bf.Bytes())
}

func (m *MetricsClient) incr(k string, v int64) {
	c := metrics.GetOrRegisterCounter(k, m.r)
	c.Inc(v)
	d := metrics.GetOrRegisterCounter(k, m.d)
	d.Inc(v)
}

func (m *MetricsClient) incrEx(k string, v, cost int64) {
	c := metrics.GetOrRegisterCounter(k, m.r)
	c.Inc(v)
	d := metrics.GetOrRegisterCounter(k, m.d)
	d.Inc(v)
	switch {
	case cost < 10:
		k = k + "-[0-10ms]"
	case cost < 50:
		k = k + "-[10-50ms]"
	case cost < 100:
		k = k + "-[50-100ms]"
	case cost < 500:
		k = k + "-[100-500ms]"
	case cost < 1000:
		k = k + "-[500ms-1s]"
	default:
		k = k + "-[1s,+]"
	}
	d = metrics.GetOrRegisterCounter(k, m.d)
	d.Inc(v)
}

func (m *MetricsClient) decr(k string, v int64) {
	// TODO
}

func Add(key string, args ...int64) {
	var pkt *Packet
	if len(args) == 1 {
		pkt = &Packet{INCR, key, args[0], 0}
	} else if len(args) == 2 {
		pkt = &Packet{INCR_EX, key, args[0], args[1]}
	}
	select {
	case defaultClient.in <- pkt:
	default:
		println("metrics chan is full")
	}
}
