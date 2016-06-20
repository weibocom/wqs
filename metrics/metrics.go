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
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/weibocom/wqs/config"
	"github.com/weibocom/wqs/log"

	"github.com/rcrowley/go-metrics"
)

var (
	errInvalidParam       = fmt.Errorf("Invalid params")
	errMetricsClientIsNil = fmt.Errorf("MetricsClient is nil")
	errUnknownTransport   = fmt.Errorf("Unknown transport")
)

const (
	incrCmd = iota
	incrExCmd
	incrEx2Cmd
	decrCmd
)

var opMap = map[uint8]string{
	incrCmd:    "incr",
	incrExCmd:  "incr_ex",
	incrEx2Cmd: "incr_ex2",
	decrCmd:    "decr",
}

const (
	defaultChSize    = 1024 * 10
	defaultPrintTTL  = 30
	defaultReportURI = "http://127.0.0.1:10001/v1/metrics"

	metricsGraphiteType = "graphite"
	metricsHTTPType     = "http"
)

const (
	KeyQps     = "qps"
	KeyElapsed = "elapsed"
	KeyLatency = "ltc"
	KeySent    = "sent"
	KeyRecv    = "recv"
)

type packet struct {
	Op      uint8
	Key     string
	Val     int64
	Elapsed int64
	Latency int64
}

func (p *packet) String() string {
	bf := &bytes.Buffer{}
	bf.WriteString("packet: ")
	if _, ok := opMap[p.Op]; !ok {
		bf.WriteString("unknown")
	} else {
		bf.WriteString(opMap[p.Op])
	}
	bf.WriteString("/" + p.Key)
	bf.WriteString("/" + fmt.Sprint(p.Val))
	bf.WriteString("/" + fmt.Sprint(p.Elapsed))
	bf.WriteString("/" + fmt.Sprint(p.Latency))
	return bf.String()
}

type client struct {
	in       chan *packet
	r        metrics.Registry
	d        metrics.Registry
	printTTL time.Duration

	centerAddr  string
	serviceName string
	endpoint    string
	writers     map[string]metricsStatWriter
	reader      metricsStatReader

	wg       *sync.WaitGroup
	stop     chan struct{}
	stopFlag uint32
}

var defaultClient *client

func Init(cfg *config.Config) (err error) {
	hn, err := os.Hostname()
	if err != nil {
		hn = "unknown"
	}

	sec, err := cfg.GetSection("metrics")
	if err != nil {
		return err
	}

	defaultClient = &client{
		r:           metrics.NewRegistry(),
		d:           metrics.NewRegistry(),
		in:          make(chan *packet, defaultChSize),
		serviceName: "wqs",
		endpoint:    hn,
		stop:        make(chan struct{}),
		stopFlag:    0,
		writers:     make(map[string]metricsStatWriter),
	}

	if err := defaultClient.installTransport(sec); err != nil {
		return err
	}

	uri := sec.GetStringMust("metrics.center", defaultReportURI)
	uri = uri + "/" + defaultClient.serviceName
	defaultClient.centerAddr = uri

	ttl := sec.GetInt64Must("metrics.print_ttl", defaultPrintTTL)
	defaultClient.printTTL = time.Second * time.Duration(ttl)

	go defaultClient.run()
	return
}

func (m *client) installTransport(sec config.Section) error {
	if m.writers == nil {
		m.writers = make(map[string]metricsStatWriter)
	}
	modStr := sec.GetStringMust("transport.writers", metricsGraphiteType)
	mods := strings.Split(modStr, ",")
	for _, mod := range mods {
		wr, err := defaultClient.factoryTransport(mod, sec)
		if err != nil {
			return err
		}
		m.writers[mod] = wr.(metricsStatWriter)
	}

	modStr = sec.GetStringMust("transport.reader", metricsGraphiteType)
	reader, err := defaultClient.factoryTransport(modStr, sec)
	if err != nil {
		return err
	}
	defaultClient.reader = reader.(metricsStatReader)
	return nil
}

func (m *client) factoryTransport(mod string, sec config.Section) (interface{}, error) {
	switch mod {
	case metricsHTTPType:
		return newHTTPClient(), nil
	case metricsGraphiteType:
		graphiteAddr, err := sec.GetString("graphite.report.addr.udp")
		if err != nil {
			return nil, err
		}
		graphiteServicePool, err := sec.GetString("graphite.service.pool")
		if err != nil {
			return nil, err
		}
		graphiteRoot := sec.GetStringMust("graphite.root", LOCAL)
		return newGraphiteClient(graphiteRoot, graphiteAddr, graphiteServicePool), nil
	default:
		log.Warnf("unknown transport mod: %s", mod)
	}
	return nil, errUnknownTransport
}

func (m *client) run() {
	if atomic.LoadUint32(&m.stopFlag) == 1 {
		return
	}
	tk := time.NewTicker(m.printTTL)
	defer tk.Stop()

	reportTk := time.NewTicker(time.Second * 1)
	defer reportTk.Stop()
	var p *packet
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

func (m *client) do(p *packet) {
	switch p.Op {
	case incrCmd:
		m.incr(p.Key, p.Val)
	case incrExCmd:
		m.incrEx(p.Key, p.Val, p.Elapsed)
	case incrEx2Cmd:
		m.incrEx2(p.Key, p.Val, p.Elapsed, p.Latency)
	case decrCmd:
		m.decr(p.Key, p.Val)
	}
}

func (m *client) print() {
	var bf = &bytes.Buffer{}
	shot := map[string]interface{}{
		"endpoint": m.endpoint,
		"service":  m.serviceName,
		"data":     m.r,
	}
	json.NewEncoder(bf).Encode(shot)
	log.Info("[metrics] " + bf.String())
}

func (m *client) report() {
	var bf = &bytes.Buffer{}
	shot := map[string]interface{}{
		"endpoint": m.endpoint,
		"service":  m.serviceName,
		"data":     m.d,
	}
	json.NewEncoder(bf).Encode(shot)
	log.Info("[metrics] QPS " + bf.String())

	snapshot := snapshotMetricsStats(m.d)
	m.d.Each(func(k string, _ interface{}) {
		c := metrics.GetOrRegisterCounter(k, m.d)
		c.Clear()
	})

	// TODO
	for mod := range m.writers {
		err := m.writers[mod].Send(m.centerAddr, snapshot)
		if err != nil {
			log.Errorf("metrics writers send error: %v", err)
		}
	}
}

func (m *client) incr(k string, v int64) {
	d := metrics.GetOrRegisterCounter(k, m.d)
	d.Inc(v)
}

func (m *client) incrEx(k string, v, elapsed int64) {
	d := metrics.GetOrRegisterCounter(k+"#"+KeyQps, m.d)
	d.Inc(v)
	d = metrics.GetOrRegisterCounter(k+"#"+KeyElapsed, m.d)
	d.Inc(elapsed)
	d = metrics.GetOrRegisterCounter(k+"#"+scaleTime(elapsed), m.d)
	d.Inc(v)
}

func (m *client) incrEx2(k string, v, elapsed, latency int64) {
	d := metrics.GetOrRegisterCounter(k+"#"+KeyQps, m.d)
	d.Inc(v)
	d = metrics.GetOrRegisterCounter(k+"#"+KeyElapsed, m.d)
	d.Inc(elapsed)
	d = metrics.GetOrRegisterCounter(k+"#"+scaleTime(elapsed), m.d)
	d.Inc(v)
	d = metrics.GetOrRegisterCounter(k+"#"+KeyLatency, m.d)
	d.Inc(latency)
}

func (m *client) decr(k string, v int64) {
	// TODO
}

func (m *client) Close() {
	if atomic.SwapUint32(&m.stopFlag, 1) == 0 {
		close(m.stop)
	}
}

func Add(key string, args ...int64) {
	if defaultClient == nil {
		return
	}
	var pkt *packet
	if len(args) == 1 {
		pkt = &packet{incrCmd, key, args[0], 0, 0}
	} else if len(args) == 2 {
		pkt = &packet{incrExCmd, key, args[0], args[1], 0}
	} else if len(args) == 3 {
		pkt = &packet{incrEx2Cmd, key, args[0], args[1], args[2]}
	}

	select {
	case defaultClient.in <- pkt:
	default:
		log.Warnf("metrics chan is full: %s", pkt)
	}
}

func GetMetrics(param *MetricsQueryParam) (stat string, err error) {

	if defaultClient == nil || defaultClient.reader == nil {
		return "", errMetricsClientIsNil
	}

	if param.StartTime == 0 || param.EndTime == 0 ||
		param.Step == 0 || param.Host == "" ||
		param.Queue == "" || param.Group == "" ||
		param.ActionKey == "" || param.MetricsKey == "" {
		return "", errInvalidParam
	}

	return defaultClient.reader.GroupMetrics(param)
}

func scaleTime(elapsed int64) string {
	switch {
	case elapsed < 10:
		return "less_10ms"
	case elapsed < 50:
		return "less_50ms"
	case elapsed < 100:
		return "less_100ms"
	case elapsed < 500:
		return "less_500ms"
	case elapsed < 1000:
		return "less_1s"
	case elapsed < 5000:
		return "less_5s"
	default:
		return "more_5s"
	}
}
