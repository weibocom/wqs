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
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"github.com/weibocom/wqs/config"
	"github.com/weibocom/wqs/log"

	"github.com/rcrowley/go-metrics"
)

var (
	errInvalidParam       = errors.New("Invalid params")
	errMetricsClientIsNil = errors.New("MetricsClient is nil")
	errUnknownTransport   = errors.New("Unknown transport")
)

type eventType int32

const (
	eventCounter eventType = iota
	eventMeter
	eventTimer
)

const (
	ElapseLess10ms  = "Less10ms"
	ElapseLess20ms  = "Less20ms"
	ElapseLess50ms  = "Less50ms"
	ElapseLess100ms = "Less100ms"
	ElapseLess200ms = "Less200ms"
	ElapseLess500ms = "Less500ms"
	ElapseMore500ms = "More500ms"

	CmdGet  = "GET"
	CmdSet  = "SET"
	CmdAck  = "ACK"
	Qps     = "qps"
	Ops     = "ops"
	Latency = "Latency"
	ToConn  = "ToConn"
	ReConn  = "ReConn"
	Elapsed = "elapsed"

	eventBufferSize = 1024 * 10
	defaultWriter   = graphiteWriter
	defaultReader   = graphiteWriter
	defaultCenter   = "http://127.0.0.1:10001/v1/metrics"
	sinkDuration    = time.Second * 5
)

type event struct {
	event eventType
	key   string
	value int64
}

type metricsClient struct {
	eventBus    chan *event
	registry    metrics.Registry
	serviceName string
	centerAddr  string
	writers     map[string]metricsStatWriter
	reader      metricsStatReader
	stopCh      chan struct{}
	stopping    int32
}

var client *metricsClient

func Start(cfg *config.Config) (err error) {

	section, err := cfg.GetSection("metrics")
	if err != nil {
		return err
	}

	client = &metricsClient{
		registry:    metrics.NewRegistry(),
		eventBus:    make(chan *event, eventBufferSize),
		serviceName: "wqs",
		stopCh:      make(chan struct{}),
		stopping:    0,
		writers:     make(map[string]metricsStatWriter),
	}

	if err := client.initWriterAndReader(section); err != nil {
		return err
	}

	uri := section.GetStringMust("metrics.center", defaultCenter)
	uri = uri + "/" + client.serviceName
	client.centerAddr = uri

	go client.eventLoop()
	return nil
}

func Stop() {
	if client != nil {
		client.stop()
	}
}

func (m *metricsClient) stop() {
	if atomic.SwapInt32(&m.stopping, 1) == 0 {
		close(m.stopCh)
	}
}

func (m *metricsClient) initWriterAndReader(section config.Section) error {

	writers := section.GetStringMust("transport.writers", defaultWriter)
	names := strings.Split(writers, ",")
	for _, name := range names {
		w, err := getWriter(name, section)
		if err != nil {
			return err
		}
		m.writers[name] = w
	}

	var err error
	reader := section.GetStringMust("transport.reader", defaultReader)
	client.reader, err = getReader(reader, section)
	if err != nil {
		return err
	}
	return nil
}

func (m *metricsClient) eventLoop() {

	if atomic.LoadInt32(&m.stopping) == 1 {
		return
	}

	ticker := time.NewTicker(sinkDuration)
	defer ticker.Stop()

	for {
		select {
		case evt := <-m.eventBus:
			m.processEvent(evt)
		case <-ticker.C:
			m.sink()
		case <-m.stopCh:
			return
		}
	}
}

func (m *metricsClient) processEvent(evt *event) {
	switch evt.event {
	case eventCounter:
		metrics.GetOrRegisterCounter(evt.key, m.registry).Inc(evt.value)
	case eventMeter:
		getOrRegisterMeter(evt.key, m.registry).Mark(evt.value)
	case eventTimer:
		getOrRegisterTimer(evt.key, m.registry).Update(time.Duration(evt.value))
	}
}

func (m *metricsClient) snapshot() metrics.Registry {
	hasElement := false
	snap := metrics.NewRegistry()

	m.registry.Each(func(key string, i interface{}) {
		switch m := i.(type) {
		case metrics.Counter:
			snap.Register(key, m.Snapshot())
		case metrics.Gauge:
			snap.Register(key, m.Snapshot())
		case metrics.GaugeFloat64:
			snap.Register(key, m.Snapshot())
		case metrics.Histogram:
			snap.Register(key, m.Snapshot())
		case metrics.Meter:
			snap.Register(key, m.Snapshot())
		case metrics.Timer:
			snap.Register(key, m.Snapshot())
		}
		hasElement = true
	})
	if !hasElement {
		return nil
	}
	return snap
}

func (m *metricsClient) sink() {

	snap := m.snapshot()
	if snap == nil {
		log.Warn("metrics snapshot empty.")
		return
	}

	for name, writer := range m.writers {
		err := writer.Write(m.centerAddr, snap)
		if err != nil {
			log.Errorf("metrics writer %s error : %v", name, err)
		}
	}
	snap.UnregisterAll()
}

func AddCounter(key string, value int64) {
	evt := &event{event: eventCounter, key: key, value: value}
	select {
	case client.eventBus <- evt:
	default:
		log.Error("metrics eventBus is full.")
	}
}

func GetCounter(key string) int64 {
	return metrics.GetOrRegisterCounter(key, client.registry).Count()
}

func AddMeter(key string, value int64) {
	evt := &event{event: eventMeter, key: key, value: value}
	select {
	case client.eventBus <- evt:
	default:
		log.Error("metrics eventBus is full.")
	}
}

// about Rate1: http://blog.sina.com.cn/s/blog_5069fdde0100g4ua.html
func GetMeterRate(key string) float64 {
	return getOrRegisterMeter(key, client.registry).Rate1()
}

func AddTimer(key string, duration int64) {
	evt := &event{event: eventTimer, key: key, value: duration}
	select {
	case client.eventBus <- evt:
	default:
		log.Error("metrics eventBus is full.")
	}
}

func GetTimerMean(key string) float64 {
	return getOrRegisterTimer(key, client.registry).RateMean()
}

func GetMetrics(param *MetricsQueryParam) (stat string, err error) {

	if client == nil || client.reader == nil {
		return "", errMetricsClientIsNil
	}

	if param.StartTime == 0 || param.EndTime == 0 ||
		param.Step == 0 || param.Host == "" ||
		param.Queue == "" || param.Group == "" ||
		param.ActionKey == "" || param.MetricsKey == "" {
		return "", errInvalidParam
	}

	return client.reader.GroupMetrics(param)
}

func ElapseTimeString(t int64) string {
	switch {
	case t < 10:
		return ElapseLess10ms
	case t < 20:
		return ElapseLess20ms
	case t < 50:
		return ElapseLess50ms
	case t < 100:
		return ElapseLess100ms
	case t < 200:
		return ElapseLess200ms
	case t < 500:
		return ElapseLess500ms
	default:
		return ElapseMore500ms
	}
}

func getWriter(name string, section config.Section) (metricsStatWriter, error) {
	switch name {
	case graphiteWriter:
		graphiteAddr, err := section.GetString("graphite.report.addr.udp")
		if err != nil {
			return nil, err
		}
		graphiteServicePool, err := section.GetString("graphite.service.pool")
		if err != nil {
			return nil, err
		}
		graphiteRoot := section.GetStringMust("graphite.root", LOCAL)
		return newGraphite(graphiteRoot, graphiteAddr, graphiteServicePool), nil
	case profileWriter:
		return newProfileWriter(), nil
	default:
		log.Errorf("unknown metrics writer: %s", name)
	}
	return nil, errUnknownTransport
}

func getReader(name string, section config.Section) (metricsStatReader, error) {
	switch name {
	case graphiteWriter:
		graphiteAddr, err := section.GetString("graphite.report.addr.udp")
		if err != nil {
			return nil, err
		}
		graphiteServicePool, err := section.GetString("graphite.service.pool")
		if err != nil {
			return nil, err
		}
		graphiteRoot := section.GetStringMust("graphite.root", LOCAL)
		return newGraphite(graphiteRoot, graphiteAddr, graphiteServicePool), nil
	default:
		log.Errorf("unknown metrics writer: %s", name)
	}
	return nil, errUnknownTransport
}
