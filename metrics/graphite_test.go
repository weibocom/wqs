package metrics

import (
	"bufio"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestGraphiteSend(t *testing.T) {
	var cnt uint64
	var udpServe = func(stop chan struct{}) error {
		laddr, err := net.ResolveUDPAddr("udp", ":10086")
		if err != nil {
			return err
		}
		l, err := net.ListenUDP("udp", laddr)
		if err != nil {
			return err
		}

		reader := bufio.NewReader(l)
		for {
			select {
			case <-stop:
				return l.Close()
			default:
			}
			line, err := reader.ReadSlice('\n')
			if err != nil {
				println("ERROR")
				continue
			}
			print(len(line), "bytes:", string(line))
			atomic.AddUint64(&cnt, 1)
		}
	}

	var local = "localhost"
	var metricsStructs = []*MetricsStat{
		&MetricsStat{
			Endpoint: local,
			Queue:    "q1",
			Group:    "g1",
			Sent: &MetricsStruct{
				Total:   1,
				Elapsed: 9.01,
				Scale:   map[string]int64{"less_10ms": 1},
			},
			Recv: &MetricsStruct{
				Total:   1,
				Elapsed: 11.02,
				Latency: 100.92,
				Scale:   map[string]int64{"less_20ms": 1},
			},
			Accum: 0,
		},
	}

	stop := make(chan struct{})
	go udpServe(stop)
	cli := newGraphiteClient("localhost", "127.0.0.1:10086", "wqs")
	cli.Send("http://127.0.0.1:10086/upload", metricsStructs)
	time.Sleep(time.Second * 2)
	close(stop)
	if atomic.LoadUint64(&cnt) != 7 {
		t.FailNow()
	}
}
