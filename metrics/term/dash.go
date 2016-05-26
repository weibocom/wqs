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

package term

import (
	"bytes"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gizak/termui"
)

const (
	HEIGHT    = 5
	WIDTH     = 40
	PERPAGE   = 16
	MAX_WIDTH = 200
)

type Dash struct {
	stat map[string]map[string]*Stat
	lock *sync.RWMutex
	s    *httpServer
	in   chan *MetricsStat

	gList   []termui.Bufferer
	hosts   []string
	curPage int
	port    int
	height  int
	page    int
	width   int
}

func Init(port, page, width, height int, hosts []string) *Dash {
	if page <= 0 || page > PERPAGE {
		page = PERPAGE
	}

	if width <= 0 {
		width = WIDTH
	}
	if height <= 0 {
		height = HEIGHT
	}
	in := make(chan *MetricsStat, 1024)
	s := newHTTPServer(port, in)
	return &Dash{
		stat:   make(map[string]map[string]*Stat),
		lock:   new(sync.RWMutex),
		s:      s,
		in:     in,
		height: height,
		hosts:  hosts,
		width:  width,
		page:   page,
		port:   port,
	}
}

func (d *Dash) update(ms *MetricsStat) {
	d.lock.Lock()
	if _, ok := d.stat[ms.Service]; !ok {
		d.stat[ms.Service] = make(map[string]*Stat)
	}
	st := &Stat{
		ts:   time.Now().Unix(),
		data: make(map[string]int64),
	}
	for mt := range ms.Data {
		st.data[mt] = ms.Data[mt].Cnt
	}
	d.stat[ms.Service][ms.Endpoint] = st
	d.lock.Unlock()
}

func (d *Dash) run() {
	go d.s.Start()
	var ms *MetricsStat
	for {
		select {
		case ms = <-d.in:
			d.update(ms)
		}
	}
}

func mock() {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()
	cli := &http.Client{}
	randFunc := func() [][]byte {
		var res [][]byte
		f := `{"data":{"queue#group-sent":{"count":6},"queue#group-sent-[0-10ms]":{"count":1},"queue#group-sent-[10-50ms]":{"count":1},"queue#group-sent-[100-500ms]":{"count":2},"queue#group-sent-[1s,+]":{"count":1},"queue#group-sent-[500ms-1s]":{"count":1},"queue1#default-recv":{"count":1},"queue1#default-recv-[10-50ms]":{"count":1},"queue1#default-sent":{"count":1},"queue1#default-sent-[10-50ms]":{"count":1},"queue2#default-recv":{"count":1},"queue2#default-recv-[10-50ms]":{"count":1},"queue2#default-sent":{"count":1},"queue2#default-sent-[10-50ms]":{"count":1},"queue3#default-recv":{"count":1},"queue3#default-recv-[10-50ms]":{"count":1},"queue3#default-sent":{"count":1},"queue3#default-sent-[10-50ms]":{"count":1},"queue4#default-recv":{"count":1},"queue4#default-recv-[10-50ms]":{"count":1}},"endpoint":"bj-m-%02d.local","service":"wqs"}`
		for i := 0; i < 30; i++ {
			res = append(res, []byte(fmt.Sprintf(f, i)))
		}
		return res
	}
	data := randFunc()
	for range tk.C {
		for index := range data {
			req, _ := http.NewRequest("POST", "http://127.0.0.1:10001/v1/metrics/wqs", bytes.NewReader(data[index]))
			cli.Do(req)
		}
	}
}

func (d *Dash) Start() {
	go d.run()
	go mock()

	PL := MAX_WIDTH / d.width

	err := termui.Init()
	if err != nil {
		panic(err)
	}
	defer termui.Close()

	var lists []termui.Bufferer
	draw := func() {
		newRow := false
		row := 0
		d.lock.Lock()
		for k := range d.stat {
			index := 0
			var hosts []string
			if len(d.hosts) == 0 {
				hosts = d.sortHost(k)
			} else {
				hosts = d.hosts
			}
			for _, h := range hosts {
				st, ok := d.stat[k][h]
				if !ok {
					continue
				}
				ls := termui.NewList()
				ls.Items = st.List()
				ls.ItemFgColor = termui.ColorYellow
				ls.BorderLabel = fmt.Sprintf("%s@%s", k, h)
				ls.Height = d.height
				ls.Width = d.width
				ls.X = (index % PL) * d.width
				if index >= PL && index%PL == 0 {
					newRow = true
				}
				if newRow {
					row++
				}
				ls.Y = d.height * row
				newRow = false
				d.gList = append(d.gList, ls)
				index++
				if index%d.page == 0 {
					index = 0
					row = 0
					newRow = false
				}
			}
			newRow = true
		}
		d.lock.Unlock()

		if len(d.gList) > d.page {
			lists = d.gList[:d.page]
		} else {
			lists = d.gList[:]
		}
	}
	termui.Render(lists...)
	termui.Handle("/timer/1s", func(e termui.Event) {
		if len(lists) == 0 {
			draw()
		}
		for index := range lists {
			ls := lists[index].(*termui.List)
			dd := strings.Split(ls.BorderLabel, "@")
			sv, pc := dd[0], dd[1]
			d.lock.RLock()
			ls.Items = d.stat[sv][pc].List()
			d.lock.RUnlock()
			lists[index] = ls
		}
		termui.Render(lists...)
	})
	termui.Handle("/sys/kbd/q", func(termui.Event) {
		termui.StopLoop()
	})

	termui.Handle("/sys/kbd/p", func(termui.Event) {
		termui.Clear()
		start := (d.curPage - 1) * d.page
		if start < 0 {
			start = 0
		} else {
			d.curPage--
		}
		lists = d.gList[start : start+d.page]
		d.lock.Lock()
		for index := range lists {
			ls := lists[index].(*termui.List)
			dd := strings.Split(ls.BorderLabel, "@")
			sv, pc := dd[0], dd[1]
			ls.Items = d.stat[sv][pc].List()
			lists[index] = ls
		}
		d.lock.Unlock()
		termui.Render(lists...)
	})
	termui.Handle("/sys/kbd/n", func(termui.Event) {
		termui.Clear()
		end := (d.curPage + 1) * d.page
		if end > len(d.gList) {
			end = len(d.gList) - 1
		} else {
			d.curPage++
		}
		lists = d.gList[end-d.page : end]
		d.lock.Lock()
		for index := range lists {
			ls := lists[index].(*termui.List)
			dd := strings.Split(ls.BorderLabel, "@")
			sv, pc := dd[0], dd[1]
			ls.Items = d.stat[sv][pc].List()
			lists[index] = ls
		}
		d.lock.Unlock()
		termui.Render(lists...)
	})
	termui.Loop()
}

func (d *Dash) sortHost(service string) (hosts []string) {
	if _, ok := d.stat[service]; !ok {
		return
	}
	for h := range d.stat[service] {
		hosts = append(hosts, h)
	}
	sort.Strings(hosts)
	return
}
