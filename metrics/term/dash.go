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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/weibocom/wqs/metrics"

	"github.com/gizak/termui"
)

const (
	HEIGHT    = 5
	WIDTH     = 60
	PERPAGE   = 10
	MAX_WIDTH = 150
)

type Dash struct {
	stat      map[string]map[string]*Stat
	lock      *sync.RWMutex
	s         *httpServer
	in        chan *metrics.MetricsStat
	view      *termui.List
	dashboard bool

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
	in := make(chan *metrics.MetricsStat, 1024)
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

func (d *Dash) update(ms *metrics.MetricsStat) {
	d.lock.Lock()
	if _, ok := d.stat[ms.Endpoint]; !ok {
		d.stat[ms.Endpoint] = make(map[string]*Stat)
	}
	st := &Stat{
		ts:   time.Now().Unix(),
		data: make(map[string]interface{}),
	}
	st.data["qps[sent/recv]"] = fmt.Sprintf("%v/%v", ms.Sent.Total, ms.Recv.Total)
	st.data["elapsed[sent/recv]"] = fmt.Sprintf("%v/%v", ms.Sent.Elapsed, ms.Recv.Elapsed)
	st.data["latency"] = ms.Recv.Latency
	st.data["accum"] = ms.Accum

	d.stat[ms.Endpoint][ms.Queue+"#"+ms.Group] = st
	d.lock.Unlock()
}

func (d *Dash) run() {
	go d.s.Start()
	var ms *metrics.MetricsStat
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
		f := `[{"endpoint":"localhost%02d","queue":"queue","group":"group","sent":{"total":9,"cost":280.11,"latency":0},"recv":{"total":6,"cost":161.17,"latency":10.67},"accum":3,"ts":0}]`
		for i := 0; i < 3; i++ {
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

func (d *Dash) drawView() {
	st := d.sum()
	d.view = termui.NewList()
	d.view.ItemFgColor = termui.ColorYellow
	d.view.BorderLabel = "overview"
	d.view.Height = d.height
	d.view.Width = d.width
	d.view.X = 40
	d.view.Y = 0
	d.view.Items = st.List()
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
		if d.dashboard {
			row = 1 // row=0 :view
		}
		d.lock.Lock()
		var hosts []string
		if len(d.hosts) == 0 {
			hosts = d.sortHost()
		} else {
			hosts = d.hosts
		}
		index := 0
		for _, h := range hosts {
			for s := range d.stat[h] {
				st, ok := d.stat[h][s]
				if !ok {
					continue
				}
				ls := termui.NewList()
				ls.Items = st.List()
				ls.ItemFgColor = termui.ColorYellow
				ls.BorderLabel = fmt.Sprintf("%s@%s", s, h)
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
				if index%d.page == 0 && index != 1 {
					index = 0
					row = 0
					newRow = false
				}
			}
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
		if d.dashboard {
			d.drawView()
			termui.Render(d.view)
		} else {
			for index := range lists {
				ls := lists[index].(*termui.List)
				dd := strings.Split(ls.BorderLabel, "@")
				sv, pc := dd[0], dd[1]
				d.lock.RLock()
				ls.Items = d.stat[pc][sv].List()
				d.lock.RUnlock()
				lists[index] = ls
			}
			termui.Render(lists...)
		}
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

	termui.Handle("/sys/kbd/a", func(termui.Event) {
		termui.Clear()
		d.dashboard = true
	})

	termui.Handle("/sys/kbd/s", func(termui.Event) {
		termui.Clear()
		d.dashboard = false
	})
	termui.Loop()
}

func (d *Dash) sortHost() (hosts []string) {
	for h := range d.stat {
		hosts = append(hosts, h)
	}
	sort.Strings(hosts)
	return
}

func (d *Dash) sum() *Stat {
	f := func(tmp string) (float64, float64) {
		ss := strings.Split(tmp, "/")
		if len(ss) != 2 {
			return 0.0, 0.0
		}

		s, _ := strconv.ParseFloat(ss[0], 64)
		r, _ := strconv.ParseFloat(ss[1], 64)
		return s, r

	}

	st := &Stat{
		data: make(map[string]interface{}),
		ts:   time.Now().Unix(),
	}
	first := false
	d.lock.RLock()
	{
		for h := range d.stat {
			for q := range d.stat[h] {
				var (
					tmp     string
					uIntVal int64
					fltVal  float64
				)
				if _, ok := st.data["qps[sent/recv]"]; ok {
					tmp = st.data["qps[sent/recv]"].(string)
				}
				s, r := f(tmp)
				s1, r1 := f(d.stat[h][q].data["qps[sent/recv]"].(string))
				s += s1
				r += r1
				st.data["qps[sent/recv]"] = fmt.Sprintf("%v/%v", s, r)

				tmp = ""
				if _, ok := st.data["elapsed[sent/recv]"]; ok {
					tmp = st.data["elapsed[sent/recv]"].(string)
				} else {
					first = true
				}
				s, r = f(tmp)
				s1, r1 = f(d.stat[h][q].data["elapsed[sent/recv]"].(string))
				if first {
					s = s1
					r = r1
				} else {
					s = (s + s1) / 2
					r = (s + r1) / 2
				}
				st.data["elapsed[sent/recv]"] = fmt.Sprintf("%v/%v", s, r)

				if _, ok := st.data["latency"]; ok {
					fltVal = st.data["latency"].(float64)
				}
				fltVal += d.stat[h][q].data["latency"].(float64)
				st.data["latency"] = fltVal

				if _, ok := st.data["accum"]; ok {
					uIntVal = st.data["accum"].(int64)
				}
				uIntVal += d.stat[h][q].data["accum"].(int64)
				st.data["accum"] = uIntVal
			}
		}
	}
	d.lock.RUnlock()
	return st
}
