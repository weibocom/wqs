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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/weibocom/wqs/metrics"

	"github.com/julienschmidt/httprouter"
)

type httpServer struct {
	serveAddr string

	out chan *metrics.MetricsStat
	r   *httprouter.Router
}

func newHTTPServer(port int, out chan *metrics.MetricsStat) *httpServer {
	router := httprouter.New()
	s := &httpServer{
		serveAddr: fmt.Sprintf(":%d", port),
		r:         router,
		out:       out,
	}
	router.POST("/v1/metrics/:service", s.do)
	return s
}

func (s *httpServer) Start() {
	go s.run()
	err := http.ListenAndServe(s.serveAddr, s.r)
	if err != nil {
		panic(err)
	}
}

func (s *httpServer) do(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	// TODO limit policy
	service := params.ByName("service")
	_ = service
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		println(err.Error())
		return
	}
	ms := make([]*metrics.MetricsStat, 0)
	err = json.Unmarshal(data, &ms)
	if err != nil {
		return
	}

	for _, m := range ms {
		select {
		case s.out <- m:
		default:
			println("recv chan is full")
		}
	}
}

func (s *httpServer) run() {
	/*
		var ms *MetricsStat
		for {
			select {
			case ms = <-s.out:
				println("xxx")
				_ = ms
			}
		}
	*/
}
