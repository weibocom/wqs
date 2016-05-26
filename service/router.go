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

package service

import (
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/weibocom/wqs/log"

	"github.com/julienschmidt/httprouter"
)

type Router struct {
	accessLog int32
	*httprouter.Router
}

func NewRouter() *Router {
	return &Router{
		accessLog: 1,
		Router:    httprouter.New(),
	}
}

func (r *Router) buildAccessLog(w http.ResponseWriter, req *http.Request, cost int64) {
	host := strings.SplitN(req.RemoteAddr, ":", 2)[0]
	username := "-"
	if req.URL.User != nil {
		if name := req.URL.User.Username(); name != "" {
			username = name
		}
	}

	log.Infof("%s %s %s %s cost %d", host, username, req.Method, req.RequestURI, cost)
}

func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {

	var startTime time.Time
	accessLog := atomic.LoadInt32(&r.accessLog) == 1
	if accessLog {
		startTime = time.Now()
	}

	if strings.Contains(req.Header.Get(HeaderAcceptEncoding), "gzip") {
		grp := newGzipResponseWriter(w)
		r.Router.ServeHTTP(grp, req)
		grp.Close()
	} else {
		r.Router.ServeHTTP(w, req)
	}

	if accessLog {
		cost := time.Now().Sub(startTime) / time.Millisecond
		r.buildAccessLog(w, req, int64(cost))
	}
}

func (r *Router) NotFound(handle http.Handler) {
	r.Router.NotFound = handle
}
