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
	r         *httprouter.Router
}

func NewRouter() *Router {
	return &Router{
		accessLog: 1,
		r:         httprouter.New(),
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
		r.r.ServeHTTP(grp, req)
		grp.Close()
	} else {
		r.r.ServeHTTP(w, req)
	}

	if accessLog {
		cost := time.Now().Sub(startTime) / time.Millisecond
		r.buildAccessLog(w, req, int64(cost))
	}
}

//For assets
func (r *Router) ServeFiles(path string, root http.FileSystem) {
	r.r.ServeFiles(path, root)
}

// GET is a shortcut for router.Handle("GET", path, handle)
func (r *Router) GET(path string, handle httprouter.Handle) {
	r.r.Handle("GET", path, handle)
}

// HEAD is a shortcut for router.Handle("HEAD", path, handle)
func (r *Router) HEAD(path string, handle httprouter.Handle) {
	r.r.Handle("HEAD", path, handle)
}

// OPTIONS is a shortcut for router.Handle("OPTIONS", path, handle)
func (r *Router) OPTIONS(path string, handle httprouter.Handle) {
	r.r.Handle("OPTIONS", path, handle)
}

// POST is a shortcut for router.Handle("POST", path, handle)
func (r *Router) POST(path string, handle httprouter.Handle) {
	r.r.Handle("POST", path, handle)
}

// PUT is a shortcut for router.Handle("PUT", path, handle)
func (r *Router) PUT(path string, handle httprouter.Handle) {
	r.r.Handle("PUT", path, handle)
}

// PATCH is a shortcut for router.Handle("PATCH", path, handle)
func (r *Router) PATCH(path string, handle httprouter.Handle) {
	r.r.Handle("PATCH", path, handle)
}

func (r *Router) NotFound(handle http.Handler) {
	r.r.NotFound = handle
}
