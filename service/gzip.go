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
	"compress/gzip"
	"net/http"
)

const (
	HeaderAcceptEncoding  = "Accept-Encoding"
	HeaderContentEncoding = "Content-Encoding"
	HeaderContentLength   = "Content-Length"
	HeaderVary            = "Vary"
)

type gzipResponseWriter struct {
	w  http.ResponseWriter
	gw *gzip.Writer
}

func newGzipResponseWriter(w http.ResponseWriter) *gzipResponseWriter {
	w.Header().Set(HeaderContentEncoding, "gzip")
	w.Header().Set(HeaderVary, HeaderAcceptEncoding)
	w.Header().Del(HeaderContentLength)
	return &gzipResponseWriter{
		w:  w,
		gw: gzip.NewWriter(w),
	}
}

func (g *gzipResponseWriter) Header() http.Header {
	return g.w.Header()
}

func (g *gzipResponseWriter) Write(p []byte) (int, error) {
	return g.gw.Write(p)
}

func (g *gzipResponseWriter) WriteHeader(code int) {
	g.w.WriteHeader(code)
}

func (g *gzipResponseWriter) Flush() {
	g.gw.Flush()
}

func (g *gzipResponseWriter) Close() {
	g.gw.Close()
}
