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
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGetLoggerHandler(t *testing.T) {

	router := NewRouter()
	router.GET("/loggers", getLoggerHandler)
	w := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "http://example.com/loggers", nil)
	if err != nil {
		t.Fatalf("unexpect error : %v", err)
	}

	router.ServeHTTP(w, req)
	msg := &ResponseMessage{}
	err = json.NewDecoder(w.Body).Decode(msg)
	if err != nil {
		t.Fatalf("unexpect error : %v", err)
	}

	if w.Code != 200 {
		t.Errorf("response status code error: want %d, now %d", 200, msg.Code)
	}

	if msg.Code != 200 {
		t.Errorf("response msg error: want %d, now %d msg %s", 200, msg.Code, msg.Message)
	}
	t.Logf("message : %q", msg.Message)
}

func TestChangeLoggerHandlerNotFound(t *testing.T) {

	router := NewRouter()
	router.POST("/loggers/:name", changeLoggerHandler)
	w := httptest.NewRecorder()
	req, err := http.NewRequest("POST", "http://example.com/loggers/noexist", nil)
	if err != nil {
		t.Errorf("unexpect error : %v", err)
	}

	router.ServeHTTP(w, req)
	errMessage := &ResponseMessage{}
	data, err := ioutil.ReadAll(w.Body)
	if err != nil {
		t.Errorf("unexpect error : %v", err)
	}

	err = json.Unmarshal(data, errMessage)
	if err != nil {
		t.Errorf("unexpect error : %v", err)
	}

	if w.Code != 404 {
		t.Errorf("response code error, want %d, now %d", 404, w.Code)
	}

	if errMessage.Code != 404 {
		t.Errorf("response message code error, want %d, now %d", 404, errMessage.Code)
	}
}

func TestChangeLoggerHandlerFound(t *testing.T) {

	router := NewRouter()
	router.POST("/loggers/:name", changeLoggerHandler)
	router.GET("/loggers", getLoggerHandler)

	w := httptest.NewRecorder()
	req, err := http.NewRequest("POST", "http://example.com/loggers/info", strings.NewReader(`{"level":"warning"}`))
	if err != nil {
		t.Errorf("unexpect error : %v", err)
	}

	router.ServeHTTP(w, req)
	msg := &ResponseMessage{}

	err = json.NewDecoder(w.Body).Decode(msg)
	if err != nil {
		t.Fatalf("unexpect error : %v", err)
	}

	if w.Code != 200 {
		t.Fatalf("response status code error, want %d, now %d, msg %s", 200, w.Code, msg.Message)
	}

	if msg.Code != 200 {
		t.Fatalf("response message code error, want %d, now %d, msg %s", 200, msg.Code, msg.Message)
	}

	if msg.Message != "OK" {
		t.Fatalf("response message error, want %s, now %s", "OK", msg.Message)
	}

	w = httptest.NewRecorder()
	req, err = http.NewRequest("GET", "http://example.com/loggers", nil)
	if err != nil {
		t.Fatalf("unexpect error : %v", err)
	}
	router.ServeHTTP(w, req)
	msg = &ResponseMessage{}
	err = json.NewDecoder(w.Body).Decode(msg)
	if err != nil {
		t.Fatalf("unexpect error : %v", err)
	}

	if w.Code != 200 {
		t.Errorf("response status code error: want %d, now %d", 200, msg.Code)
	}

	if msg.Code != 200 {
		t.Errorf("response msg error: want %d, now %d msg %s", 200, msg.Code, msg.Message)
	}

	loggers := make(map[string]string)
	err = json.NewDecoder(strings.NewReader(msg.Message)).Decode(&loggers)
	if err != nil {
		t.Fatalf("unexpect error : %v", err)
	}

	if loggers["info"] != "warning" {
		t.Errorf("response msg error: want %s, now %s", "warning", loggers["info"])
	}
	t.Logf("set info logger to %s", loggers["info"])
}
