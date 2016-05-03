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

package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/weibocom/wqs/config"
	"github.com/weibocom/wqs/engine/queue"
	"github.com/weibocom/wqs/log"
	"github.com/weibocom/wqs/utils"

	"github.com/juju/errors"
)

type HttpServer struct {
	port         string
	uidir        string
	queueService queue.Queue
	listener     *utils.Listener
}

func NewHttpServer(queueService queue.Queue, config *config.Config) *HttpServer {
	return &HttpServer{
		port:         config.HttpPort,
		uidir:        config.UiDir,
		queueService: queueService,
	}
}

func (s *HttpServer) Start() error {

	var err error
	mux := http.NewServeMux()
	mux.HandleFunc("/queue", s.queueHandler)
	mux.HandleFunc("/group", s.groupHandler)
	mux.HandleFunc("/monitor", s.monitorHandler)
	mux.HandleFunc("/alarm", s.alarmHandler)
	mux.HandleFunc("/msg", s.msgHandler)

	if s.uidir != "" {
		// Static file serving done from /ui/
		mux.Handle("/", http.StripPrefix("/", http.FileServer(http.Dir(s.uidir))))
	}

	s.listener, err = utils.Listen("tcp", fmt.Sprintf(":%s", s.port))
	if err != nil {
		return errors.Trace(err)
	}

	server := &http.Server{Handler: mux}
	server.SetKeepAlivesEnabled(true)
	go server.Serve(s.listener)
	return nil
}

//队列操作handler
func (s *HttpServer) queueHandler(w http.ResponseWriter, r *http.Request) {

	var result string
	r.ParseForm()
	action := r.FormValue("action")
	queue := r.FormValue("queue")

	switch action {
	case "create":
		result = s.queueCreate(queue)
	case "remove":
		result = s.queueRemove(queue)
	case "update":
		result = s.queueUpdate(queue)
	case "lookup":
		biz := r.FormValue("biz")
		result = s.queueLookup(queue, biz)
	default:
		result = "error, param action=" + action + " not support!"
	}
	fmt.Fprintf(w, result)
}

func (s *HttpServer) queueCreate(queue string) string {
	err := s.queueService.Create(queue)
	if err != nil {
		log.Debugf("CreateQueue err:%s", errors.ErrorStack(err))
		return `{"action":"create","result":false}`
	}
	return `{"action":"create","result":true}`
}

func (s *HttpServer) queueRemove(queue string) string {
	err := s.queueService.Delete(queue)
	if err != nil {
		log.Debugf("DeleteQueue err:%s", errors.ErrorStack(err))
		return `{"action":"remove","result":false}`
	}
	return `{"action":"remove","result":true}`
}

func (s *HttpServer) queueUpdate(queue string) string {
	err := s.queueService.Update(queue)
	if err != nil {
		log.Debugf("UpdateQueue err:%s", errors.ErrorStack(err))
		return `{"action":"update","result":false}`
	}
	return `{"action":"update","result":true}`
}

func (s *HttpServer) queueLookup(queue string, biz string) string {
	r, err := s.queueService.Lookup(queue, biz)
	if err != nil {
		log.Debugf("LookupQueue err:%s", errors.ErrorStack(err))
		return "[]"
	}
	result, err := json.Marshal(r)
	if err != nil {
		log.Debugf("queueLookup Marshal err:%s", err)
		return "[]"
	}
	return string(result)
}

//业务操作handler
func (s *HttpServer) groupHandler(w http.ResponseWriter, r *http.Request) {

	var result string
	r.ParseForm()
	action := r.FormValue("action")
	queue := r.FormValue("queue")
	group := r.FormValue("group")
	write := r.FormValue("write")
	read := r.FormValue("read")
	url := r.FormValue("url")
	ips := r.FormValue("ips")

	switch action {
	case "add":
		result = s.groupAdd(group, queue, write, read, url, ips)
	case "remove":
		result = s.groupRemove(group, queue)
	case "update":
		result = s.groupUpdate(group, queue, write, read, url, ips)
	case "lookup":
		result = s.groupLookup(group)
	default:
		result = "error, param action=" + action + " not support!"
	}
	fmt.Fprintf(w, result)
}

func (s *HttpServer) groupAdd(group string, queue string, write string, read string, url string, ips string) string {

	w, _ := strconv.ParseBool(write)
	r, _ := strconv.ParseBool(read)
	ips_array := strings.Split(ips, ",")

	if url == "" {
		url = fmt.Sprintf("%s.%s.intra.weibo.com", group, queue)
	}

	err := s.queueService.AddGroup(group, queue, w, r, url, ips_array)
	if err != nil {
		log.Debugf("AddGroup failed: %s", errors.ErrorStack(err))
		return `{"action":"add","result":false}`
	}
	return `{"action":"add","result":true}`
}

func (s *HttpServer) groupRemove(group string, queue string) string {
	err := s.queueService.DeleteGroup(group, queue)
	if err != nil {
		log.Debugf("groupRemove failed: %s", errors.ErrorStack(err))
		return `{"action":"remove","result":false}`
	}
	return `{"action":"remove","result":true}`
}

func (s *HttpServer) groupUpdate(group string, queue string,
	write string, read string, url string, ips string) string {

	config, err := s.queueService.GetSingleGroup(group, queue)
	if err != nil {
		log.Debugf("GetSingleGroup err:%s", errors.ErrorStack(err))
		return `{"action":"update","result":false}`
	}
	if write != "" {
		w, err := strconv.ParseBool(write)
		if err == nil {
			config.Write = w
		}
	}
	if read != "" {
		r, err := strconv.ParseBool(read)
		if err == nil {
			config.Read = r
		}
	}
	if url != "" {
		config.Url = url
	}
	if ips != "" {
		config.Ips = strings.Split(ips, ",")
	}

	err = s.queueService.UpdateGroup(group, queue, config.Write, config.Read, config.Url, config.Ips)
	if err != nil {
		log.Debugf("groupUpdate failed: %s", errors.ErrorStack(err))
		return `{"action":"update","result":false}`
	}
	return `{"action":"update","result":true}`
}

func (s *HttpServer) groupLookup(group string) string {
	r, err := s.queueService.LookupGroup(group)
	if err != nil {
		log.Debugf("LookupGroup err: %s", errors.ErrorStack(err))
		return "[]"
	}
	result, err := json.Marshal(r)
	if err != nil {
		log.Debugf("LookupGroup Marshal err: %s", err)
		return "[]"
	}
	return string(result)
}

//消息操作handler
func (s *HttpServer) msgHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()

	action := r.FormValue("action")
	queue := r.FormValue("queue")
	group := r.FormValue("group")
	msg := r.FormValue("msg")

	var result string
	switch action {
	case "receive":
		result = s.msgReceive(queue, group)
	case "send":
		result = s.msgSend(queue, group, msg)
	case "ack":
		result = s.msgAck(queue, group)
	default:
		result = "error, param action=" + action + " not support!"
	}
	fmt.Fprintf(w, result)
}

func (s *HttpServer) msgSend(queue string, group string, msg string) string {
	var result string
	_, err := s.queueService.SendMsg(queue, group, []byte(msg))
	if err != nil {
		log.Debugf("msgSend failed: %s", errors.ErrorStack(err))
		result = err.Error()
	} else {
		result = `{"action":"send","result":true}`
	}
	return result
}

func (s *HttpServer) msgReceive(queue string, group string) string {
	var result string
	_, data, err := s.queueService.RecvMsg(queue, group)
	if err != nil {
		log.Debugf("msgReceive failed: %s", errors.ErrorStack(err))
		result = err.Error()
	} else {
		result = `{"action":"receive","msg":"` + string(data) + `"}`
	}
	return result
}

func (s *HttpServer) msgAck(queue string, group string) string {
	var result string
	err := s.queueService.AckMsg(queue, group)
	if err != nil {
		result = err.Error()
	} else {
		result = `{"action":"ack","result":true}`
	}
	return result
}

func (s *HttpServer) monitorHandler(w http.ResponseWriter, r *http.Request) {

	r.ParseForm()
	monitorType := r.FormValue("type")
	queue := r.FormValue("queue")
	group := r.FormValue("group")

	end := time.Now().Unix()
	start := end - 5*60 //5min
	interval := int64(1)

	startTime := r.FormValue("start")
	if startTime != "" {
		start, _ = strconv.ParseInt(startTime, 10, 0)
	}
	endTime := r.FormValue("end")
	if endTime != "" {
		end, _ = strconv.ParseInt(endTime, 10, 0)
	}
	intervalTime := r.FormValue("interval")
	if intervalTime != "" {
		interval, _ = strconv.ParseInt(intervalTime, 10, 0)
	}

	var result string

	switch monitorType {
	case "send":
		m, err := s.queueService.GetSendMetrics(queue, group, start, end, interval)
		if err != nil {
			log.Debug("GetSendMetrics err: %s", errors.ErrorStack(err))
			return
		}
		sm, err := json.Marshal(m)
		if err != nil {
			log.Debugf("GetSendMetrics Marshal err: %s", err)
			return
		}
		result = string(sm)
	case "receive":
		m, err := s.queueService.GetReceiveMetrics(queue, group, start, end, interval)
		if err != nil {
			log.Debug("GetReceiveMetrics err: %s", errors.ErrorStack(err))
			return
		}
		rm, err := json.Marshal(m)
		if err != nil {
			log.Debugf("GetReceiveMetrics Marshal err: %s", err)
			return
		}
		result = string(rm)
	default:
		result = "error, param type=" + monitorType + " not support!"
	}
	fmt.Fprintf(w, result)
}

func (this *HttpServer) alarmHandler(w http.ResponseWriter, r *http.Request) {

}
