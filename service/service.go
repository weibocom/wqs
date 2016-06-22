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
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/pprof"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/weibocom/wqs/config"
	"github.com/weibocom/wqs/engine/queue"
	"github.com/weibocom/wqs/log"
	"github.com/weibocom/wqs/metrics"
	"github.com/weibocom/wqs/service/mc"
	"github.com/weibocom/wqs/utils"

	"github.com/juju/errors"
)

type Server struct {
	config   *config.Config
	queue    queue.Queue
	mc       *mc.McServer
	listener *utils.Listener
}

func NewServer(conf *config.Config, version string) (*Server, error) {

	queue, err := queue.NewQueue(conf, version)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Server{
		config: conf,
		queue:  queue,
	}, nil
}

func (s *Server) Start() error {

	router := NewRouter()
	if s.config.UiDir != "" {
		if assets, err := filepath.Abs(s.config.UiDir); err == nil {
			router.NotFound(http.FileServer(http.Dir(assets)))
		}
	}

	router.GET("/queue", CompatibleWarp(s.queueHandler))
	router.POST("/queue", CompatibleWarp(s.queueHandler))
	router.GET("/group", CompatibleWarp(s.groupHandler))
	router.POST("/group", CompatibleWarp(s.groupHandler))
	router.GET("/monitor", CompatibleWarp(s.monitorHandler))
	router.GET("/msg", CompatibleWarp(s.msgHandler))
	router.POST("/msg", CompatibleWarp(s.msgHandler))

	//queue's api
	router.GET("/queue/:queue/:group/metrics/:action/:type", s.getMetricsHandler)
	//loggers
	router.GET("/loggers", getLoggerHandler)
	router.POST("/loggers/:name", changeLoggerHandler)
	//proxy
	router.GET("/proxies/", s.getProxiesHandler)
	router.GET("/proxies/:id/config", s.getProxyConfigByIDHandler)
	//version
	router.GET("/version", s.getVersion)
	//pprof
	router.GET("/debug/pprof/", CompatibleWarp(pprof.Index))
	router.GET("/debug/pprof/cmdline", CompatibleWarp(pprof.Cmdline))
	router.GET("/debug/pprof/profile", CompatibleWarp(pprof.Profile))
	router.GET("/debug/pprof/symbol", CompatibleWarp(pprof.Symbol))
	router.POST("/debug/pprof/symbol", CompatibleWarp(pprof.Symbol))
	router.GET("/debug/pprof/trace", CompatibleWarp(pprof.Trace))

	var err error
	s.listener, err = utils.Listen("tcp", fmt.Sprintf(":%s", s.config.HttpPort))
	if err != nil {
		return errors.Trace(err)
	}

	server := &http.Server{Handler: router}
	server.SetKeepAlivesEnabled(true)

	s.mc = mc.NewMcServer(s.queue, s.config)
	err = s.mc.Start()
	if err != nil {
		return errors.Trace(err)
	}

	go server.Serve(s.listener)
	return nil
}

func (s *Server) Stop() (err error) {
	if s.mc != nil {
		s.mc.Stop()
	}
	if s.listener != nil {
		err = s.listener.Close()
	}
	s.queue.Close()
	return
}

//队列操作handler
func (s *Server) queueHandler(w http.ResponseWriter, r *http.Request) {

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

func (s *Server) queueCreate(queue string) string {
	err := s.queue.Create(queue)
	if err != nil {
		log.Debugf("CreateQueue err:%s", errors.ErrorStack(err))
		return `{"action":"create","result":false}`
	}
	return `{"action":"create","result":true}`
}

func (s *Server) queueRemove(queue string) string {
	err := s.queue.Delete(queue)
	if err != nil {
		log.Debugf("DeleteQueue err:%s", errors.ErrorStack(err))
		return `{"action":"remove","result":false}`
	}
	return `{"action":"remove","result":true}`
}

func (s *Server) queueUpdate(queue string) string {
	err := s.queue.Update(queue)
	if err != nil {
		log.Debugf("UpdateQueue err:%s", errors.ErrorStack(err))
		return `{"action":"update","result":false}`
	}
	return `{"action":"update","result":true}`
}

func (s *Server) queueLookup(queue string, biz string) string {
	r, err := s.queue.Lookup(queue, biz)
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
func (s *Server) groupHandler(w http.ResponseWriter, r *http.Request) {

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

func (s *Server) groupAdd(group string, queue string, write string, read string, url string, ips string) string {

	w, _ := strconv.ParseBool(write)
	r, _ := strconv.ParseBool(read)
	ips_array := strings.Split(ips, ",")

	if url == "" {
		url = fmt.Sprintf("%s.%s.intra.weibo.com", group, queue)
	}

	err := s.queue.AddGroup(group, queue, w, r, url, ips_array)
	if err != nil {
		log.Debugf("AddGroup failed: %s", errors.ErrorStack(err))
		return `{"action":"add","result":false}`
	}
	return `{"action":"add","result":true}`
}

func (s *Server) groupRemove(group string, queue string) string {
	err := s.queue.DeleteGroup(group, queue)
	if err != nil {
		log.Debugf("groupRemove failed: %s", errors.ErrorStack(err))
		return `{"action":"remove","result":false}`
	}
	return `{"action":"remove","result":true}`
}

func (s *Server) groupUpdate(group string, queue string,
	write string, read string, url string, ips string) string {

	config, err := s.queue.GetSingleGroup(group, queue)
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

	err = s.queue.UpdateGroup(group, queue, config.Write, config.Read, config.Url, config.Ips)
	if err != nil {
		log.Debugf("groupUpdate failed: %s", errors.ErrorStack(err))
		return `{"action":"update","result":false}`
	}
	return `{"action":"update","result":true}`
}

func (s *Server) groupLookup(group string) string {
	r, err := s.queue.LookupGroup(group)
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
func (s *Server) msgHandler(w http.ResponseWriter, r *http.Request) {
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

func (s *Server) msgSend(queue string, group string, msg string) string {
	var result string
	_, err := s.queue.SendMessage(queue, group, []byte(msg), 0)
	if err != nil {
		log.Debugf("msgSend failed: %s", errors.ErrorStack(err))
		result = err.Error()
	} else {
		result = `{"action":"send","result":true}`
	}
	return result
}

func (s *Server) msgReceive(queue string, group string) string {
	var result string
	id, data, _, err := s.queue.RecvMessage(queue, group)
	if err != nil {
		log.Debugf("msgReceive failed: %s", errors.ErrorStack(err))
		result = err.Error()
	} else {
		err = s.queue.AckMessage(queue, group, id)
		if err != nil {
			log.Warnf("ack message queue:%q group:%q id:%q err:%s", queue, group, id, err)
			result = err.Error()
		} else {
			result = `{"action":"receive","msg":"` + string(data) + `"}`
		}
	}
	return result
}

func (s *Server) msgAck(queue string, group string) string {
	//	var result string
	//	err := s.queue.AckMessage(queue, group)
	//	if err != nil {
	//		result = err.Error()
	//	} else {
	//		result = `{"action":"ack","result":true}`
	//	}
	//	return result
	return `{"action":"ack","result":true}`
}

func (s *Server) monitorHandler(w http.ResponseWriter, r *http.Request) {

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
		m, err := s.queue.GetSendMetrics(queue, group, start, end, interval)
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
		m, err := s.queue.GetReceiveMetrics(queue, group, start, end, interval)
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

// Get all online proxies, return id and hostname
func (s *Server) getProxiesHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

	proxys, err := s.queue.GetProxys()
	if err != nil {
		response(w, 500, err.Error())
		return
	}

	buff := &bytes.Buffer{}
	err = json.NewEncoder(buff).Encode(proxys)
	if err != nil {
		response(w, 500, err.Error())
		return
	}
	response(w, 200, buff.String())
}

// Get an online proxy's config
func (s *Server) getProxyConfigByIDHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	id := ps.ByName("id")
	if id == "" {
		response(w, 400, "invalid proxy id")
		return
	}

	proxyID, err := strconv.Atoi(id)
	if err != nil {
		response(w, 400, err.Error())
		return
	}

	config, err := s.queue.GetProxyConfigByID(proxyID)
	if err != nil {
		if errors.IsNotFound(err) {
			response(w, 404, err.Error())
			return
		}
		response(w, 500, err.Error())
		return
	}
	response(w, 200, config)
}

// Get a group's metrics
// path "/queue/:queue/:group/metrics/:action/:type"
func (s *Server) getMetricsHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	var start, end, step int64
	var err error
	queue := ps.ByName("queue")
	group := ps.ByName("group")
	action := ps.ByName("action")
	typ := ps.ByName("type")

	switch action {
	case metrics.CmdSet, metrics.CmdGet:
	default:
		response(w, 400, fmt.Sprintf("not support action: %s", action))
		return
	}

	switch typ {
	case metrics.Qps, metrics.Elapsed, metrics.Latency:
	default:
		response(w, 400, fmt.Sprintf("not support type: %s", typ))
		return
	}

	if _, err = s.queue.GetSingleGroup(group, queue); err != nil {
		response(w, 404, err.Error())
		return
	}

	qStart := r.FormValue("start")
	qEnd := r.FormValue("end")
	qStep := r.FormValue("step")

	if qStart == "" {
		start = time.Now().Add(-4 * time.Hour).Unix()
	} else {
		start, err = strconv.ParseInt(qStart, 10, 64)
		if err != nil {
			response(w, 400, err.Error())
			return
		}
	}

	if qEnd == "" {
		end = time.Now().Unix()
	} else {
		end, err = strconv.ParseInt(qEnd, 10, 64)
		if err != nil {
			response(w, 400, err.Error())
			return
		}
	}

	if qStep == "" {
		step = 240
	} else {
		step, err = strconv.ParseInt(qStep, 10, 64)
		if err != nil {
			response(w, 400, err.Error())
			return
		}
	}

	queryParam := &metrics.MetricsQueryParam{
		Host:       metrics.AllHost,
		Queue:      queue,
		Group:      group,
		ActionKey:  action,
		MetricsKey: typ,
		StartTime:  start,
		EndTime:    end,
		Step:       step,
	}

	data, err := metrics.GetMetrics(queryParam)
	if err != nil {
		response(w, 500, err.Error())
		return
	}
	response(w, 200, data)
}

func getLoggerHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {

	loggers := make(map[string]string)
	msg := &ResponseMessage{}

	infoLevel := log.GetLogger(log.LogInfo).GetLevel()
	loggers["info"] = log.LogLevel2String(infoLevel)

	if log.GetLogger(log.LogDebug).GetLevel() < log.LogDebug {
		loggers["debug"] = LoggerClose
	} else {
		loggers["debug"] = LoggerOpen
	}

	if log.ProfileGetLogger().GetLevel() < log.LogInfo {
		loggers["profile"] = LoggerClose
	} else {
		loggers["profile"] = LoggerOpen
	}

	buffer := &bytes.Buffer{}
	err := json.NewEncoder(buffer).Encode(loggers)
	if err != nil {
		msg.Code = 500
		msg.Message = err.Error()
	} else {
		msg.Code = 200
		msg.Message = buffer.String()
	}
	w.WriteHeader(msg.Code)
	w.Write(msg.Bytes())
}

// Get this server version information
// path "/version"
func (s *Server) getVersion(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	response(w, 200, s.queue.GetVersion())
}

func changeLoggerHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	type ReqMessage struct {
		Level string `json:"level"`
	}

	name := ps.ByName("name")
	msg := &ResponseMessage{Code: 200, Message: "OK"}
	switch name {
	case "info", "debug", "profile":
	default:
		response(w, 404, fmt.Sprintf("not found logger %s", name))
		return
	}

	reqMessage := ReqMessage{}
	err := json.NewDecoder(r.Body).Decode(&reqMessage)
	if err != nil {
		response(w, 400, err.Error())
		return
	}

	// TODO more graceful
	switch name {
	case "info":
		switch reqMessage.Level {
		case log.LogInfoS:
			log.GetLogger(log.LogInfo).SetLogLevel(log.LogInfo)
		case log.LogWarningS:
			log.GetLogger(log.LogInfo).SetLogLevel(log.LogWarning)
		case log.LogErrorS:
			log.GetLogger(log.LogInfo).SetLogLevel(log.LogError)
		default:
			msg.Code = 400
			msg.Message = fmt.Sprintf("not support %q", reqMessage.Level)
		}
	case "debug":
		switch reqMessage.Level {
		case LoggerOpen:
			log.GetLogger(log.LogDebug).SetLogLevel(log.LogDebug)
		case LoggerClose:
			log.GetLogger(log.LogDebug).SetLogLevel(log.LogError)
		default:
			msg.Code = 400
			msg.Message = fmt.Sprintf("not support %q", reqMessage.Level)
		}
	case "profile":
		switch reqMessage.Level {
		case LoggerOpen:
			log.ProfileGetLogger().SetLogLevel(log.LogInfo)
		case LoggerClose:
			log.ProfileGetLogger().SetLogLevel(log.LogError)
		default:
			msg.Code = 400
			msg.Message = fmt.Sprintf("not support %q", reqMessage.Level)
		}
	}

	w.WriteHeader(msg.Code)
	w.Write(msg.Bytes())
}

func response(w http.ResponseWriter, code int, message string) {
	msg := &ResponseMessage{Code: code, Message: message}
	w.WriteHeader(msg.Code)
	w.Write(msg.Bytes())
}
