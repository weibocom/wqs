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
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/weibocom/wqs/config"
	"github.com/weibocom/wqs/service"
)

type HttpServer struct {
	port         string
	uidir        string
	queueService *service.QueueService
}

func NewHttpServer(queueService *service.QueueService, config config.Config) *HttpServer {
	httpServer := HttpServer{}
	httpServer.port = config.HttpPort
	httpServer.uidir = config.UiDir
	httpServer.queueService = queueService
	return &httpServer
}

func (this *HttpServer) Start() {
	http.HandleFunc("/queue", this.queueHandler)
	http.HandleFunc("/group", this.groupHandler)
	http.HandleFunc("/monitor", this.monitorHandler)
	http.HandleFunc("/alarm", this.alarmHandler)
	http.HandleFunc("/msg", this.msgHandler)
	//http.HandleFunc("/msg/ha", this.msgHandler)
	//http.HandleFunc("/msg/pipeline", this.msgHandler)

	if this.uidir != "" {
		// Static file serving done from /ui/
		http.Handle("/", http.StripPrefix("/", http.FileServer(http.Dir(this.uidir))))
	}

	err := http.ListenAndServe(":"+this.port, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

//队列操作handler
func (this *HttpServer) queueHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()

	var action string = r.FormValue("action")
	var queue string = r.FormValue("queue")
	var biz string = r.FormValue("biz")

	var result string

	switch action {
	case "create":
		result = this.queueCreate(queue)
	case "remove":
		result = this.queueRemove(queue)
	case "update":
		result = this.queueUpdate(queue)
	case "lookup":
		result = this.queueLookup(queue, biz)
	default:
		result = "error, param action=" + action + " not support!"
	}
	fmt.Fprintf(w, result)
}

func (this *HttpServer) queueCreate(queue string) string {
	r := this.queueService.CreateQueue(queue)
	result := `{"action":"create","result":` + strconv.FormatBool(r) + `}`
	return result
}

func (this *HttpServer) queueRemove(queue string) string {
	r := this.queueService.DeleteQueue(queue)
	result := `{"action":"remove","result":` + strconv.FormatBool(r) + `}`
	return result
}

func (this *HttpServer) queueUpdate(queue string) string {
	r := this.queueService.UpdateQueue(queue)
	result := `{"action":"update","result":` + strconv.FormatBool(r) + `}`
	return result
}

func (this *HttpServer) queueLookup(queue string, biz string) string {
	r := this.queueService.LookupQueue(queue, biz)
	result, _ := json.Marshal(r)
	return string(result)
}

//业务操作handler
func (this *HttpServer) groupHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()

	var action string = r.FormValue("action")
	var queue string = r.FormValue("queue")
	var group string = r.FormValue("group")
	var write string = r.FormValue("write")
	var read string = r.FormValue("read")
	var url string = r.FormValue("url")
	var ips string = r.FormValue("ips")

	var result string

	switch action {
	case "add":
		result = this.groupAdd(group, queue, write, read, url, ips)
	case "remove":
		result = this.groupRemove(group, queue)
	case "update":
		result = this.groupUpdate(group, queue, write, read, url, ips)
	case "lookup":
		result = this.groupLookup(group)
	default:
		result = "error, param action=" + action + " not support!"
	}
	fmt.Fprintf(w, result)
}

func (this *HttpServer) groupAdd(group string, queue string, write string, read string, url string, ips string) string {
	var w bool
	var r bool
	var ips_array []string

	if write != "" {
		w, _ = strconv.ParseBool(write)
	}

	if read != "" {
		r, _ = strconv.ParseBool(read)
	}

	if ips != "" {
		ips_array = strings.Split(ips, ",")
	}

	if url == "" {
		url = group + "." + queue + ".intra.weibo.com"
	}

	result := this.queueService.AddGroup(group, queue, w, r, url, ips_array)
	return `{"action":"add","result":` + strconv.FormatBool(result) + `}`
}

func (this *HttpServer) groupRemove(group string, queue string) string {
	result := this.queueService.DeleteGroup(group, queue)
	return `{"action":"remove","result":` + strconv.FormatBool(result) + `}`
}

func (this *HttpServer) groupUpdate(group string, queue string, write string, read string, url string, ips string) string {
	config := this.queueService.GetSingleGroup(group, queue)
	fmt.Println("config:", config)
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

	result := this.queueService.UpdateGroup(group, queue, config.Write, config.Read, config.Url, config.Ips)
	return `{"action":"update","result":` + strconv.FormatBool(result) + `}`
}

func (this *HttpServer) groupLookup(group string) string {
	r := this.queueService.LookupGroup(group)
	result, _ := json.Marshal(r)
	return string(result)
}

//消息操作handler
func (this *HttpServer) msgHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()

	var action string = r.FormValue("action")
	var queue string = r.FormValue("queue")
	var group string = r.FormValue("group")
	var msg string = r.FormValue("msg")

	var result string
	switch action {
	case "receive":
		result = this.msgReceive(queue, group)
	case "send":
		result = this.msgSend(queue, group, msg)
	case "ack":
		result = this.msgAck(queue, group)
	default:
		result = "error, param action=" + action + " not support!"
	}
	fmt.Fprintf(w, result)
}

func (this *HttpServer) msgSend(queue string, group string, msg string) string {
	var result string
	err := this.queueService.SendMsg(queue, group, []byte(msg))
	if err != nil {
		result = err.Error()
	} else {
		result = `{"action":"send","result":` + strconv.FormatBool(true) + `}`
	}
	return result
}

func (this *HttpServer) msgReceive(queue string, group string) string {
	var result string
	data, err := this.queueService.ReceiveMsg(queue, group)
	if err != nil {
		result = err.Error()
	} else {
		result = `{"action":"receive","msg":"` + string(data) + `"}`
	}
	return result
}

func (this *HttpServer) msgAck(queue string, group string) string {
	var result string
	err := this.queueService.AckMsg(queue, group)
	if err != nil {
		result = err.Error()
	} else {
		result = `{"action":"ack","result":` + strconv.FormatBool(true) + `}`
	}
	return result
}

func (this *HttpServer) monitorHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()

	var monitorType string = r.FormValue("type")
	var queue string = r.FormValue("queue")
	var group string = r.FormValue("group")

	end := time.Now().Unix()
	start := end - 5*60 //5min
	interval := 1

	if r.FormValue("start") != "" {
		temp, _ := strconv.Atoi(r.FormValue("start"))
		start = int64(temp)
	}
	if r.FormValue("end") != "" {
		temp, _ := strconv.Atoi(r.FormValue("end"))
		end = int64(temp)
	}
	if r.FormValue("interval") != "" {
		temp, _ := strconv.Atoi(r.FormValue("interval"))
		interval = temp
	}

	var result string

	switch monitorType {
	case "send":
		sm, _ := json.Marshal(this.queueService.GetSendMetrics(queue, group, start, end, interval))
		result = string(sm)
	case "receive":
		rm, _ := json.Marshal(this.queueService.GetReceiveMetrics(queue, group, start, end, interval))
		result = string(rm)
	default:
		result = "error, param type=" + monitorType + " not support!"
	}
	fmt.Fprintf(w, result)
}

func (this *HttpServer) alarmHandler(w http.ResponseWriter, r *http.Request) {

}
