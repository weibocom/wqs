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

package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	log "github.com/cihub/seelog"
	"github.com/juju/errors"
	"github.com/weibocom/wqs/config"
	"github.com/weibocom/wqs/engine/queue"
	"github.com/weibocom/wqs/protocol/http"
	"github.com/weibocom/wqs/protocol/mc"
)

var (
	configFile   = flag.String("config", "config.properties", "qservice's configure file")
	configSeelog = flag.String("seelog", "seelog.xml", "qservice's seelog configure")
)

func main() {

	flag.Parse()
	err := initLogger(*configSeelog)
	if err != nil {
		log.Critical(errors.ErrorStack(err))
		return
	}
	conf, err := config.NewConfigFromFile(*configFile)
	if err != nil {
		log.Critical(errors.ErrorStack(err))
		return
	}

	queue, err := queue.NewQueue(conf)
	if err != nil {
		log.Critical(errors.ErrorStack(err))
		return
	}

	httpServer := http.NewHttpServer(queue, conf)
	go httpServer.Start()
	mcServer := mc.NewMcServer(queue, conf)
	go mcServer.Start()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, os.Interrupt, os.Kill)
	log.Infof("Process start")
	<-c
	mcServer.Close()
	log.Info("Process stop")
	log.Flush()
	log.Current.Close()
}
