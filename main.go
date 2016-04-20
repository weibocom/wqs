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
	"os"
	"os/signal"
	"syscall"

	log "github.com/Sirupsen/logrus"
	"github.com/weibocom/wqs/config"
	"github.com/weibocom/wqs/protocol/http"
	"github.com/weibocom/wqs/protocol/mc"
	"github.com/weibocom/wqs/service"
)

func main() {
	conf := config.LoadConfig()
	queueService := service.NewQueueService(conf)
	httpServer := http.NewHttpServer(queueService, conf)
	go httpServer.Start()
	mcServer := mc.NewMcServer(queueService, conf)
	go mcServer.Start()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, os.Interrupt, os.Kill)
	log.Info("Process start")
	<-c
	log.Info("Process stop")
}
