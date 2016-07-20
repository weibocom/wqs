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
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/juju/errors"
	"github.com/weibocom/wqs/config"
	"github.com/weibocom/wqs/log"
	"github.com/weibocom/wqs/metrics"
	"github.com/weibocom/wqs/service"
)

var (
	configFile  = flag.String("config", "config.properties", "qservice's configure file")
	flagVersion = flag.Bool("version", false, "Show version information")
	version     = "unknown"
)

func initLogger(conf *config.Config) error {
	loggerInfo, err := log.NewLogger(conf.LogInfo).Open()
	if err != nil {
		return errors.Trace(err)
	}

	loggerDebug, err := log.NewLogger(conf.LogDebug).Open()
	if err != nil {
		loggerInfo.Close()
		return errors.Trace(err)
	}

	loggerProfile, err := log.NewLogger(conf.LogProfile).Open()
	if err != nil {
		loggerInfo.Close()
		loggerDebug.Close()
		return errors.Trace(err)
	}

	loggerInfo.SetFlags(log.LstdFlags | log.Llevel)
	loggerInfo.SetLogLevel(log.LogInfo)
	loggerInfo.SetRolling(log.RollingByDay)
	log.RestLogger(loggerInfo, log.LogFatal, log.LogError, log.LogWarning, log.LogInfo)

	loggerDebug.SetFlags(log.LstdFlags)
	loggerDebug.SetLogLevel(log.LogDebug)
	loggerDebug.SetRolling(log.RollingByHour)
	log.RestLogger(loggerDebug, log.LogDebug)

	loggerProfile.SetRolling(log.RollingByHour)
	log.RestProfileLogger(loggerProfile)

	log.NewCleaner(conf.LogExpire, conf.LogInfo, conf.LogDebug, conf.LogProfile).Start()
	return nil
}

func main() {

	flag.Parse()

	if *flagVersion {
		fmt.Printf("version : %s\n", version)
		return
	}

	waitExist := make(chan os.Signal, 1)
	signal.Notify(waitExist, syscall.SIGTERM, os.Interrupt, os.Kill, syscall.SIGSTOP)

	conf, err := config.NewConfigFromFile(*configFile)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	if err = initLogger(conf); err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	// 由于下面的service Start时，加载之前持久化的统计数据，因此要在它之前初始化好metrics模块
	if err := metrics.Start(conf); err != nil {
		log.Fatalf("init metrics err: %v", err)
	}

	server, err := service.NewServer(conf, version)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	if err = server.Start(); err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

	log.Info("<======= process start =======>")
	log.Infof("<======= receive signal %s to exist... =======>", <-waitExist)

	server.Stop()
	metrics.Stop()
	log.Info("<======= process stop =======>")
}
