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

//主要提供配置加载功能，目前是从配置文件中进行配置加载
package config

import (
	"time"

	"github.com/weibocom/wqs/config/ext"

	"github.com/juju/errors"
)

type Config struct {
	*ext.Config
	KafkaZKAddr       string
	KafkaZKRoot       string
	KafkaPartitions   int
	KafkaReplications int

	ProxyId            int
	UiDir              string
	HttpPort           string
	McPort             string
	McSocketRecvBuffer int
	McSocketSendBuffer int
	MotanPort          string
	MetaDataZKAddr     string
	MetaDataZKRoot     string
	RedisAddr          string
	LogInfo            string
	LogDebug           string
	LogProfile         string
	LogExpire          string
}

func NewConfigFromFile(file string) (*Config, error) {
	cfg, err := ext.NewConfig(file, time.Second*0)
	if err != nil {
		return nil, err
	}

	// kafka cluster config
	kafkaSec, err := cfg.GetSection("kafka")
	if err != nil {
		return nil, err
	}
	kafkaZKAddr, err := kafkaSec.GetString("zookeeper.connect")
	if err != nil {
		return nil, errors.NotFoundf("kafka.zookeeper.connect")
	}
	kafkaZKRoot, err := kafkaSec.GetString("zookeeper.root")
	if err != nil {
		return nil, errors.NotFoundf("kafka.zookeeper.root")
	}

	kafkaPartitions := int(kafkaSec.GetInt64Must("topic.partitions", 0))
	if kafkaPartitions == 0 {
		return nil, errors.NotValidf("kafka.topic.partitions")
	}
	kafkaReplications := int(kafkaSec.GetInt64Must("topic.replications", 0))
	if kafkaReplications == 0 {
		return nil, errors.NotValidf("kafka.topic.replications")
	}

	// proxy config
	proxySec, err := cfg.GetSection("proxy")
	if err != nil {
		return nil, err
	}
	proxyId := int(proxySec.GetInt64Must("id", -1))
	if proxyId == -1 {
		return nil, errors.NotValidf("proxy.id")
	}

	uiSec, err := cfg.GetSection("ui")
	if err != nil {
		return nil, err
	}
	uiDir, err := uiSec.GetString("dir")
	if err != nil {
		return nil, errors.NotFoundf("ui.dir")
	}

	pSec, err := cfg.GetSection("protocol")
	if err != nil {
		return nil, err
	}

	httpPort, err := pSec.GetString("http.port")
	if err != nil {
		return nil, errors.NotFoundf("protocol.http.port")
	}

	mcPort, err := pSec.GetString("mc.port")
	if err != nil {
		return nil, errors.NotFoundf("protocol.mc.port")
	}
	mcSocketRecvBuffer := int(pSec.GetInt64Must("mc.socket.buffer.recv", 4096))
	mcSocketSendBuffer := int(pSec.GetInt64Must("mc.socket.buffer.send", 4096))

	motanPort, err := pSec.GetString("motan.port")
	if err != nil {
		return nil, errors.NotFoundf("protocol.motan.port")
	}

	metaSec, err := cfg.GetSection("metadata")
	if err != nil {
		return nil, errors.NotFoundf("metadata sec")
	}

	metaDataZKAddr, err := metaSec.GetString("zookeeper.connect")
	if err != nil {
		return nil, errors.NotFoundf("metadata.zookeeper.connect")
	}
	metaDataZKRoot, err := metaSec.GetString("zookeeper.root")
	if err != nil {
		return nil, errors.NotFoundf("metadata.zookeeper.root")
	}

	sec, err := cfg.GetSection("redis")
	if err != nil {
		return nil, errors.NotFoundf("redis sec")
	}
	redisAddr, err := sec.GetString("connect")
	if err != nil {
		return nil, errors.NotFoundf("redis")
	}

	logSec, err := cfg.GetSection("log")
	if err != nil {
		return nil, errors.NotFoundf("log sec")
	}
	logInfo, err := logSec.GetString("info")
	if err != nil {
		return nil, errors.NotFoundf("log.info")
	}
	logDebug, err := logSec.GetString("debug")
	if err != nil {
		return nil, errors.NotFoundf("log.debug")
	}
	logProfile, err := logSec.GetString("profile")
	if err != nil {
		return nil, errors.NotFoundf("log.profile")
	}
	logExpire, err := logSec.GetString("expire")
	if err != nil {
		logExpire = "72h"
	}

	return &Config{
		KafkaZKAddr:        kafkaZKAddr,
		KafkaZKRoot:        kafkaZKRoot,
		KafkaPartitions:    kafkaPartitions,
		KafkaReplications:  kafkaReplications,
		ProxyId:            proxyId,
		UiDir:              uiDir,
		HttpPort:           httpPort,
		McPort:             mcPort,
		McSocketRecvBuffer: mcSocketRecvBuffer,
		McSocketSendBuffer: mcSocketSendBuffer,
		MotanPort:          motanPort,
		MetaDataZKAddr:     metaDataZKAddr,
		MetaDataZKRoot:     metaDataZKRoot,
		RedisAddr:          redisAddr,
		LogInfo:            logInfo,
		LogDebug:           logDebug,
		LogProfile:         logProfile,
		LogExpire:          logExpire,
		Config:             cfg,
	}, nil
}
