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
	"github.com/juju/errors"
	"github.com/magiconair/properties"
)

type Config struct {
	KafkaZKAddr       string
	KafkaZKRoot       string
	KafkaBrokerAddr   string
	KafkaPartitions   int
	KafkaReplications int

	ProxyId            int
	UiDir              string
	HttpPort           string
	McPort             string
	McSocketRecvBuffer int
	McSocketSendBuffer int
	MotanPort          string
	KafkaLib           string
	MetaDataZKAddr     string
	MetaDataZKRoot     string
	RedisAddr          string
}

func NewConfigFromFile(file string) (*Config, error) {

	p, err := properties.LoadFile(file, properties.UTF8)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// kafka cluster config
	kafkaZKAddr, exist := p.Get("kafka.zookeeper.connect")
	if !exist {
		return nil, errors.NotFoundf("kafka.zookeeper.connect")
	}
	kafkaZKRoot, exist := p.Get("kafka.zookeeper.root")
	if !exist {
		return nil, errors.NotFoundf("kafka.zookeeper.root")
	}
	kafkaBrokerAddr, exist := p.Get("kafka.broker.connect")
	if !exist {
		return nil, errors.NotFoundf("kafka.broker.connect")
	}

	kafkaPartitions := int(p.GetInt64("kafka.topic.partitions", 0))
	if kafkaPartitions == 0 {
		return nil, errors.NotValidf("kafka.topic.partitions")
	}
	kafkaReplications := int(p.GetInt64("kafka.topic.replications", 0))
	if kafkaReplications == 0 {
		return nil, errors.NotValidf("kafka.topic.replications")
	}

	// proxy config
	proxyId := int(p.GetInt64("proxy.id", -1))
	if proxyId == -1 {
		return nil, errors.NotValidf("proxy.id")
	}
	kafkaLib, exist := p.Get("kafka.lib")
	if !exist {
		return nil, errors.NotFoundf("kafka.lib")
	}
	uiDir, exist := p.Get("ui.dir")
	if !exist {
		return nil, errors.NotFoundf("ui.dir")
	}
	httpPort, exist := p.Get("protocol.http.port")
	if !exist {
		return nil, errors.NotFoundf("protocol.http.port")
	}

	mcPort, exist := p.Get("protocol.mc.port")
	if !exist {
		return nil, errors.NotFoundf("protocol.mc.port")
	}
	mcSocketRecvBuffer := int(p.GetInt64("mc.socket.recv.buffer", 4096))
	mcSocketSendBuffer := int(p.GetInt64("mc.socket.send.buffer", 4096))

	motanPort, exist := p.Get("protocol.motan.port")
	if !exist {
		return nil, errors.NotFoundf("protocol.motan.port")
	}

	metaDataZKAddr, exist := p.Get("metadata.zookeeper.connect")
	if !exist {
		return nil, errors.NotFoundf("metadata.zookeeper.connect")
	}
	metaDataZKRoot, exist := p.Get("metadata.zookeeper.root")
	if !exist {
		return nil, errors.NotFoundf("metadata.zookeeper.root")
	}
	redisAddr, exist := p.Get("redis.connect")
	if !exist {
		return nil, errors.NotFoundf("redis.connect")
	}

	return &Config{
		KafkaZKAddr:       kafkaZKAddr,
		KafkaZKRoot:       kafkaZKRoot,
		KafkaBrokerAddr:   kafkaBrokerAddr,
		KafkaPartitions:   kafkaPartitions,
		KafkaReplications: kafkaReplications,

		ProxyId:            proxyId,
		UiDir:              uiDir,
		HttpPort:           httpPort,
		McPort:             mcPort,
		McSocketRecvBuffer: mcSocketRecvBuffer,
		McSocketSendBuffer: mcSocketSendBuffer,
		MotanPort:          motanPort,
		KafkaLib:           kafkaLib,
		MetaDataZKAddr:     metaDataZKAddr,
		MetaDataZKRoot:     metaDataZKRoot,
		RedisAddr:          redisAddr,
	}, nil
}
