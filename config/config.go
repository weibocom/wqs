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
	ZookeeperAddr      string
	ZookeeperRootPath  string
	BrokerAddr         string
	HttpPort           string
	McPort             string
	McSocketRecvBuffer int
	McSocketSendBuffer int
	MotanPort          string
	UiDir              string
	PartitionsNum      int
	ReplicationsNum    int
	RedisAddr          string
}

func NewConfig() *Config {
	return &Config{
		ZookeeperAddr:      "localhost:2181",
		ZookeeperRootPath:  "",
		BrokerAddr:         "localhost:9092",
		HttpPort:           "8080",
		McPort:             "11211",
		MotanPort:          "8881",
		UiDir:              "./ui",
		PartitionsNum:      16,
		ReplicationsNum:    2,
		RedisAddr:          "localhost:6379",
		McSocketRecvBuffer: 4096,
		McSocketSendBuffer: 4096,
	}
}

func NewConfigFromFile(file string) (*Config, error) {

	p, err := properties.LoadFile(file, properties.UTF8)
	if err != nil {
		return nil, errors.Trace(err)
	}

	httpPort, exist := p.Get("http.port")
	if !exist {
		return nil, errors.NotFoundf("http.port")
	}

	uiDir, exist := p.Get("ui.dir")
	if !exist {
		return nil, errors.NotFoundf("ui.dir")
	}

	zookeeperAddr, exist := p.Get("zookeeper.connect")
	if !exist {
		return nil, errors.NotFoundf("zookeeper.connect")
	}

	zookeeperRootPath, exist := p.Get("zookeeper.rootpath")
	if !exist {
		return nil, errors.NotFoundf("zookeeper.rootpath")
	}

	brokerAddr, exist := p.Get("broker.connect")
	if !exist {
		return nil, errors.NotFoundf("broker.connect")
	}

	partitionsNum := int(p.GetInt64("partitions.num", 0))
	if partitionsNum == 0 {
		return nil, errors.NotValidf("partitions.num")
	}

	replicationsNum := int(p.GetInt64("replications.num", -1))
	if replicationsNum == -1 {
		return nil, errors.NotValidf("replications.num")
	}

	redisAddr, exist := p.Get("redis.connect")
	if !exist {
		return nil, errors.NotFoundf("redis.connect")
	}

	mcPort, exist := p.Get("mc.port")
	if !exist {
		return nil, errors.NotFoundf("mc.port")
	}

	motanPort, exist := p.Get("motan.port")
	if !exist {
		return nil, errors.NotFoundf("motan.port")
	}

	mcSocketRecvBuffer := int(p.GetInt64("mc.socket.recv.buffer", 4096))
	mcSocketSendBuffer := int(p.GetInt64("mc.socket.send.buffer", 4096))

	return &Config{
		HttpPort:           httpPort,
		UiDir:              uiDir,
		ZookeeperAddr:      zookeeperAddr,
		ZookeeperRootPath:  zookeeperRootPath,
		BrokerAddr:         brokerAddr,
		PartitionsNum:      partitionsNum,
		ReplicationsNum:    replicationsNum,
		RedisAddr:          redisAddr,
		McPort:             mcPort,
		MotanPort:          motanPort,
		McSocketRecvBuffer: mcSocketRecvBuffer,
		McSocketSendBuffer: mcSocketSendBuffer,
	}, nil
}
