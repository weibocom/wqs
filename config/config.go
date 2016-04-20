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

package config

/*
主要提供配置加载功能，目前是从配置文件中进行配置加载
*/

import (
	"flag"
	//"fmt"
	"github.com/magiconair/properties"
)

type Config struct {
	ZookeeperAddr      string
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

func NewConfig() Config {
	config := Config{}
	config.ZookeeperAddr = "10.77.109.121:2181,10.77.109.122:2181,10.77.109.131:2181"
	config.BrokerAddr = "10.77.109.129:9092,10.77.109.130:9092"
	config.HttpPort = "8080"
	config.McPort = "11211"
	config.MotanPort = "8881"
	config.UiDir = "./ui"
	config.PartitionsNum = 16
	config.ReplicationsNum = 2
	config.RedisAddr = "10.77.109.132:6379"
	config.McSocketRecvBuffer = 4096
	config.McSocketSendBuffer = 4096
	return config
}

/*
从配置文件中加载配置，将proxy和kafka的配置加载到不同的结构体
*/
func (config *Config) loadProperties() {
	filename := flag.String("config", "config.properties", "Load Properties of Proxy and Kafka")
	flag.Parse()
	p := properties.MustLoadFile(*filename, properties.UTF8)

	config.HttpPort = p.MustGetString("http.port")
	config.UiDir = p.MustGetString("ui.dir")
	config.ZookeeperAddr = p.MustGetString("zookeeper.connect")
	config.BrokerAddr = p.MustGetString("broker.connect")
	config.PartitionsNum = p.MustGetInt("partitions.num")
	config.ReplicationsNum = p.MustGetInt("replications.num")
	config.RedisAddr = p.MustGetString("redis.connect")
	config.McPort = p.MustGetString("mc.port")
	config.MotanPort = p.MustGetString("motan.port")
	config.McSocketRecvBuffer = p.MustGetInt("mc.socket.recv.buffer")
	config.McSocketSendBuffer = p.MustGetInt("mc.socket.send.buffer")
}

func LoadConfig() Config {
	var config Config
	config.loadProperties()
	return config
}
