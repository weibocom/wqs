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

import (
	"fmt"
	"testing"
)

func TestConfig(t *testing.T) {
	config, _ := NewConfigFromFile("test.properties")
	fmt.Println(config.KafkaZKAddr)
	fmt.Println(config.KafkaZKRoot)
	fmt.Println(config.KafkaBrokerAddr)
	fmt.Println(config.KafkaPartitions)
	fmt.Println(config.KafkaReplications)

	fmt.Println(config.UiDir)
	fmt.Println(config.HttpPort)
	fmt.Println(config.McPort)
	fmt.Println(config.McSocketRecvBuffer)
	fmt.Println(config.McSocketSendBuffer)
	fmt.Println(config.MotanPort)
	fmt.Println(config.KafkaLib)
	fmt.Println(config.MetaDataZKAddr)
	fmt.Println(config.MetaDataZKRoot)
	fmt.Println(config.RedisAddr)

}
