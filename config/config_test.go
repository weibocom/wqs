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
	config, _ := NewConfigFromFile("../test.properties")
	fmt.Println("KafkaZKAddr:", config.KafkaZKAddr)
	fmt.Println("KafkaZKRoot:", config.KafkaZKRoot)
	fmt.Println("KafkaBrokerAddr:", config.KafkaBrokerAddr)
	fmt.Println("KafkaPartitions:", config.KafkaPartitions)
	fmt.Println("KafkaReplications:", config.KafkaReplications)

	fmt.Println("ProxyId:", config.ProxyId)
	fmt.Println("UiDir:", config.UiDir)
	fmt.Println("HttpPort:", config.HttpPort)
	fmt.Println("McPort:", config.McPort)
	fmt.Println("McSocketRecvBuffer:", config.McSocketRecvBuffer)
	fmt.Println("McSocketSendBuffer:", config.McSocketSendBuffer)
	fmt.Println("MotanPort:", config.MotanPort)
	fmt.Println("KafkaLib:", config.KafkaLib)
	fmt.Println("MetaDataZKAddr:", config.MetaDataZKAddr)
	fmt.Println("MetaDataZKRoot:", config.MetaDataZKRoot)
	fmt.Println("RedisAddr:", config.RedisAddr)

}
