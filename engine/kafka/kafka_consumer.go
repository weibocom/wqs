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

package kafka

import (
	"errors"
	"fmt"
	"strings"
	"time"

	log "github.com/cihub/seelog"
	"github.com/elodina/go_kafka_client"
	"github.com/weibocom/wqs/config"
)

type KafkaConsumer struct {
	consumConfig *go_kafka_client.ConsumerConfig
	consumer     *go_kafka_client.Consumer
	msgChan      chan *go_kafka_client.Message
	IsConnected  bool
	topic        string
	group        string
}

func NewKafkaConsumer(topic string, group string, config *config.Config) *KafkaConsumer {
	kafkaConsumer := KafkaConsumer{}

	kafkaConsumer.msgChan = make(chan *go_kafka_client.Message)

	kafkaConsumer.consumConfig = go_kafka_client.DefaultConsumerConfig()
	kafkaConsumer.consumConfig.Groupid = group

	//zk配置
	zkConfig := go_kafka_client.NewZookeeperConfig()
	zkConfig.ZookeeperConnect = strings.Split(config.ZookeeperAddr, ",")
	kafkaConsumer.consumConfig.Coordinator = go_kafka_client.NewZookeeperCoordinator(zkConfig)

	kafkaConsumer.consumConfig.Strategy = func(_ *go_kafka_client.Worker, msg *go_kafka_client.Message, id go_kafka_client.TaskId) go_kafka_client.WorkerResult {
		kafkaConsumer.msgChan <- msg
		return go_kafka_client.NewSuccessfulResult(id)
	}

	kafkaConsumer.consumConfig.WorkerFailureCallback = func(_ *go_kafka_client.WorkerManager) go_kafka_client.FailedDecision {
		fmt.Println("====WorkerFailureCallback====")
		return go_kafka_client.CommitOffsetAndContinue
	}

	kafkaConsumer.consumConfig.WorkerFailedAttemptCallback = func(_ *go_kafka_client.Task, _ go_kafka_client.WorkerResult) go_kafka_client.FailedDecision {
		fmt.Println("====WorkerFailedAttemptCallback====")
		return go_kafka_client.CommitOffsetAndContinue
	}

	kafkaConsumer.IsConnected = false
	kafkaConsumer.topic = topic
	kafkaConsumer.group = group

	return &kafkaConsumer
}

func (this *KafkaConsumer) Connect() bool {
	this.consumer = go_kafka_client.NewConsumer(this.consumConfig)
	topicCountMap := make(map[string]int)
	topicCountMap[this.topic] = 1
	go this.consumer.StartStatic(topicCountMap)
	this.IsConnected = true
	return true
}

func (this *KafkaConsumer) Disconnect() bool {
	result := <-this.consumer.Close()
	return result
}

func (this *KafkaConsumer) Get() (data []byte, err error) {
	select {
	case msg := <-this.msgChan:
		data = msg.Value
	case <-time.After(500 * time.Millisecond): // 500ms超时
		log.Warnf("time out, queue: %s biz: %s", this.topic, this.group)
		err = errors.New("time out")
	}
	return
}
