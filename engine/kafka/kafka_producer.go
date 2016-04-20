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
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/elodina/siesta"
	siesta_producer "github.com/elodina/siesta-producer"
)

type KafkaProducer struct {
	producer *siesta_producer.KafkaProducer
}

func NewKafkaProducer(brokerAddr string) *KafkaProducer {
	kafkaProducer := KafkaProducer{}

	producerConfig := siesta_producer.NewProducerConfig()
	connConfig := siesta.NewConnectorConfig()

	brokerList := strings.Split(brokerAddr, ",")

	producerConfig.BrokerList = brokerList
	connConfig.BrokerList = brokerList

	connector, err := siesta.NewDefaultConnector(connConfig)
	if err != nil {
		log.Errorf("error:%s", err.Error())
	}

	kafkaProducer.producer = siesta_producer.NewKafkaProducer(producerConfig, siesta_producer.ByteSerializer, siesta_producer.ByteSerializer, connector)

	return &kafkaProducer
}

func (this *KafkaProducer) Set(topic string, data []byte) error {
	record := &siesta_producer.ProducerRecord{
		Topic: topic,
		Value: data,
	}

	recordMetadata := <-this.producer.Send(record)

	// 这里注意：消息写入正常，err也不为空，内容为：No error - it worked!，太坑了
	if recordMetadata.Error == siesta.ErrNoError {
		log.Infof("send message successfully, topic: %s, msg: %s", topic, string(data))
		return nil
	} else {
		log.Warnf("send message failed, topic: %s, msg: %s", topic, string(data))
		return recordMetadata.Error
	}
}
