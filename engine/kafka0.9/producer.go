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
	"github.com/Shopify/sarama"
	log "github.com/cihub/seelog"
)

type Producer struct {
	producer sarama.SyncProducer
}

func NewProducer(addrs []string) *Producer {
	producer, err := sarama.NewSyncProducer(addrs, nil)
	if err != nil {
		log.Errorf("kafka producer init failed, addrs:%s, err:%v", addrs, err)
	}
	return &Producer{producer}
}

func (k *Producer) Send(topic string, data []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
	}
	partition, offset, err := k.producer.SendMessage(msg)
	log.Debugf("send message, topic:%s, partition:%d, offset:%d, err:%v", topic, partition, offset, err)
	return err
}
