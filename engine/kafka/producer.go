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
	"crypto/md5"
	"encoding/hex"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/weibocom/wqs/log"
)

type Producer struct {
	producer sarama.SyncProducer
}

func NewProducer(brokerAddrs []string, conf *sarama.Config) (*Producer, error) {
	producer, err := sarama.NewSyncProducer(brokerAddrs, conf)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &Producer{producer}, nil
}

func (k *Producer) Send(topic string, key, data []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(data),
	}
	partition, offset, err := k.producer.SendMessage(msg)
	if err != nil {
		log.Debug("write message err: %s", err)
		return err
	}
	dig := md5.Sum(data)
	log.Infof("W %s@%d@%d %s", topic, partition, offset, hex.EncodeToString(dig[:]))
	return nil
}
