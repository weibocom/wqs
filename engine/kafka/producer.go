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
	"github.com/juju/errors"
)

type Producer struct {
	sarama.SyncProducer
}

func NewProducer(brokerAddrs []string, conf *sarama.Config) (*Producer, error) {
	producer, err := sarama.NewSyncProducer(brokerAddrs, conf)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &Producer{producer}, nil
}

func (p *Producer) Send(topic string, key, data []byte) (partition int32, offset int64, err error) {

	return p.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(data),
	})
}
