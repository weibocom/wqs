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
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/weibocom/wqs/log"
)

type Consumer struct {
	topic    string
	group    string
	consumer *cluster.Consumer
}

const (
	timeout = 10 * time.Millisecond
)

func NewConsumer(brokerAddrs []string, topic, group string) (*Consumer, error) {
	//FIXME: consumer的config是否需要支持配置
	consumer, err := cluster.NewConsumer(brokerAddrs, group, []string{topic}, nil)
	if err != nil {
		log.Errorf("kafka consumer init failed, addrs:%s, err:%v", brokerAddrs, err)
		return nil, err
	}
	go func() {
		for err := range consumer.Errors() {
			log.Warnf("consumer err : %v", err)
		}
	}()
	return &Consumer{topic, group, consumer}, nil
}

func (c *Consumer) Recv() (msg *sarama.ConsumerMessage, err error) {

	select {
	case msg = <-c.consumer.Messages():
		c.consumer.MarkOffset(msg, "") // metedata的用处？
	case <-time.After(timeout):
		err = errors.New("time out")
	}
	return
}

func (c *Consumer) Close() error {
	return c.consumer.Close()
}
