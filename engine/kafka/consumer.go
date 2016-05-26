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
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/weibocom/wqs/log"
)

const (
	timeout    = 10 * time.Millisecond
	paddingMax = 1024
)

var (
	ErrTimeout = errors.New("Timeout")
	ErrClosed  = errors.New("consumer closed")
	ErrBadAck  = errors.New("bad ack")
)

type ackNode struct {
	msg  *sarama.ConsumerMessage
	prev *ackNode
	next *ackNode
}

func (head *ackNode) Empty() bool {
	return head.next == head
}

func (head *ackNode) Front() *ackNode {
	if head.next != head {
		return head.next
	}
	return nil
}

func (head *ackNode) RemoveSelf() {
	head.prev.next = head.next
	head.next.prev = head.prev
	head.prev = nil
	head.next = nil
}

func (head *ackNode) Push(a *ackNode) {
	head.prev.next = a
	a.prev = head.prev
	a.next = head
	head.prev = a
}

func newAckNode(msg *sarama.ConsumerMessage) *ackNode {
	node := &ackNode{msg: msg}
	node.prev = node
	node.next = node
	return node
}

type Consumer struct {
	topic          string
	group          string
	consumer       *cluster.Consumer
	padding        uint32
	partitionHeads map[int32]*ackNode
	ackMessages    map[int32]map[int64]*ackNode
	mu             sync.Mutex
}

func NewConsumer(brokerAddrs []string, config *cluster.Config, topic, group string) (*Consumer, error) {
	//FIXME: consumer的config是否需要支持配置
	consumer, err := cluster.NewConsumer(brokerAddrs, group, []string{topic}, config)
	if err != nil {
		log.Errorf("kafka consumer init failed, addrs:%s, err:%v", brokerAddrs, err)
		return nil, err
	}
	go func() {
		for err := range consumer.Errors() {
			log.Warnf("consumer err : %v", err)
		}
	}()
	return &Consumer{
		topic:          topic,
		group:          group,
		consumer:       consumer,
		padding:        0,
		partitionHeads: make(map[int32]*ackNode),
		ackMessages:    make(map[int32]map[int64]*ackNode),
	}, nil
}

func (c *Consumer) recv() (msg *sarama.ConsumerMessage, err error) {
	select {
	case msg = <-c.consumer.Messages():
		if msg == nil {
			err = ErrClosed
		}
	case <-time.After(timeout):
		err = ErrTimeout
	}
	return
}

//Get a message
func (c *Consumer) Recv() (msg *sarama.ConsumerMessage, err error) {
	c.mu.Lock()
	if c.padding < paddingMax {
		msg, err = c.recv()
		if err == nil {
			head, ok := c.partitionHeads[msg.Partition]
			if !ok {
				head = newAckNode(nil)
				c.partitionHeads[msg.Partition] = head
				c.ackMessages[msg.Partition] = make(map[int64]*ackNode)
			}
			node := newAckNode(msg)
			head.Push(node)
			c.ackMessages[msg.Partition][msg.Offset] = node
			c.padding++
		}
	} else {
	Found:
		for _, head := range c.partitionHeads {
			if !head.Empty() {
				msg = head.Front().msg
				break Found
			}
		}
	}
	c.mu.Unlock()
	return
}

func (c *Consumer) Ack(partition int32, offset int64) error {
	c.mu.Lock()
	head, ok := c.partitionHeads[partition]
	if !ok {
		c.mu.Unlock()
		return ErrBadAck
	}

	partitionMessages, ok := c.ackMessages[partition]
	if !ok {
		c.mu.Unlock()
		return ErrBadAck
	}

	node, ok := partitionMessages[offset]
	if !ok {
		c.mu.Unlock()
		return ErrBadAck
	}

	first := head.Front()
	node.RemoveSelf()
	delete(partitionMessages, offset)
	c.padding--
	if first == node {
		c.consumer.MarkOffset(node.msg, "")
		if !head.Empty() {
			first = head.Front()
			if first.msg.Offset > offset+1 {
				c.consumer.MarkPartitionOffset(first.msg.Topic,
					first.msg.Partition, first.msg.Offset-1, "")
			}
		}
	}
	c.mu.Unlock()
	return nil
}

func (c *Consumer) Close() error {
	return c.consumer.Close()
}
