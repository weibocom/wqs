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
	"unsafe"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/weibocom/wqs/log"
	"github.com/weibocom/wqs/utils/list"
)

const (
	timeout    = 10 * time.Millisecond
	paddingMax = 1024
	expiredMax = 10 * time.Second
)

var (
	ErrTimeout = errors.New("timeout")
	ErrClosed  = errors.New("consumer closed")
	ErrBadAck  = errors.New("bad ack")
)

type ackHead struct {
	ackHead list.Node
	getHead list.Node
}

func (h *ackHead) Push(n *ackNode) {
	n.ackList.InsertToTail(&h.ackHead)
	n.getList.InsertToTail(&h.getHead)
}

func (h *ackHead) Empty() bool {
	return h.ackHead.Empty()
}

func (h *ackHead) Front() *ackNode {
	var aNode *ackNode
	node := h.ackHead.Next()
	aNode = (*ackNode)(list.ContainerOf(unsafe.Pointer(node), unsafe.Offsetof(aNode.ackList)))
	return aNode
}

func (h *ackHead) GetExpired(now time.Time) *ackNode {
	var node *ackNode
	if h.getHead.Empty() {
		return nil
	}

	n := h.getHead.Next()
	node = (*ackNode)(list.ContainerOf(unsafe.Pointer(n), unsafe.Offsetof(node.getList)))
	if now.Sub(node.expired) > expiredMax {
		node.getList.MoveToTail(&h.getHead)
		node.expired = now
		return node
	}
	return nil
}

func newAckHead() *ackHead {
	head := &ackHead{}
	head.ackHead.Init()
	head.getHead.Init()
	return head
}

type ackNode struct {
	msg     *sarama.ConsumerMessage
	expired time.Time
	ackList list.Node
	getList list.Node
}

func (n *ackNode) RemoveBySelf() {
	n.ackList.Remove()
	n.getList.Remove()
}

func newAckNode(msg *sarama.ConsumerMessage) *ackNode {
	node := &ackNode{msg: msg, expired: time.Now()}
	node.ackList.Init()
	node.getList.Init()
	return node
}

type Consumer struct {
	topic          string
	group          string
	consumer       *cluster.Consumer
	padding        uint32
	partitionHeads map[int32]*ackHead
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
		partitionHeads: make(map[int32]*ackHead),
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
		if msg, err = c.recv(); err == nil {
			head, ok := c.partitionHeads[msg.Partition]
			if !ok {
				head = newAckHead()
				c.partitionHeads[msg.Partition] = head
				c.ackMessages[msg.Partition] = make(map[int64]*ackNode)
			}
			node := newAckNode(msg)
			head.Push(node)
			c.ackMessages[msg.Partition][msg.Offset] = node
			c.padding++
			c.mu.Unlock()
			return
		}
	}

	if c.padding > 0 {
		now := time.Now()
	Found:
		for _, head := range c.partitionHeads {
			if node := head.GetExpired(now); node != nil {
				msg = node.msg
				err = nil
				break Found
			}
		}
	}
	c.mu.Unlock()
	if msg == nil && err == nil {
		err = ErrTimeout
	}
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

	//internal error, should not be empty
	if head.Empty() {
		c.mu.Unlock()
		return ErrBadAck
	}

	first := head.Front()
	node.RemoveBySelf()
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
