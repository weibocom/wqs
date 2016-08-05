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
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/weibocom/wqs/log"
	"github.com/weibocom/wqs/metrics"
	"github.com/weibocom/wqs/utils/list"
)

const (
	timeout    = 8 * time.Millisecond
	paddingMax = 1024
	expiredMax = 10 * time.Second
)

var (
	ErrTimeout          = errors.New("timeout")
	ErrClosed           = errors.New("consumer closed")
	ErrBadAck           = errors.New("bad ack")
	ErrNewConsumer      = errors.New("new kafka consumer failed")
	ErrIdcNotExist      = errors.New("idc not exist")
	ErrInvaildPartition = errors.New("invaild partition")
	ErrInvaildOffset    = errors.New("invaild offset")
	ErrEmptyAddr        = errors.New("empty addrs")
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

func (n *ackNode) Remove() {
	n.ackList.Remove()
	n.getList.Remove()
}

func newAckNode(msg *sarama.ConsumerMessage) *ackNode {
	node := &ackNode{msg: msg, expired: time.Now()}
	node.ackList.Init()
	node.getList.Init()
	return node
}

type none struct{}

type message struct {
	idc string
	msg *sarama.ConsumerMessage
}

type ackGroup struct {
	ackMessages    map[int32]map[int64]*ackNode
	partitionHeads map[int32]*ackHead
	sync.Mutex
}

type Consumer struct {
	topic     string
	group     string
	padding   int32
	consumers map[string]*cluster.Consumer
	ackGroups map[string]*ackGroup
	messages  chan *message
	dying     chan none
	mu        sync.Mutex
	dead      sync.WaitGroup
}

func (c *Consumer) receiveNotification(idc string, notification <-chan *cluster.Notification) {
	for _ = range notification {
		metrics.AddMeter(c.topic+"."+c.group+"."+metrics.Rebalance+"."+metrics.Qps, 1)
		log.Infof("idc %q topic %q group %q consumer occur rebalance", idc, c.topic, c.group)
	}
}

func (c *Consumer) dispatch(idc string, in <-chan *sarama.ConsumerMessage, errors <-chan error) {
	defer func() {
		if x := recover(); x != nil {
			log.Errorf("idc %q topic %q group %q dispatch painc: %v", idc, c.topic, c.group, x)
		}
		c.dead.Done()
	}()

	for {
		select {
		case msg := <-in:
			if msg == nil {
				// in channel closed, it means consumer closed.
				return
			}
			select {
			case c.messages <- &message{idc: idc, msg: msg}:
			case <-c.dying:
				return
			}
		case err := <-errors:
			metrics.AddMeter(c.topic+"."+c.group+"."+metrics.RecvError+"."+metrics.Qps, 1)
			log.Errorf("idc %q topic %q group %q consumer occur error: %v", idc, c.topic, c.group, err)
		case <-c.dying:
			return
		}
	}
}

func NewConsumer(brokerAddrs map[string][]string, config *cluster.Config, topic, group string) (*Consumer, error) {

	var consumer *Consumer
	kConsumers := make(map[string]*cluster.Consumer)
	if len(brokerAddrs) == 0 {
		return nil, ErrEmptyAddr
	}
	for idc, brokerAddr := range brokerAddrs {
		kConsumer, err := cluster.NewConsumer(brokerAddr, group, []string{topic}, config)
		if err != nil {
			log.Errorf("new kafka consumer failed, addrs: %s, idc: %s, err: %v", brokerAddr, idc, err)
			goto Error
		}
		kConsumers[idc] = kConsumer
	}

	consumer = &Consumer{
		topic:     topic,
		group:     group,
		padding:   0,
		consumers: kConsumers,
		ackGroups: make(map[string]*ackGroup),
		messages:  make(chan *message),
		dying:     make(chan none),
	}

	for idc, kConsumer := range kConsumers {
		consumer.dead.Add(1)
		go consumer.dispatch(idc, kConsumer.Messages(), kConsumer.Errors())

		if config.Group.Return.Notifications {
			go consumer.receiveNotification(idc, kConsumer.Notifications())
		}
	}
	return consumer, nil

Error:
	for idc, kConsumer := range kConsumers {
		if err := kConsumer.Close(); err != nil {
			log.Errorf("on error, close %s kConsumer err: %v", idc, err)
		}
	}
	return nil, ErrNewConsumer
}

func (c *Consumer) recv() (msg *sarama.ConsumerMessage, idc string, err error) {
	select {
	case m := <-c.messages:
		if m == nil {
			err = ErrClosed
			return
		}
		// 用2个锁来减少ack数据结构的锁粒度，保证一定的并发效率
		node := newAckNode(m.msg)
		c.mu.Lock()
		g, ok := c.ackGroups[m.idc]
		if !ok {
			g = &ackGroup{
				ackMessages:    make(map[int32]map[int64]*ackNode),
				partitionHeads: make(map[int32]*ackHead),
			}
			c.ackGroups[m.idc] = g
		}
		c.mu.Unlock()
		msg = m.msg
		idc = m.idc
		g.Lock()
		head, ok := g.partitionHeads[msg.Partition]
		if !ok {
			head = newAckHead()
			g.partitionHeads[msg.Partition] = head
			g.ackMessages[msg.Partition] = make(map[int64]*ackNode)
		}
		head.Push(node)
		g.ackMessages[msg.Partition][msg.Offset] = node
		atomic.AddInt32(&c.padding, 1)
		g.Unlock()
	case <-time.After(timeout):
		err = ErrTimeout
	}
	return
}

//Get a message
func (c *Consumer) Recv() (msg *sarama.ConsumerMessage, idc string, err error) {

	if atomic.LoadInt32(&c.padding) < paddingMax {
		if msg, idc, err = c.recv(); err == nil {
			return msg, idc, nil
		}
	}

	if atomic.LoadInt32(&c.padding) > 0 {
		now := time.Now()
		// TODO 这里怎么优化？如何做到遍历的同时不同时获得2个锁，减小锁粒度。
		c.mu.Lock()
	Found:
		for i, g := range c.ackGroups {
			g.Lock()
			for _, head := range g.partitionHeads {
				if node := head.GetExpired(now); node != nil {
					msg = node.msg
					idc = i
					err = nil
					g.Unlock()
					break Found
				}
			}
			g.Unlock()
		}
		c.mu.Unlock()
	}

	if msg == nil && err == nil {
		err = ErrTimeout
	}
	return msg, idc, err
}

func (c *Consumer) Ack(idc string, partition int32, offset int64) error {

	c.mu.Lock()
	g, ok := c.ackGroups[idc]
	c.mu.Unlock()
	if !ok {
		return ErrIdcNotExist
	}

	// 不采用defer unlock是为了减少defer产生的allocate和更高的执行效率，可惜golang没有RAII特性。
	// refer http://lk4d4.darth.io/posts/defer/
	g.Lock()
	head, ok := g.partitionHeads[partition]
	if !ok {
		g.Unlock()
		return ErrInvaildPartition
	}

	partitionMessages, ok := g.ackMessages[partition]
	if !ok {
		g.Unlock()
		return ErrInvaildPartition
	}

	node, ok := partitionMessages[offset]
	if !ok {
		g.Unlock()
		return ErrInvaildOffset
	}

	//internal error, should not be empty
	if head.Empty() {
		g.Unlock()
		return ErrBadAck
	}

	first := head.Front()
	node.Remove()
	delete(partitionMessages, offset)
	atomic.AddInt32(&c.padding, -1)
	if first == node {
		// c.consumers 是一个read-only的map，因此不需要锁保护
		kConsumer := c.consumers[idc]
		kConsumer.MarkOffset(node.msg, "")
		if !head.Empty() {
			first = head.Front()
			if first.msg.Offset > offset+1 {
				kConsumer.MarkPartitionOffset(first.msg.Topic,
					first.msg.Partition, first.msg.Offset-1, "")
			}
		}
	}
	g.Unlock()
	return nil
}

// Close 不能多次重复调用
func (c *Consumer) Close() {
	close(c.dying)
	c.dead.Wait()
	for idc, kConsumer := range c.consumers {
		if err := kConsumer.Close(); err != nil {
			log.Errorf("idc %s consumer close occur error: %v", idc, err)
		}
	}
}
