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

package queue

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/weibocom/wqs/config"
	"github.com/weibocom/wqs/engine/kafka"
	"github.com/weibocom/wqs/log"
	"github.com/weibocom/wqs/metrics"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/juju/errors"
)

type kafkaLogger struct {
}

func (l *kafkaLogger) Print(v ...interface{}) {
	args := []interface{}{"[sarama] "}
	args = append(args, v...)
	log.Info(args...)
}

func (l *kafkaLogger) Printf(format string, v ...interface{}) {
	log.Info("[sarama] ", fmt.Sprintf(format, v...))
}

func (l *kafkaLogger) Println(v ...interface{}) {
	args := []interface{}{"[sarama] "}
	args = append(args, v...)
	log.Info(args...)
}

func init() {
	sarama.Logger = &kafkaLogger{}
}

type queueImp struct {
	conf          *config.Config
	clusterConfig *cluster.Config
	metadata      *Metadata
	producer      *kafka.Producer
	monitor       *metrics.Monitor
	idGenerator   *idGenerator
	consumerMap   map[string]*kafka.Consumer
	vaildName     *regexp.Regexp
	mu            sync.Mutex
}

func newQueue(config *config.Config) (*queueImp, error) {

	hostname, err := os.Hostname()
	if err != nil {
		return nil, errors.Trace(err)
	}
	clusterConfig := cluster.NewConfig()
	// Network
	clusterConfig.Config.Net.KeepAlive = 30 * time.Second
	clusterConfig.Config.Net.MaxOpenRequests = 20
	clusterConfig.Config.Net.DialTimeout = 10 * time.Second
	clusterConfig.Config.Net.ReadTimeout = 10 * time.Second
	clusterConfig.Config.Net.WriteTimeout = 10 * time.Second
	// Metadata
	clusterConfig.Config.Metadata.Retry.Backoff = 100 * time.Millisecond
	clusterConfig.Config.Metadata.Retry.Max = 5
	clusterConfig.Config.Metadata.RefreshFrequency = 1 * time.Minute
	// Producer
	clusterConfig.Config.Producer.RequiredAcks = sarama.WaitForLocal
	//conf.Producer.RequiredAcks = sarama.NoResponse //this one high performance than WaitForLocal
	clusterConfig.Config.Producer.Partitioner = sarama.NewRandomPartitioner
	clusterConfig.Config.Producer.Flush.Frequency = time.Millisecond
	clusterConfig.Config.Producer.Flush.MaxMessages = 200
	clusterConfig.Config.ChannelBufferSize = 1024
	// Common
	clusterConfig.Config.ClientID = fmt.Sprintf("%d..%s", os.Getpid(), hostname)
	clusterConfig.Group.Heartbeat.Interval = 50 * time.Millisecond
	// Consumer
	clusterConfig.Config.Consumer.Retry.Backoff = 500 * time.Millisecond
	clusterConfig.Config.Consumer.Offsets.CommitInterval = 100 * time.Millisecond
	//clusterConfig.Config.Consumer.Offsets.Initial = sarama.OffsetOldest
	clusterConfig.Group.Offsets.Retry.Max = 3
	clusterConfig.Group.Session.Timeout = 10 * time.Second

	metadata, err := NewMetadata(config, &clusterConfig.Config)
	if err != nil {
		return nil, errors.Trace(err)
	}

	srvInfo := &ServiceInfo{
		Host:   hostname,
		Config: config,
	}
	err = metadata.RegisterService(config.ProxyId, srvInfo.String())
	if err != nil {
		metadata.Close()
		return nil, errors.Trace(err)
	}
	producer, err := kafka.NewProducer(metadata.manager.BrokerAddrs(), &clusterConfig.Config)
	if err != nil {
		return nil, errors.Trace(err)
	}

	qs := &queueImp{
		conf:          config,
		clusterConfig: clusterConfig,
		metadata:      metadata,
		producer:      producer,
		monitor:       metrics.NewMonitor(config.RedisAddr),
		idGenerator:   newIDGenerator(uint64(config.ProxyId)),
		vaildName:     regexp.MustCompile(`^[a-zA-Z0-9]{1,20}$`),
		consumerMap:   make(map[string]*kafka.Consumer),
	}
	return qs, nil
}

//Create a queue by name.
func (q *queueImp) Create(queue string) error {
	// 1. check queue name valid
	if !q.vaildName.MatchString(queue) {
		return errors.NotValidf("queue : %q", queue)
	}
	// 2. add metadata of queue
	if err := q.metadata.AddQueue(queue); err != nil {
		return errors.Trace(err)
	}
	return nil
}

//Updata queue information by name. Nothing to be update so far.
func (q *queueImp) Update(queue string) error {

	if !q.vaildName.MatchString(queue) {
		return errors.NotValidf("queue : %q", queue)
	}
	//TODO
	err := q.metadata.RefreshMetadata()
	if err != nil {
		return errors.Trace(err)
	}
	if exist := q.metadata.ExistQueue(queue); !exist {
		return errors.NotFoundf("queue : %q", queue)
	}
	//TODO 暂时没有需要更新的内容
	return nil
}

//Delete queue by name
func (q *queueImp) Delete(queue string) error {
	// 1. check queue name valid
	if !q.vaildName.MatchString(queue) {
		return errors.NotValidf("queue : %q", queue)
	}
	// 2. delete metadata of queue
	if err := q.metadata.DelQueue(queue); err != nil {
		return errors.Trace(err)
	}
	return nil
}

//Get queue information by queue name and group name
//When queue name is "" to get all queue' information.
func (q *queueImp) Lookup(queue string, group string) (queueInfos []*QueueInfo, err error) {

	err = q.metadata.RefreshMetadata()
	if err != nil {
		return nil, errors.Trace(err)
	}

	switch {
	case queue == "":
		//Get all queue's information
		queues := q.metadata.GetQueues()
		queueInfos, err = q.metadata.GetQueueConfig(queues...)
	case queue != "" && group == "":
		//Get a queue's all groups information
		queueInfos, err = q.metadata.GetQueueConfig(queue)
	default:
		//Get group's information by queue and group's name
		exist := q.metadata.ExistGroup(queue, group)
		if !exist {
			err = errors.NotFoundf("queue: %q, group : %q")
			return
		}
		queueInfos, err = q.metadata.GetQueueConfig(queue)
		if err != nil || len(queueInfos) != 1 {
			return
		}
		for _, groupConfig := range queueInfos[0].Groups {
			if groupConfig.Group == group {
				queueInfos[0].Groups = queueInfos[0].Groups[:1]
				queueInfos[0].Groups[0] = groupConfig
				return
			}
		}
		queueInfos[0].Groups = make([]*GroupConfig, 0)
	}
	return
}

func (q *queueImp) AddGroup(group string, queue string,
	write bool, read bool, url string, ips []string) error {

	if !q.vaildName.MatchString(group) || !q.vaildName.MatchString(queue) {
		return errors.NotValidf("group : %q , queue : %q", group, queue)
	}

	if err := q.metadata.AddGroupConfig(group, queue, write, read, url, ips); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (q *queueImp) UpdateGroup(group string, queue string,
	write bool, read bool, url string, ips []string) error {

	if !q.vaildName.MatchString(group) || !q.vaildName.MatchString(queue) {
		return errors.NotValidf("group : %q , queue : %q", group, queue)
	}

	if err := q.metadata.UpdateGroupConfig(group, queue, write, read, url, ips); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (q *queueImp) DeleteGroup(group string, queue string) error {

	if !q.vaildName.MatchString(group) || !q.vaildName.MatchString(queue) {
		return errors.NotValidf("group : %q , queue : %q", group, queue)
	}

	if err := q.metadata.DeleteGroupConfig(group, queue); err != nil {
		return errors.Trace(err)
	}

	return nil
}

//Get group's information
func (q *queueImp) LookupGroup(group string) ([]*GroupInfo, error) {

	groupInfos := make([]*GroupInfo, 0)
	if err := q.metadata.RefreshMetadata(); err != nil {
		return groupInfos, errors.Trace(err)
	}

	if group == "" {
		//GET all groups' information
		groupMap := q.metadata.GetGroupMap()
		for group, queues := range groupMap {

			groupInfo := GroupInfo{
				Group:  group,
				Queues: make([]*GroupConfig, 0),
			}

			for _, queue := range queues {
				groupConfig, err := q.metadata.GetGroupConfig(group, queue)
				if err != nil {
					continue
				}
				groupInfo.Queues = append(groupInfo.Queues, groupConfig)
			}
			groupInfos = append(groupInfos, &groupInfo)
		}
	} else {
		//GET one group's information
		groupMap := q.metadata.GetGroupMap()
		queues, ok := groupMap[group]
		if !ok {
			return groupInfos, errors.NotFoundf("group : %q", group)
		}

		groupInfo := GroupInfo{
			Group:  group,
			Queues: make([]*GroupConfig, 0),
		}

		for _, queue := range queues {
			groupConfig, err := q.metadata.GetGroupConfig(group, queue)
			if err != nil {
				continue
			}
			groupInfo.Queues = append(groupInfo.Queues, groupConfig)
		}
		groupInfos = append(groupInfos, &groupInfo)
	}
	return groupInfos, nil
}

func (q *queueImp) GetSingleGroup(group string, queue string) (*GroupConfig, error) {
	return q.metadata.GetGroupConfig(group, queue)
}

func (q *queueImp) SendMessage(queue string, group string, data []byte, flag uint64) (string, error) {
	start := time.Now()

	exist := q.metadata.ExistGroup(queue, group)
	if !exist {
		return "", errors.NotFoundf("queue : %q , group: %q", queue, group)
	}

	sequenceID := q.idGenerator.Get()
	key := fmt.Sprintf("%x:%x", sequenceID, flag)

	partition, offset, err := q.producer.Send(queue, []byte(key), data)
	if err != nil {
		return "", errors.Trace(err)
	}
	messageID := fmt.Sprintf("%x:%s:%s:%x:%x", sequenceID, queue, group, partition, offset)
	cost := time.Now().Sub(start).Nanoseconds() / 1e6

	metrics.Add(queue+"#"+group+"#sent", 1, cost)

	log.Debugf("send %s:%s key %s id %s cost %d", queue, group, key, messageID, cost)
	return messageID, nil
}

func (q *queueImp) RecvMessage(queue string, group string) (string, []byte, uint64, error) {
	var err error
	start := time.Now()
	exist := q.metadata.ExistGroup(queue, group)
	if !exist {
		return "", nil, 0, errors.NotFoundf("queue : %q , group: %q", queue, group)
	}

	owner := fmt.Sprintf("%s@%s", queue, group)
	q.mu.Lock()
	consumer, ok := q.consumerMap[owner]
	if !ok {
		consumer, err = kafka.NewConsumer(q.metadata.manager.BrokerAddrs(), q.clusterConfig, queue, group)
		if err != nil {
			q.mu.Unlock()
			return "", nil, 0, errors.Trace(err)
		}
		q.consumerMap[owner] = consumer
	}
	q.mu.Unlock()

	msg, err := consumer.Recv()
	if err != nil {
		return "", nil, 0, errors.Trace(err)
	}

	var sequenceID, flag uint64
	tokens := strings.Split(string(msg.Key), ":")
	sequenceID, _ = strconv.ParseUint(tokens[0], 16, 64)
	if len(tokens) > 1 {
		flag, _ = strconv.ParseUint(tokens[1], 16, 32)
	}

	messageID := fmt.Sprintf("%x:%s:%s:%x:%x", sequenceID, queue, group, msg.Partition, msg.Offset)

	end := time.Now()
	cost := end.Sub(start).Nanoseconds() / 1e6
	delay := end.UnixNano()/1e6 - baseTime - int64((sequenceID>>24)&0xFFFFFFFFFF)

	metrics.Add(queue+"#"+group+"#recv", 1, cost, delay)

	log.Debugf("recv %s:%s key %s id %s cost %d delay %d", queue, group, string(msg.Key), messageID, cost, delay)
	return messageID, msg.Value, flag, nil
}

func (q *queueImp) AckMessage(queue string, group string, id string) error {
	start := time.Now()

	if exist := q.metadata.ExistGroup(queue, group); !exist {
		return errors.NotFoundf("queue : %q , group: %q", queue, group)
	}

	owner := fmt.Sprintf("%s@%s", queue, group)
	q.mu.Lock()
	consumer, ok := q.consumerMap[owner]
	q.mu.Unlock()
	if !ok {
		return errors.NotFoundf("group consumer")
	}

	tokens := strings.Split(id, ":")
	if len(tokens) != 5 {
		return errors.NotValidf("message ID : %q", id)
	}

	partition, err := strconv.ParseInt(tokens[3], 16, 32)
	if err != nil {
		return errors.NotValidf("message ID : %q", id)
	}
	offset, err := strconv.ParseInt(tokens[4], 16, 64)
	if err != nil {
		return errors.NotValidf("message ID : %q", id)
	}

	err = consumer.Ack(int32(partition), offset)
	if err != nil {
		return errors.Trace(err)
	}

	cost := time.Now().Sub(start).Nanoseconds() / 1e6
	log.Debugf("ack %s:%s key nil id %s cost %d", queue, group, id, cost)
	return nil
}

func (q *queueImp) GetSendMetrics(queue string, group string,
	start int64, end int64, intervalnum int64) (metrics.MetricsObj, error) {

	if exist := q.metadata.ExistGroup(queue, group); !exist {
		return nil, errors.NotFoundf("GetSendMetrics queue : %q , group : %q", queue, group)
	}

	return q.monitor.GetSendMetrics(queue, group, start, end, intervalnum)
}

func (q *queueImp) GetReceiveMetrics(queue string, group string, start int64, end int64, intervalnum int64) (metrics.MetricsObj, error) {

	if exist := q.metadata.ExistGroup(queue, group); !exist {
		return nil, errors.NotFoundf("GetReceiveMetrics queue : %q , group : %q", queue, group)
	}

	return q.monitor.GetReceiveMetrics(queue, group, start, end, intervalnum)
}

func (q *queueImp) AccumulationStatus() ([]AccumulationInfo, error) {
	accumulationInfos := make([]AccumulationInfo, 0)
	err := q.metadata.RefreshMetadata()
	if err != nil {
		return nil, errors.Trace(err)
	}

	queueMap := q.metadata.GetQueueMap()
	for queue, groups := range queueMap {
		for _, group := range groups {
			total, consumed, err := q.metadata.Accumulation(queue, group)
			if err != nil {
				return nil, err
			}
			accumulationInfos = append(accumulationInfos, AccumulationInfo{
				Group:    group,
				Queue:    queue,
				Total:    total,
				Consumed: consumed,
			})
		}
	}
	return accumulationInfos, nil
}

func (q *queueImp) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()

	err := q.producer.Close()
	if err != nil {
		log.Errorf("close producer err: %s", err)
	}

	for name, consumer := range q.consumerMap {
		err = consumer.Close()
		if err != nil {
			log.Errorf("close consumer %s err: %s", name, err)
		}
		delete(q.consumerMap, name)
	}

	err = q.monitor.Close()
	if err != nil {
		log.Errorf("close monitor err: %s", err)
	}

	q.metadata.Close()
}
