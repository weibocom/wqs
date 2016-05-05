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
	"strings"
	"sync"
	"time"

	"github.com/weibocom/wqs/config"
	"github.com/weibocom/wqs/engine/kafka"
	"github.com/weibocom/wqs/log"
	"github.com/weibocom/wqs/metrics"
	"github.com/weibocom/wqs/model"
	"github.com/weibocom/wqs/utils"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
)

type queueImp struct {
	conf          *config.Config
	saramaConf    *sarama.Config
	manager       *kafka.Manager
	extendManager *kafka.ExtendManager
	producer      *kafka.Producer
	monitor       *metrics.Monitor
	consumerMap   map[string]*kafka.Consumer
	mu            sync.Mutex
}

func newQueue(config *config.Config) (*queueImp, error) {

	hostname, err := os.Hostname()
	if err != nil {
		return nil, errors.Trace(err)
	}
	sConf := sarama.NewConfig()
	sConf.Net.KeepAlive = 30 * time.Second
	sConf.Metadata.Retry.Backoff = 100 * time.Millisecond
	sConf.Metadata.Retry.Max = 5
	sConf.Metadata.RefreshFrequency = 1 * time.Minute
	sConf.Producer.RequiredAcks = sarama.WaitForLocal
	//conf.Producer.RequiredAcks = sarama.NoResponse //this one high performance than WaitForLocal
	sConf.Producer.Partitioner = sarama.NewRandomPartitioner
	sConf.Producer.Flush.Frequency = time.Millisecond
	sConf.Producer.Flush.MaxMessages = 200
	sConf.ClientID = fmt.Sprintf("%d..%s", os.Getpid(), hostname)
	sConf.ChannelBufferSize = 1024

	extendManager, err := kafka.NewExtendManager(strings.Split(config.MetaDataZKAddr, ","), config.MetaDataZKRoot)
	if err != nil {
		return nil, errors.Trace(err)
	}
	producer, err := kafka.NewProducer(strings.Split(config.KafkaBrokerAddr, ","), sConf)
	if err != nil {
		return nil, errors.Trace(err)
	}
	manager, err := kafka.NewManager(strings.Split(config.KafkaBrokerAddr, ","), config.KafkaLib, sConf)
	if err != nil {
		return nil, errors.Trace(err)
	}

	qs := &queueImp{
		conf:          config,
		saramaConf:    sConf,
		manager:       manager,
		extendManager: extendManager,
		producer:      producer,
		monitor:       metrics.NewMonitor(config.RedisAddr),
		consumerMap:   make(map[string]*kafka.Consumer),
	}
	return qs, nil
}

//Create a queue by name.
func (q *queueImp) Create(queue string) error {
	// 1. check queue name valid
	if utils.BlankString(queue) {
		return errors.NotValidf("CreateQueue queue:%s", queue)
	}

	// 2. check kafka whether the queue exists
	exist, err := q.manager.ExistTopic(queue, true)
	if err != nil {
		return errors.Trace(err)
	}
	if exist {
		return errors.AlreadyExistsf("CreateQueue queue:%s ", queue)
	}

	// 3. check metadata whether the queue exists
	exist, err = q.extendManager.ExistQueue(queue)
	if err != nil {
		return errors.Trace(err)
	}
	if exist {
		return errors.AlreadyExistsf("CreateQueue queue:%s ", queue)
	}
	// 4. create kafka topic
	if err = q.manager.CreateTopic(queue, q.conf.KafkaReplications,
		q.conf.KafkaPartitions, q.conf.KafkaZKAddr); err != nil {
		return errors.Trace(err)
	}
	// 5. add metadata of queue
	if err = q.extendManager.AddQueue(queue); err != nil {
		return errors.Trace(err)
	}
	return nil
}

//Updata queue information by name. Nothing to be update so far.
func (q *queueImp) Update(queue string) error {

	if utils.BlankString(queue) {
		return errors.NotValidf("UpdateQueue queue:%s", queue)
	}
	exist, err := q.manager.ExistTopic(queue, true)
	if err != nil {
		return errors.Trace(err)
	}
	if !exist {
		return errors.NotFoundf("UpdateQueue queue:%s ", queue)
	}
	//TODO
	return nil
}

//Delete queue by name
func (q *queueImp) Delete(queue string) error {
	// 1. check queue name valid
	if utils.BlankString(queue) {
		return errors.NotValidf("DeleteQueue queue:%s", queue)
	}
	// 2. check kafka whether the queue exists
	exist, err := q.manager.ExistTopic(queue, true)
	if err != nil {
		return errors.Trace(err)
	}
	if !exist {
		return errors.NotFoundf("DeleteQueue queue:%s ", queue)
	}
	// 3. check metadata whether the queue exists
	exist, err = q.extendManager.ExistQueue(queue)
	if err != nil {
		return errors.Trace(err)
	}
	if !exist {
		return errors.NotFoundf("DeleteQueue queue:%s ", queue)
	}
	// 4. check metadata whether the queue has group
	can, err := q.extendManager.CanDeleteQueue(queue)
	if err != nil {
		return errors.Trace(err)
	}
	if !can {
		return errors.NotValidf("DeleteQueue queue:%s has one or more group", queue)
	}
	// 5. delete kafka topic
	if err = q.manager.DeleteTopic(queue, q.conf.KafkaZKAddr); err != nil {
		return errors.Trace(err)
	}
	// 6. delete metadata of queue
	if err = q.extendManager.DelQueue(queue); err != nil {
		return errors.Trace(err)
	}
	return nil
}

//Get queue information by queue name and group name
//When queue name is "" to get all queue' information.
func (q *queueImp) Lookup(queue string, group string) ([]*model.QueueInfo, error) {

	queueInfos := make([]*model.QueueInfo, 0)
	switch {
	case queue == "":
		//Get all queue's information
		queueMap, err := q.extendManager.GetQueueMap()
		if err != nil {
			return queueInfos, errors.Trace(err)
		}
		for queueName, groupNames := range queueMap {
			groupConfigs := make([]*model.GroupConfig, 0)
			for _, groupName := range groupNames {
				config, err := q.extendManager.GetGroupConfig(groupName, queueName)
				if err != nil {
					return queueInfos, errors.Trace(err)
				}
				if config != nil {
					groupConfigs = append(groupConfigs, &model.GroupConfig{
						Group: config.Group,
						Write: config.Write,
						Read:  config.Read,
						Url:   config.Url,
						Ips:   config.Ips,
					})
				} else {
					log.Warnf("config is nil queue:%s, group:%s", queueName, groupName)
				}
			}

			ctime, _ := q.extendManager.QueueCreateTime(queueName)
			queueInfos = append(queueInfos, &model.QueueInfo{
				Queue:  queueName,
				Ctime:  ctime,
				Length: 0,
				Groups: groupConfigs,
			})
		}
	case queue != "" && group == "":
		//Get a queue's all groups information
		queueMap, err := q.extendManager.GetQueueMap()
		if err != nil {
			return queueInfos, errors.Trace(err)
		}
		groupNames, exists := queueMap[queue]
		if !exists {
			break
		}
		groupConfigs := make([]*model.GroupConfig, 0)
		for _, gName := range groupNames {
			config, err := q.extendManager.GetGroupConfig(gName, queue)
			if err != nil {
				return queueInfos, errors.Trace(err)
			}
			if config != nil {
				groupConfigs = append(groupConfigs, &model.GroupConfig{
					Group: config.Group,
					Write: config.Write,
					Read:  config.Read,
					Url:   config.Url,
					Ips:   config.Ips,
				})
			} else {
				log.Warnf("config is nil queue:%s, group:%s", queue, gName)
			}
		}

		ctime, _ := q.extendManager.QueueCreateTime(queue)
		queueInfos = append(queueInfos, &model.QueueInfo{
			Queue:  queue,
			Ctime:  ctime,
			Length: 0,
			Groups: groupConfigs,
		})
	default:
		//Get group's information by queue and group's name
		config, err := q.extendManager.GetGroupConfig(group, queue)
		if err != nil {
			return queueInfos, errors.Trace(err)
		}
		groupConfigs := make([]*model.GroupConfig, 0)
		if config != nil {
			groupConfigs = append(groupConfigs, &model.GroupConfig{
				Group: config.Group,
				Write: config.Write,
				Read:  config.Read,
				Url:   config.Url,
				Ips:   config.Ips,
			})
		}

		ctime, _ := q.extendManager.QueueCreateTime(queue)
		queueInfos = append(queueInfos, &model.QueueInfo{
			Queue:  queue,
			Ctime:  ctime,
			Length: 0,
			Groups: groupConfigs,
		})
	}
	return queueInfos, nil
}

func (q *queueImp) AddGroup(group string, queue string,
	write bool, read bool, url string, ips []string) error {

	if utils.BlankString(group) || utils.BlankString(queue) {
		return errors.NotValidf("add group:%s @ queue:%s", group, queue)
	}

	exist, err := q.extendManager.ExistQueue(queue)
	if err != nil {
		return errors.Trace(err)
	}
	if !exist {
		return errors.NotFoundf("AddGroup queue:%s ", queue)
	}

	if err = q.extendManager.AddGroupConfig(group, queue, write, read, url, ips); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (q *queueImp) UpdateGroup(group string, queue string,
	write bool, read bool, url string, ips []string) error {

	if utils.BlankString(group) || utils.BlankString(queue) {
		return errors.NotValidf("update group:%s @ queue:%s", group, queue)
	}

	exist, err := q.extendManager.ExistQueue(queue)
	if err != nil {
		return errors.Trace(err)
	}
	if !exist {
		return errors.NotFoundf("UpdateGroup queue:%s ", queue)
	}

	if err = q.extendManager.UpdateGroupConfig(group, queue, write, read, url, ips); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (q *queueImp) DeleteGroup(group string, queue string) error {

	if utils.BlankString(group) || utils.BlankString(queue) {
		return errors.NotValidf("delete group:%s @ queue:%s", group, queue)
	}

	exist, err := q.extendManager.ExistQueue(queue)
	if err != nil {
		return errors.Trace(err)
	}
	if !exist {
		return errors.NotFoundf("DeleteGroup queue:%s ", queue)
	}

	if err = q.extendManager.DeleteGroupConfig(group, queue); err != nil {
		return errors.Trace(err)
	}

	return nil
}

//Get group's information
func (q *queueImp) LookupGroup(group string) ([]*model.GroupInfo, error) {

	groupInfos := make([]*model.GroupInfo, 0)

	if group == "" {
		//GET all groups' information
		groupMap, err := q.extendManager.GetGroupMap()
		if err != nil {
			return groupInfos, errors.Trace(err)
		}
		for groupName, queueNames := range groupMap {
			groupConfigs := make([]*model.GroupConfig, 0)
			for _, queueName := range queueNames {
				config, err := q.extendManager.GetGroupConfig(groupName, queueName)
				if err != nil {
					return groupInfos, errors.Trace(err)
				}
				if config != nil {
					groupConfigs = append(groupConfigs, &model.GroupConfig{
						Queue: config.Queue,
						Write: config.Write,
						Read:  config.Read,
						Url:   config.Url,
						Ips:   config.Ips,
					})
				} else {
					log.Warnf("config is nil group:%s, queue:%s", groupName, queueName)
				}
			}
			groupInfos = append(groupInfos, &model.GroupInfo{
				Group:  groupName,
				Queues: groupConfigs,
			})
		}
	} else {
		//GET one group's information
		groupMap, err := q.extendManager.GetGroupMap()
		if err != nil {
			return groupInfos, errors.Trace(err)
		}
		queueNames, exist := groupMap[group]
		if !exist {
			return groupInfos, nil
		}
		groupConfigs := make([]*model.GroupConfig, 0)
		for _, queue := range queueNames {
			config, err := q.extendManager.GetGroupConfig(group, queue)
			if err != nil {
				return groupInfos, errors.Trace(err)
			}
			if config != nil {
				groupConfigs = append(groupConfigs, &model.GroupConfig{
					Queue: config.Queue,
					Write: config.Write,
					Read:  config.Read,
					Url:   config.Url,
					Ips:   config.Ips,
				})
			} else {
				log.Warnf("config is nil group:%s, queue:%s", group, queue)
			}
		}
		groupInfos = append(groupInfos, &model.GroupInfo{
			Group:  group,
			Queues: groupConfigs,
		})
	}
	return groupInfos, nil
}

func (q *queueImp) GetSingleGroup(group string, queue string) (*model.GroupConfig, error) {

	exist, err := q.manager.ExistTopic(queue, true)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !exist {
		return nil, errors.NotFoundf("GetSingleGroup queue:%s ", queue)
	}

	return q.extendManager.GetGroupConfig(group, queue)
}

func (q *queueImp) SendMsg(queue string, group string, data []byte) (uint64, error) {
	start := time.Now()
	exist, err := q.manager.ExistTopic(queue, false)
	if err != nil {
		return uint64(0), errors.Trace(err)
	}
	if !exist {
		return uint64(0), errors.NotFoundf("SendMsg queue:%s ", queue)
	}
	id := q.genMsgId(queue, group)
	err = q.producer.Send(queue, utils.Uint64ToBytes(id), data)
	if err != nil {
		return uint64(0), errors.Trace(err)
	}

	cost := time.Now().Sub(start).Nanoseconds() / 1000000
	metrics.StatisticSend(queue, group, cost)
	q.monitor.StatisticSend(queue, group, 1)

	return id, nil
}

func (q *queueImp) RecvMsg(queue string, group string) (uint64, []byte, error) {
	start := time.Now()
	exist, err := q.manager.ExistTopic(queue, false)
	if err != nil {
		return uint64(0), nil, errors.Trace(err)
	}
	if !exist {
		return uint64(0), nil, errors.NotFoundf("ReceiveMsg queue:%s ", queue)
	}

	key := fmt.Sprintf("%s@%s", queue, group)
	q.mu.Lock()
	consumer, ok := q.consumerMap[key]
	if !ok {
		consumer, err = kafka.NewConsumer(strings.Split(q.conf.KafkaBrokerAddr, ","), queue, group)
		if err != nil {
			q.mu.Unlock()
			return uint64(0), nil, errors.Trace(err)
		}
		q.consumerMap[key] = consumer
	}
	q.mu.Unlock()

	id, data, err := consumer.Recv()
	if err != nil {
		return uint64(0), nil, errors.Trace(err)
	}

	cost := time.Now().Sub(start).Nanoseconds() / 1000000
	metrics.StatisticRecv(queue, group, cost)
	q.monitor.StatisticReceive(queue, group, 1)

	return utils.BytesToUint64(id), data, nil
}

func (q *queueImp) AckMsg(queue string, group string) error {
	return errors.NotImplementedf("ack")
}

func (q *queueImp) genMsgId(queue string, group string) uint64 {
	//TODO: generate real msg id
	return uint64(1234567890)
}

func (q *queueImp) GetSendMetrics(queue string, group string,
	start int64, end int64, intervalnum int64) (metrics.MetricsObj, error) {

	exist, err := q.manager.ExistTopic(queue, true)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !exist {
		return nil, errors.NotFoundf("GetSendMetrics queue:%s ", queue)
	}

	return q.monitor.GetSendMetrics(queue, group, start, end, intervalnum)
}

func (q *queueImp) GetReceiveMetrics(queue string, group string, start int64, end int64, intervalnum int64) (metrics.MetricsObj, error) {

	exist, err := q.manager.ExistTopic(queue, true)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !exist {
		return nil, errors.NotFoundf("GetReceiveMetrics queue:%s ", queue)
	}

	return q.monitor.GetReceiveMetrics(queue, group, start, end, intervalnum)
}
