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

package service

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/weibocom/wqs/config"
	"github.com/weibocom/wqs/engine/kafka0.9"
	"github.com/weibocom/wqs/metrics"
	"github.com/weibocom/wqs/model"

	"github.com/Shopify/sarama"
	log "github.com/cihub/seelog"
	"github.com/juju/errors"
)

type queueService09 struct {
	conf          *config.Config
	saramaConf    *sarama.Config
	client        sarama.Client
	manager       *kafka.Manager
	extendManager *kafka.ExtendManager
	producer      *kafka.Producer
	monitor       *metrics.Monitor
	consumerMap   map[string]*kafka.Consumer
	mu            sync.Mutex
}

func newQueueService09(config *config.Config) (*queueService09, error) {

	hostname, err := os.Hostname()
	if err != nil {
		return nil, errors.Trace(err)
	}
	sConf := sarama.NewConfig()
	sConf.Net.KeepAlive = 30 * time.Second
	sConf.Metadata.Retry.Backoff = 100 * time.Millisecond
	sConf.Metadata.Retry.Max = 5
	sConf.Metadata.RefreshFrequency = 3 * time.Minute
	sConf.Producer.RequiredAcks = sarama.WaitForLocal
	//conf.Producer.RequiredAcks = sarama.NoResponse //this one high performance than WaitForLocal
	sConf.Producer.Partitioner = sarama.NewRandomPartitioner
	sConf.Producer.Flush.Frequency = time.Millisecond
	sConf.Producer.Flush.MaxMessages = 200
	sConf.ClientID = fmt.Sprintf("%d..%s", os.Getpid(), hostname)
	sConf.ChannelBufferSize = 1024

	client, err := sarama.NewClient(strings.Split(config.BrokerAddr, ","), sConf)
	if err != nil {
		return nil, errors.Trace(err)
	}
	extendManager, err := kafka.NewExtendManager(strings.Split(config.ZookeeperAddr, ","), config.ZookeeperRootPath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	producer, err := kafka.NewProducer(client)
	if err != nil {
		return nil, errors.Trace(err)
	}

	qs := &queueService09{
		conf:          config,
		saramaConf:    sConf,
		client:        client,
		manager:       kafka.NewManager(client, config.KafkaBin),
		extendManager: extendManager,
		producer:      producer,
		monitor:       metrics.NewMonitor(config.RedisAddr),
		consumerMap:   make(map[string]*kafka.Consumer),
	}
	return qs, nil
}

//Create a queue by name.
func (q *queueService09) CreateQueue(queue string) error {

	if len(queue) == 0 {
		errors.NotValidf("CreateQueue queue:%s", queue)
	}
	exist, err := q.manager.ExistTopic(queue, true)
	if err != nil {
		return errors.Trace(err)
	}
	if exist {
		return errors.AlreadyExistsf("CreateQueue queue:%s ", queue)
	}

	if !q.extendManager.AddQueue(queue) {
		return errors.Errorf("extern manager add queue %s failed.", queue)
	}
	return q.manager.CreateTopic(queue, q.conf.ReplicationsNum,
		q.conf.PartitionsNum, q.conf.ZookeeperAddr)
}

//Updata queue information by name. Nothing to be update so far.
func (q *queueService09) UpdateQueue(queue string) error {

	if len(queue) == 0 {
		errors.NotValidf("UpdateQueue queue:%s", queue)
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
func (q *queueService09) DeleteQueue(queue string) error {

	if len(queue) == 0 {
		errors.NotValidf("DeleteQueue queue:%s", queue)
	}
	exist, err := q.manager.ExistTopic(queue, true)
	if err != nil {
		return errors.Trace(err)
	}
	if !exist {
		return errors.NotFoundf("DeleteQueue queue:%s ", queue)
	}

	if !q.extendManager.DelQueue(queue) {
		return errors.Errorf("extern manager del queue %s failed.", queue)
	}
	return q.manager.DeleteTopic(queue, q.conf.ZookeeperAddr)
}

//Get queue information by queue name and group name
//When queue name is "" to get all queue' information.
func (q *queueService09) LookupQueue(queue string, group string) ([]*model.QueueInfo, error) {

	queueInfos := make([]*model.QueueInfo, 0)
	groupConfigs := make([]*model.GroupConfig, 0)
	switch {
	case queue == "":
		//Get all queue's information
		queueMap := q.extendManager.GetQueueMap()
		for queueName, groupNames := range queueMap {
			for _, groupName := range groupNames {
				config := q.extendManager.GetGroupConfig(groupName, queueName)
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
			//FIXME
			//ctime := q.manager.GetTopicCreateTime(queueName)
			ctime := int64(0)
			queueInfos = append(queueInfos, &model.QueueInfo{
				Queue:  queueName,
				Ctime:  ctime,
				Length: 0,
				Groups: groupConfigs,
			})
		}
	case queue != "" && group == "":
		//Get a queue's all groups information
		queueMap := q.extendManager.GetQueueMap()
		groupNames, exists := queueMap[queue]
		if !exists {
			break
		}
		for _, gName := range groupNames {
			config := q.extendManager.GetGroupConfig(gName, queue)
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
		//FIXME
		//ctime := q.manager.GetTopicCreateTime(queue)
		ctime := int64(0)
		queueInfos = append(queueInfos, &model.QueueInfo{
			Queue:  queue,
			Ctime:  ctime,
			Length: 0,
			Groups: groupConfigs,
		})
	default:
		//Get group's information by queue and group's name
		config := q.extendManager.GetGroupConfig(group, queue)
		if config != nil {
			groupConfigs = append(groupConfigs, &model.GroupConfig{
				Group: config.Group,
				Write: config.Write,
				Read:  config.Read,
				Url:   config.Url,
				Ips:   config.Ips,
			})
		}
		//FIXME
		//ctime := q.manager.GetTopicCreateTime(queue)
		ctime := int64(0)
		queueInfos = append(queueInfos, &model.QueueInfo{
			Queue:  queue,
			Ctime:  ctime,
			Length: 0,
			Groups: groupConfigs,
		})
	}
	return queueInfos, nil
}

func (q *queueService09) AddGroup(group string, queue string,
	write bool, read bool, url string, ips []string) error {

	if len(group) == 0 || len(queue) == 0 {
		errors.NotValidf("add group:%s @ queue:%s", group, queue)
	}

	exist, err := q.manager.ExistTopic(queue, true)
	if err != nil {
		return errors.Trace(err)
	}
	if !exist {
		return errors.NotFoundf("AddGroup queue:%s ", queue)
	}

	if !q.extendManager.AddGroupConfig(group, queue, write, read, url, ips) {
		return errors.Errorf("AddGroupConfig group:%s @ queue:%s faild.", group, queue)
	}
	return nil
}

func (q *queueService09) UpdateGroup(group string, queue string,
	write bool, read bool, url string, ips []string) error {

	if len(group) == 0 || len(queue) == 0 {
		errors.NotValidf("add group:%s @ queue:%s", group, queue)
	}

	exist, err := q.manager.ExistTopic(queue, true)
	if err != nil {
		return errors.Trace(err)
	}
	if !exist {
		return errors.NotFoundf("UpdateGroup queue:%s ", queue)
	}

	if !q.extendManager.UpdateGroupConfig(group, queue, write, read, url, ips) {
		return errors.Errorf("UpdateGroupConfig group:%s @ queue:%s faild.", group, queue)
	}
	return nil
}

func (q *queueService09) DeleteGroup(group string, queue string) error {

	if len(group) == 0 || len(queue) == 0 {
		errors.NotValidf("add group:%s @ queue:%s", group, queue)
	}

	exist, err := q.manager.ExistTopic(queue, true)
	if err != nil {
		return errors.Trace(err)
	}
	if !exist {
		return errors.NotFoundf("DeleteGroup queue:%s ", queue)
	}

	if !q.extendManager.DeleteGroupConfig(group, queue) {
		return errors.Errorf("DeleteGroupConfig group:%s @ queue:%s faild.", group, queue)
	}

	return nil
}

//Get group's information
func (q *queueService09) LookupGroup(group string) ([]*model.GroupInfo, error) {

	groupInfos := make([]*model.GroupInfo, 0)
	groupConfigs := make([]*model.GroupConfig, 0)

	if group == "" {
		//GET all groups' information
		groupMap := q.extendManager.GetGroupMap()
		for groupName, queueNames := range groupMap {
			for _, queueName := range queueNames {
				config := q.extendManager.GetGroupConfig(groupName, queueName)
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
		groupMap := q.extendManager.GetGroupMap()
		queueNames, exist := groupMap[group]
		if !exist {
			return groupInfos, nil
		}

		for _, queue := range queueNames {
			config := q.extendManager.GetGroupConfig(group, queue)
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

func (q *queueService09) GetSingleGroup(group string, queue string) (*model.GroupConfig, error) {

	exist, err := q.manager.ExistTopic(queue, true)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !exist {
		return nil, errors.NotFoundf("GetSingleGroup queue:%s ", queue)
	}

	return q.extendManager.GetGroupConfig(group, queue), nil
}

func (q *queueService09) SendMsg(queue string, group string, data []byte) error {

	exist, err := q.manager.ExistTopic(queue, false)
	if err != nil {
		return errors.Trace(err)
	}
	if !exist {
		return errors.NotFoundf("SendMsg queue:%s ", queue)
	}
	err = q.producer.Send(queue, data)
	if err != nil {
		return errors.Trace(err)
	}
	q.monitor.StatisticSend(queue, group, 1)
	return nil
}

func (q *queueService09) ReceiveMsg(queue string, group string) ([]byte, error) {

	exist, err := q.manager.ExistTopic(queue, false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !exist {
		return nil, errors.NotFoundf("ReceiveMsg queue:%s ", queue)
	}

	id := fmt.Sprintf("%s@%s", queue, group)
	q.mu.Lock()
	consumer, ok := q.consumerMap[id]
	if !ok {
		consumer, err = kafka.NewConsumer(strings.Split(q.conf.BrokerAddr, ","), queue, group)
		if err != nil {
			q.mu.Unlock()
			return nil, errors.Trace(err)
		}
		q.consumerMap[id] = consumer
	}
	q.mu.Unlock()

	data, err := consumer.Recv()
	if err != nil {
		return nil, errors.Trace(err)
	}
	q.monitor.StatisticReceive(queue, group, 1)
	return data, nil
}

func (q *queueService09) AckMsg(queue string, group string) error {
	return errors.NotImplementedf("ack")
}

func (q *queueService09) GetSendMetrics(queue string, group string,
	start int64, end int64, intervalnum int) (metrics.MetricsObj, error) {

	exist, err := q.manager.ExistTopic(queue, true)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !exist {
		return nil, errors.NotFoundf("GetSendMetrics queue:%s ", queue)
	}

	return q.monitor.GetSendMetrics(queue, group, start, end, intervalnum)
}

func (q *queueService09) GetReceiveMetrics(queue string, group string, start int64, end int64, intervalnum int) (metrics.MetricsObj, error) {

	exist, err := q.manager.ExistTopic(queue, true)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !exist {
		return nil, errors.NotFoundf("GetReceiveMetrics queue:%s ", queue)
	}

	return q.monitor.GetReceiveMetrics(queue, group, start, end, intervalnum)
}
