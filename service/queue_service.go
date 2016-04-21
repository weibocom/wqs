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
	"github.com/weibocom/wqs/config"
	"github.com/weibocom/wqs/engine/kafka"
	"github.com/weibocom/wqs/metrics"
	"github.com/weibocom/wqs/model"

	log "github.com/cihub/seelog"
	"github.com/juju/errors"
)

type QueueService interface {
	CreateQueue(queue string) error
	UpdateQueue(queue string) error
	DeleteQueue(queue string) error
	LookupQueue(queue string, group string) ([]*model.QueueInfo, error)
	AddGroup(group string, queue string, write bool, read bool, url string, ips []string) error
	UpdateGroup(group string, queue string, write bool, read bool, url string, ips []string) error
	DeleteGroup(group string, queue string) error
	LookupGroup(group string) ([]*model.GroupInfo, error)
	GetSingleGroup(group string, queue string) (*model.GroupConfig, error)
	SendMsg(queue string, group string, data []byte) error
	ReceiveMsg(queue string, group string) (data []byte, err error)
	AckMsg(queue string, group string) error
	GetSendMetrics(queue string, group string, start int64, end int64, intervalnum int) map[string][]int64
	GetReceiveMetrics(queue string, group string, start int64, end int64, intervalnum int) map[string][]int64
}

type queueService struct {
	config        *config.Config
	monitor       *metrics.Monitor
	manager       *kafka.KafkaManager
	producer      *kafka.KafkaProducer
	consumerMap   map[string]kafka.KafkaConsumer
	extendManager *kafka.ExtendManager
}

func NewQueueService(config *config.Config) QueueService {
	qs := &queueService{
		config:        config,
		monitor:       metrics.NewMonitor(config.RedisAddr),
		manager:       kafka.NewKafkaManager(config),
		producer:      kafka.NewKafkaProducer(config.BrokerAddr),
		consumerMap:   make(map[string]kafka.KafkaConsumer),
		extendManager: kafka.NewExtendManager(config),
	}
	qs.monitor.Start()
	return qs
}

//========队列操作相关函数========//

func (q *queueService) CreateQueue(queue string) error {
	q.extendManager.AddQueue(queue)
	if !q.manager.CreateTopic(queue, q.config.ReplicationsNum,
		q.config.PartitionsNum) {
		return errors.Errorf("create topic %s failed.", queue)
	}
	return nil
}

//暂时没有什么可以update的
func (q *queueService) UpdateQueue(queue string) error {
	return nil
}

func (q *queueService) DeleteQueue(queue string) error {
	// if !this.manager.ExistTopic(queue) {
	// 	return false
	// }
	queuemap := q.extendManager.GetQueueMap()
	groups, ok := queuemap[queue]
	if ok {
		for _, group := range groups {
			q.extendManager.DeleteGroupConfig(group, queue)
			q.manager.DeleteGroupTopic(group, queue)
		}
	}
	q.extendManager.DelQueue(queue)
	if !q.manager.DeleteTopic(queue) {
		return errors.Errorf("delete topic %s failed.", queue)
	}
	return nil
}

func (q *queueService) LookupQueue(queue string,
	group string) ([]*model.QueueInfo, error) {

	queueInfos := make([]*model.QueueInfo, 0)
	groupConfigs := make([]*model.GroupConfig, 0)
	if queue == "" {
		queueMap := q.extendManager.GetQueueMap()
		for qName, gs := range queueMap {

			for _, g := range gs {
				config := q.extendManager.GetGroupConfig(g, qName)
				if config != nil {
					groupConfigs = append(groupConfigs, &model.GroupConfig{
						Group: config.Group,
						Write: config.Write,
						Read:  config.Read,
						Url:   config.Url,
						Ips:   config.Ips,
					})
				} else {
					log.Errorf("config is nil group:%s, queue:%s", g, qName)
				}
			}
			ctime := q.manager.GetTopicCreateTime(qName)
			queueInfos = append(queueInfos, &model.QueueInfo{
				Queue:  qName,
				Ctime:  ctime,
				Length: 0,
				Groups: groupConfigs,
			})
		}
	} else if queue != "" && group == "" {
		queueMap := q.extendManager.GetQueueMap()
		groups := queueMap[queue]
		for _, g := range groups {
			config := q.extendManager.GetGroupConfig(g, queue)
			if config != nil {
				groupConfigs = append(groupConfigs, &model.GroupConfig{
					Group: config.Group,
					Write: config.Write,
					Read:  config.Read,
					Url:   config.Url,
					Ips:   config.Ips,
				})
			} else {
				log.Errorf("config is nil group:%s, queue:%s", g, queue)
			}
		}
		ctime := q.manager.GetTopicCreateTime(queue)
		queueInfos = append(queueInfos, &model.QueueInfo{
			Queue:  queue,
			Ctime:  ctime,
			Length: 0,
			Groups: groupConfigs,
		})
	} else {
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
		ctime := q.manager.GetTopicCreateTime(queue)
		queueInfos = append(queueInfos, &model.QueueInfo{
			Queue:  queue,
			Ctime:  ctime,
			Length: 0,
			Groups: groupConfigs,
		})
	}
	return queueInfos, nil
}

//========业务操作相关函数========//

func (q *queueService) AddGroup(group string, queue string,
	write bool, read bool, url string, ips []string) error {
	// if !this.manager.ExistTopic(queue) {
	// 	return false
	// }
	// 这是一个比较ugly的做法，注册一个新的消费方时，在zk上生成offset
	// tempConsumer := kafka.NewKafkaConsumer(queue, biz, this.config)
	// tempConsumer.Connect()
	// tempConsumer.Get()
	// tempConsumer.Disconnect()
	if !q.extendManager.AddGroupConfig(group, queue,
		write, read, url, ips) {
		return errors.Errorf("AddGroupConfig error")
	}
	return nil
}

func (q *queueService) UpdateGroup(group string, queue string,
	write bool, read bool, url string, ips []string) error {
	// if !this.manager.ExistTopic(queue) {
	// 	return false
	// }
	if !q.extendManager.UpdateGroupConfig(group, queue,
		write, read, url, ips) {
		return errors.Errorf("UpdateGroupConfig error")
	}
	return nil
}

func (q *queueService) DeleteGroup(group string, queue string) error {
	// if !this.manager.ExistTopic(queue) {
	// 	return false
	// }
	q.manager.DeleteGroupTopic(group, queue)
	// if len(this.manager.GetGroupTopics(biz)) == 0 {
	// 	this.manager.DeleteGroup(biz)
	// }
	if !q.extendManager.DeleteGroupConfig(group, queue) {
		return errors.Errorf("DeleteGroupConfig failed.")
	}
	return nil
}

func (q *queueService) LookupGroup(group string) ([]*model.GroupInfo, error) {
	groupInfos := make([]*model.GroupInfo, 0)
	groupConfigs := make([]*model.GroupConfig, 0)
	if group == "" {
		groupMap := q.extendManager.GetGroupMap()
		for gName, qs := range groupMap {
			for _, qName := range qs {
				config := q.extendManager.GetGroupConfig(gName, qName)
				if config != nil {
					groupConfigs = append(groupConfigs, &model.GroupConfig{
						Queue: config.Queue,
						Write: config.Write,
						Read:  config.Read,
						Url:   config.Url,
						Ips:   config.Ips,
					})
				} else {
					log.Errorf("config is nil group:%s, queue:%s", gName, qName)
				}
			}
			groupInfos = append(groupInfos, &model.GroupInfo{
				Group:  gName,
				Queues: groupConfigs,
			})
		}
	} else {
		groupMap := q.extendManager.GetGroupMap()
		queues := groupMap[group]
		for _, queue := range queues {
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
				log.Errorf("config is nil group:%s, queue:%s", group, queue)
			}
		}
		groupInfos = append(groupInfos, &model.GroupInfo{
			Group:  group,
			Queues: groupConfigs,
		})
	}
	return groupInfos, nil
}

func (q *queueService) GetSingleGroup(group string, queue string) (*model.GroupConfig, error) {
	return q.extendManager.GetGroupConfig(group, queue), nil
}

//========消息操作相关函数========//

func (q *queueService) SendMsg(queue string, group string, data []byte) error {
	var err error
	// if !this.manager.ExistTopic(queue) {
	// 	err = errors.New("topic not exist!")
	// } else {
	err = q.producer.Set(queue, data)
	if err == nil {
		go q.monitor.StatisticSend(queue, group, 1)
	}
	// }
	return err
}

func (q *queueService) ReceiveMsg(queue string, group string) (data []byte, err error) {
	// if !this.manager.ExistTopic(queue) {
	// 	err = errors.New("topic not exist!")
	// 	data = nil
	// } else {
	id := queue + group
	consumer, ok := q.consumerMap[id]
	if ok {
		if !consumer.IsConnected {
			consumer.Connect()
		}
		log.Debugf("msg receive, queue:%s, group:%s, consumer exists", queue, group)
		data, err = consumer.Get()

	} else {
		log.Debugf("msg receive, queue:%s, group:%s, create new consumer", queue, group)
		newConsumer := kafka.NewKafkaConsumer(queue, group, q.config)
		newConsumer.Connect()
		q.consumerMap[id] = *newConsumer
		data, err = newConsumer.Get()
	}
	if err == nil {
		go q.monitor.StatisticReceive(queue, group, 1)
	}
	// }
	return
}

func (q *queueService) AckMsg(queue string, group string) error {
	return nil
}

//========监控操作相关函数========//

func (q *queueService) GetSendMetrics(queue string, group string,
	start int64, end int64, intervalnum int) map[string][]int64 {
	if !q.manager.ExistTopic(queue) {
		return nil
	}
	return q.monitor.GetSendMetrics(queue, group, start, end, intervalnum)
}

func (q *queueService) GetReceiveMetrics(queue string, group string,
	start int64, end int64, intervalnum int) map[string][]int64 {
	if !q.manager.ExistTopic(queue) {
		return nil
	}
	return q.monitor.GetReceiveMetrics(queue, group, start, end, intervalnum)
}
