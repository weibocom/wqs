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
	log "github.com/Sirupsen/logrus"

	"github.com/weibocom/wqs/config"
	"github.com/weibocom/wqs/engine/kafka"
	"github.com/weibocom/wqs/metrics"
	"github.com/weibocom/wqs/model"
)

type QueueService struct {
	config        *config.Config
	monitor       *metrics.Monitor
	manager       *kafka.KafkaManager
	producer      *kafka.KafkaProducer
	consumerMap   map[string]kafka.KafkaConsumer
	extendManager *kafka.ExtendManager
}

func NewQueueService(config *config.Config) *QueueService {
	queueService := QueueService{}
	queueService.config = config
	queueService.monitor = metrics.NewMonitor(config.RedisAddr)
	queueService.manager = kafka.NewKafkaManager(config)
	queueService.producer = kafka.NewKafkaProducer(config.BrokerAddr)
	queueService.consumerMap = make(map[string]kafka.KafkaConsumer)
	queueService.extendManager = kafka.NewExtendManager(config)
	queueService.monitor.Start()
	return &queueService
}

//========队列操作相关函数========//

func (this *QueueService) CreateQueue(queue string) bool {
	this.extendManager.AddQueue(queue)
	return this.manager.CreateTopic(queue, this.config.ReplicationsNum, this.config.PartitionsNum)
}

//暂时没有什么可以update的
func (this *QueueService) UpdateQueue(queue string) bool {
	return false
}

func (this *QueueService) DeleteQueue(queue string) bool {
	// if !this.manager.ExistTopic(queue) {
	// 	return false
	// }
	queuemap := this.extendManager.GetQueueMap()
	groups, ok := queuemap[queue]
	if ok {
		for _, group := range groups {
			this.extendManager.DeleteGroupConfig(group, queue)
			this.manager.DeleteGroupTopic(group, queue)
		}
	}
	this.extendManager.DelQueue(queue)
	return this.manager.DeleteTopic(queue)
}

func (this *QueueService) LookupQueue(queue string, group string) []*model.QueueInfo {
	var queueInfos []*model.QueueInfo = make([]*model.QueueInfo, 0)
	if queue == "" {
		queueMap := this.extendManager.GetQueueMap()
		for q, gs := range queueMap {
			var groupConfigs []*model.GroupConfig = make([]*model.GroupConfig, 0)
			for _, g := range gs {
				config := this.extendManager.GetGroupConfig(g, q)
				if config != nil {
					groupConfigs = append(groupConfigs, &model.GroupConfig{Group: config.Group, Write: config.Write, Read: config.Read, Url: config.Url, Ips: config.Ips})
				} else {
					log.Errorf("config is nil group:%s, queue:%s", g, q)
				}
			}
			ctime := this.manager.GetTopicCreateTime(q)
			queueInfos = append(queueInfos, &model.QueueInfo{Queue: q, Ctime: ctime, Length: 0, Groups: groupConfigs})
		}
	} else if queue != "" && group == "" {
		queueMap := this.extendManager.GetQueueMap()
		groups := queueMap[queue]
		var groupConfigs []*model.GroupConfig = make([]*model.GroupConfig, 0)
		for _, g := range groups {
			config := this.extendManager.GetGroupConfig(g, queue)
			if config != nil {
				groupConfigs = append(groupConfigs, &model.GroupConfig{Group: config.Group, Write: config.Write, Read: config.Read, Url: config.Url, Ips: config.Ips})
			} else {
				log.Errorf("config is nil group:%s, queue:%s", g, queue)
			}
		}
		ctime := this.manager.GetTopicCreateTime(queue)
		queueInfos = append(queueInfos, &model.QueueInfo{Queue: queue, Ctime: ctime, Length: 0, Groups: groupConfigs})
	} else {
		var groupConfigs []*model.GroupConfig = make([]*model.GroupConfig, 0)
		config := this.extendManager.GetGroupConfig(group, queue)
		if config != nil {
			groupConfigs = append(groupConfigs, &model.GroupConfig{Group: config.Group, Write: config.Write, Read: config.Read, Url: config.Url, Ips: config.Ips})
		}
		ctime := this.manager.GetTopicCreateTime(queue)
		queueInfos = append(queueInfos, &model.QueueInfo{Queue: queue, Ctime: ctime, Length: 0, Groups: groupConfigs})
	}
	return queueInfos
}

//========业务操作相关函数========//

func (this *QueueService) AddGroup(group string, queue string, write bool, read bool, url string, ips []string) bool {
	// if !this.manager.ExistTopic(queue) {
	// 	return false
	// }
	// 这是一个比较ugly的做法，注册一个新的消费方时，在zk上生成offset
	// tempConsumer := kafka.NewKafkaConsumer(queue, biz, this.config)
	// tempConsumer.Connect()
	// tempConsumer.Get()
	// tempConsumer.Disconnect()
	return this.extendManager.AddGroupConfig(group, queue, write, read, url, ips)
}

func (this *QueueService) UpdateGroup(group string, queue string, write bool, read bool, url string, ips []string) bool {
	// if !this.manager.ExistTopic(queue) {
	// 	return false
	// }
	return this.extendManager.UpdateGroupConfig(group, queue, write, read, url, ips)
}

func (this *QueueService) DeleteGroup(group string, queue string) bool {
	// if !this.manager.ExistTopic(queue) {
	// 	return false
	// }
	this.manager.DeleteGroupTopic(group, queue)
	// if len(this.manager.GetGroupTopics(biz)) == 0 {
	// 	this.manager.DeleteGroup(biz)
	// }
	return this.extendManager.DeleteGroupConfig(group, queue)
}

func (this *QueueService) LookupGroup(group string) []*model.GroupInfo {
	var groupInfos []*model.GroupInfo = make([]*model.GroupInfo, 0)
	if group == "" {
		groupMap := this.extendManager.GetGroupMap()
		for g, qs := range groupMap {
			var groupConfigs []*model.GroupConfig = make([]*model.GroupConfig, 0)
			for _, q := range qs {
				config := this.extendManager.GetGroupConfig(g, q)
				if config != nil {
					groupConfigs = append(groupConfigs, &model.GroupConfig{Queue: config.Queue, Write: config.Write, Read: config.Read, Url: config.Url, Ips: config.Ips})
				} else {
					log.Errorf("config is nil group:%s, queue:%s", g, q)
				}
			}
			groupInfos = append(groupInfos, &model.GroupInfo{Group: g, Queues: groupConfigs})
		}
	} else {
		groupMap := this.extendManager.GetGroupMap()
		queues := groupMap[group]
		var groupConfigs []*model.GroupConfig = make([]*model.GroupConfig, 0)
		for _, queue := range queues {
			config := this.extendManager.GetGroupConfig(group, queue)
			if config != nil {
				groupConfigs = append(groupConfigs, &model.GroupConfig{Queue: config.Queue, Write: config.Write, Read: config.Read, Url: config.Url, Ips: config.Ips})
			} else {
				log.Errorf("config is nil group:%s, queue:%s", group, queue)
			}
		}
		groupInfos = append(groupInfos, &model.GroupInfo{Group: group, Queues: groupConfigs})
	}
	return groupInfos
}

func (this *QueueService) GetSingleGroup(group string, queue string) *model.GroupConfig {
	return this.extendManager.GetGroupConfig(group, queue)
}

//========消息操作相关函数========//

func (this *QueueService) SendMsg(queue string, group string, data []byte) error {
	var err error
	// if !this.manager.ExistTopic(queue) {
	// 	err = errors.New("topic not exist!")
	// } else {
	err = this.producer.Set(queue, data)
	if err == nil {
		go this.monitor.StatisticSend(queue, group, 1)
	}
	// }
	return err
}

func (this *QueueService) ReceiveMsg(queue string, group string) (data []byte, err error) {
	// if !this.manager.ExistTopic(queue) {
	// 	err = errors.New("topic not exist!")
	// 	data = nil
	// } else {
	id := queue + group
	consumer, ok := this.consumerMap[id]
	if ok {
		if !consumer.IsConnected {
			consumer.Connect()
		}
		log.Debugf("msg receive, queue:%s, group:%s, consumer exists", queue, group)
		data, err = consumer.Get()

	} else {
		log.Debugf("msg receive, queue:%s, group:%s, create new consumer", queue, group)
		newConsumer := kafka.NewKafkaConsumer(queue, group, this.config)
		newConsumer.Connect()
		this.consumerMap[id] = *newConsumer
		data, err = newConsumer.Get()
	}
	if err == nil {
		go this.monitor.StatisticReceive(queue, group, 1)
	}
	// }
	return
}

func (this *QueueService) AckMsg(queue string, group string) error {
	return nil
}

//========监控操作相关函数========//

func (this *QueueService) GetSendMetrics(queue string, group string, start int64, end int64, intervalnum int) map[string][]int64 {
	if !this.manager.ExistTopic(queue) {
		return nil
	}
	return this.monitor.GetSendMetrics(queue, group, start, end, intervalnum)
}

func (this *QueueService) GetReceiveMetrics(queue string, group string, start int64, end int64, intervalnum int) map[string][]int64 {
	if !this.manager.ExistTopic(queue) {
		return nil
	}
	return this.monitor.GetReceiveMetrics(queue, group, start, end, intervalnum)
}
