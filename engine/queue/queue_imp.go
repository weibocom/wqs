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
	conf        *config.Config
	saramaConf  *sarama.Config
	manager     *kafka.Manager
	metadata    *Metadata
	producer    *kafka.Producer
	monitor     *metrics.Monitor
	idGenerator *idGenerator
	consumerMap map[string]*kafka.Consumer
	vaildName   *regexp.Regexp
	mu          sync.Mutex
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

	metadata, err := NewMetadata(strings.Split(config.MetaDataZKAddr, ","), config.MetaDataZKRoot)
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
	producer, err := kafka.NewProducer(strings.Split(config.KafkaBrokerAddr, ","), sConf)
	if err != nil {
		return nil, errors.Trace(err)
	}
	manager, err := kafka.NewManager(strings.Split(config.KafkaBrokerAddr, ","), config.KafkaLib, sConf)
	if err != nil {
		return nil, errors.Trace(err)
	}

	qs := &queueImp{
		conf:        config,
		saramaConf:  sConf,
		manager:     manager,
		metadata:    metadata,
		producer:    producer,
		monitor:     metrics.NewMonitor(config.RedisAddr),
		idGenerator: newIDGenerator(uint64(config.ProxyId)),
		vaildName:   regexp.MustCompile(`^[a-zA-Z0-9]{1,20}$`),
		consumerMap: make(map[string]*kafka.Consumer),
	}
	return qs, nil
}

//Create a queue by name.
func (q *queueImp) Create(queue string) error {
	// 1. check queue name valid
	if !q.vaildName.MatchString(queue) {
		return errors.NotValidf("queue : %q", queue)
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
	exist, err = q.metadata.ExistQueue(queue)
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
	if err = q.metadata.AddQueue(queue); err != nil {
		return errors.Trace(err)
	}
	return nil
}

//Updata queue information by name. Nothing to be update so far.
func (q *queueImp) Update(queue string) error {

	if !q.vaildName.MatchString(queue) {
		return errors.NotValidf("queue : %q", queue)
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
	if !q.vaildName.MatchString(queue) {
		return errors.NotValidf("queue : %q", queue)
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
	exist, err = q.metadata.ExistQueue(queue)
	if err != nil {
		return errors.Trace(err)
	}
	if !exist {
		return errors.NotFoundf("DeleteQueue queue:%s ", queue)
	}
	// 4. check metadata whether the queue has group
	can, err := q.metadata.CanDeleteQueue(queue)
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
	if err = q.metadata.DelQueue(queue); err != nil {
		return errors.Trace(err)
	}
	return nil
}

//Get queue information by queue name and group name
//When queue name is "" to get all queue' information.
func (q *queueImp) Lookup(queue string, group string) ([]*QueueInfo, error) {

	queueInfos := make([]*QueueInfo, 0)
	switch {
	case queue == "":
		//Get all queue's information
		queueMap, err := q.metadata.GetQueueMap()
		if err != nil {
			return queueInfos, errors.Trace(err)
		}
		for queueName, groupNames := range queueMap {
			groupConfigs := make([]*GroupConfig, 0)
			for _, groupName := range groupNames {
				config, err := q.metadata.GetGroupConfig(groupName, queueName)
				if err != nil {
					return queueInfos, errors.Trace(err)
				}
				if config != nil {
					groupConfigs = append(groupConfigs, &GroupConfig{
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

			ctime, _ := q.metadata.QueueCreateTime(queueName)
			queueInfos = append(queueInfos, &QueueInfo{
				Queue:  queueName,
				Ctime:  ctime,
				Length: 0,
				Groups: groupConfigs,
			})
		}
	case queue != "" && group == "":
		//Get a queue's all groups information
		queueMap, err := q.metadata.GetQueueMap()
		if err != nil {
			return queueInfos, errors.Trace(err)
		}
		groupNames, exists := queueMap[queue]
		if !exists {
			break
		}
		groupConfigs := make([]*GroupConfig, 0)
		for _, gName := range groupNames {
			config, err := q.metadata.GetGroupConfig(gName, queue)
			if err != nil {
				return queueInfos, errors.Trace(err)
			}
			if config != nil {
				groupConfigs = append(groupConfigs, &GroupConfig{
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

		ctime, _ := q.metadata.QueueCreateTime(queue)
		queueInfos = append(queueInfos, &QueueInfo{
			Queue:  queue,
			Ctime:  ctime,
			Length: 0,
			Groups: groupConfigs,
		})
	default:
		//Get group's information by queue and group's name
		config, err := q.metadata.GetGroupConfig(group, queue)
		if err != nil {
			return queueInfos, errors.Trace(err)
		}
		groupConfigs := make([]*GroupConfig, 0)
		if config != nil {
			groupConfigs = append(groupConfigs, &GroupConfig{
				Group: config.Group,
				Write: config.Write,
				Read:  config.Read,
				Url:   config.Url,
				Ips:   config.Ips,
			})
		}

		ctime, _ := q.metadata.QueueCreateTime(queue)
		queueInfos = append(queueInfos, &QueueInfo{
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

	if !q.vaildName.MatchString(group) || !q.vaildName.MatchString(queue) {
		return errors.NotValidf("group : %q , queue : %q", group, queue)
	}

	exist, err := q.metadata.ExistQueue(queue)
	if err != nil {
		return errors.Trace(err)
	}
	if !exist {
		return errors.NotFoundf("AddGroup queue:%s ", queue)
	}

	if err = q.metadata.AddGroupConfig(group, queue, write, read, url, ips); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (q *queueImp) UpdateGroup(group string, queue string,
	write bool, read bool, url string, ips []string) error {

	if !q.vaildName.MatchString(group) || !q.vaildName.MatchString(queue) {
		return errors.NotValidf("group : %q , queue : %q", group, queue)
	}

	exist, err := q.metadata.ExistQueue(queue)
	if err != nil {
		return errors.Trace(err)
	}
	if !exist {
		return errors.NotFoundf("UpdateGroup queue:%s ", queue)
	}

	if err = q.metadata.UpdateGroupConfig(group, queue, write, read, url, ips); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (q *queueImp) DeleteGroup(group string, queue string) error {

	if !q.vaildName.MatchString(group) || !q.vaildName.MatchString(queue) {
		return errors.NotValidf("group : %q , queue : %q", group, queue)
	}

	exist, err := q.metadata.ExistQueue(queue)
	if err != nil {
		return errors.Trace(err)
	}
	if !exist {
		return errors.NotFoundf("DeleteGroup queue:%s ", queue)
	}

	if err = q.metadata.DeleteGroupConfig(group, queue); err != nil {
		return errors.Trace(err)
	}

	return nil
}

//Get group's information
func (q *queueImp) LookupGroup(group string) ([]*GroupInfo, error) {

	groupInfos := make([]*GroupInfo, 0)

	if group == "" {
		//GET all groups' information
		groupMap, err := q.metadata.GetGroupMap()
		if err != nil {
			return groupInfos, errors.Trace(err)
		}
		for groupName, queueNames := range groupMap {
			groupConfigs := make([]*GroupConfig, 0)
			for _, queueName := range queueNames {
				config, err := q.metadata.GetGroupConfig(groupName, queueName)
				if err != nil {
					return groupInfos, errors.Trace(err)
				}
				if config != nil {
					groupConfigs = append(groupConfigs, &GroupConfig{
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
			groupInfos = append(groupInfos, &GroupInfo{
				Group:  groupName,
				Queues: groupConfigs,
			})
		}
	} else {
		//GET one group's information
		groupMap, err := q.metadata.GetGroupMap()
		if err != nil {
			return groupInfos, errors.Trace(err)
		}
		queueNames, exist := groupMap[group]
		if !exist {
			return groupInfos, nil
		}
		groupConfigs := make([]*GroupConfig, 0)
		for _, queue := range queueNames {
			config, err := q.metadata.GetGroupConfig(group, queue)
			if err != nil {
				return groupInfos, errors.Trace(err)
			}
			if config != nil {
				groupConfigs = append(groupConfigs, &GroupConfig{
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
		groupInfos = append(groupInfos, &GroupInfo{
			Group:  group,
			Queues: groupConfigs,
		})
	}
	return groupInfos, nil
}

func (q *queueImp) GetSingleGroup(group string, queue string) (*GroupConfig, error) {

	exist, err := q.manager.ExistTopic(queue, true)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !exist {
		return nil, errors.NotFoundf("GetSingleGroup queue:%s ", queue)
	}

	return q.metadata.GetGroupConfig(group, queue)
}

func (q *queueImp) SendMessage(queue string, group string, data []byte, flag uint64) (string, error) {
	start := time.Now()
	//TODO refer metadata
	exist, err := q.manager.ExistTopic(queue, false)
	if err != nil {
		return "", errors.Trace(err)
	}
	if !exist {
		return "", errors.NotFoundf("queue : %q ", queue)
	}

	sequenceID := q.idGenerator.Get()
	key := fmt.Sprintf("%x:%x", sequenceID, flag)

	partition, offset, err := q.producer.Send(queue, []byte(key), data)
	if err != nil {
		return "", errors.Trace(err)
	}
	messageID := fmt.Sprintf("%x:%s:%s:%x:%x", sequenceID, queue, group, partition, offset)
	cost := time.Now().Sub(start).Nanoseconds() / 1e6
	metrics.StatisticSend(queue, group, cost)
	q.monitor.StatisticSend(queue, group, 1)
	log.Debugf("send %s:%s key %s id %s cost %d", queue, group, key, messageID, cost)
	return messageID, nil
}

func (q *queueImp) RecvMessage(queue string, group string) (string, []byte, uint64, error) {
	start := time.Now()
	//TODO refer metadata
	exist, err := q.manager.ExistTopic(queue, false)
	if err != nil {
		return "", nil, 0, errors.Trace(err)
	}
	if !exist {
		return "", nil, 0, errors.NotFoundf("ReceiveMsg queue: %s ", queue)
	}

	owner := fmt.Sprintf("%s@%s", queue, group)
	q.mu.Lock()
	consumer, ok := q.consumerMap[owner]
	if !ok {
		consumer, err = kafka.NewConsumer(strings.Split(q.conf.KafkaBrokerAddr, ","), queue, group)
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
	metrics.StatisticRecv(queue, group, cost)
	q.monitor.StatisticReceive(queue, group, 1)
	log.Debugf("recv %s:%s key %s id %s cost %d delay %d", queue, group, string(msg.Key), messageID, cost, delay)
	return messageID, msg.Value, flag, nil
}

func (q *queueImp) AckMessage(queue string, group string) error {
	return errors.NotImplementedf("ack")
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

func (q *queueImp) AccumulationStatus() ([]AccumulationInfo, error) {
	accumulationInfos := make([]AccumulationInfo, 0)
	queueMap, err := q.metadata.GetQueueMap()
	if err != nil {
		return nil, err
	}
	for queue, groups := range queueMap {
		for _, group := range groups {
			total, consumed, err := q.manager.Accumulation(queue, group)
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

	err = q.manager.Close()
	if err != nil {
		log.Errorf("close manager err: %s", err)
	}

	q.metadata.Close()
}
