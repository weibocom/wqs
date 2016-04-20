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
	"fmt"
	"os/exec"
	"runtime"
	"strconv"
	"strings"

	log "github.com/Sirupsen/logrus"

	"github.com/elodina/go_kafka_client"
	"github.com/weibocom/wqs/config"
	"github.com/weibocom/wqs/engine/zookeeper"
)

const (
	kafkaLibPath    = "."
	zkTopicsPath    = "/brokers/topics"
	zkConsumersPath = "/consumers"
)

type KafkaManager struct {
	zookeeperCoordinator *go_kafka_client.ZookeeperCoordinator
	config               *config.Config
	zkClient             *zookeeper.ZkClient
}

func NewKafkaManager(config *config.Config) *KafkaManager {
	kafkaManager := KafkaManager{}
	zkConfig := go_kafka_client.NewZookeeperConfig()
	zkConfig.ZookeeperConnect = strings.Split(config.ZookeeperAddr, ",")
	kafkaManager.config = config
	kafkaManager.zookeeperCoordinator = go_kafka_client.NewZookeeperCoordinator(zkConfig)
	kafkaManager.zookeeperCoordinator.Connect()
	kafkaManager.zkClient = zookeeper.NewZkClient(strings.Split(config.ZookeeperAddr, ","))
	return &kafkaManager
}

//========topic相关函数========//

//Convenience utility to create a topic topicName with numPartitions partitions in Zookeeper located at zk (format should be host:port).
//Please note that this requires Apache Kafka 0.8.1 binary distribution available through KAFKA_PATH environment variable
func (this *KafkaManager) CreateTopic(topic string, replications int, partitions int) bool {
	var out []byte
	var err error
	if runtime.GOOS == "windows" {
		params := fmt.Sprintf("--create --zookeeper %s --replication-factor %d --partitions %d --topic %s", this.config.ZookeeperAddr, replications, partitions, topic)
		script := fmt.Sprintf("%s\\kafka\\bin\\windows\\kafka-topics.bat %s", kafkaLibPath, params)
		out, err = exec.Command("cmd", "/C", script).Output()
	} else {
		params := fmt.Sprintf("--create --zookeeper %s --replication-factor %d --partitions %d --topic %s", this.config.ZookeeperAddr, replications, partitions, topic)
		script := fmt.Sprintf("%s/kafka/bin/kafka-topics.sh %s", kafkaLibPath, params)
		out, err = exec.Command("sh", "-c", script).Output()
	}
	log.Infof("create topic:%s, result:\n", topic, string(out))
	if err != nil {
		panic(err)
	}

	return true
}

//numReplication貌似不支持变更，此处参数不起作用
func (this *KafkaManager) UpdateTopic(topic string, replications int, partitions int) bool {
	if runtime.GOOS == "windows" {
		params := fmt.Sprintf("--alter --zookeeper %s --partitions %d --topic %s", this.config.ZookeeperAddr, partitions, topic)
		script := fmt.Sprintf("%s\\kafka\\bin\\windows\\kafka-topics.bat %s", kafkaLibPath, params)
		exec.Command("cmd", "/C", script).Output()
	} else {
		params := fmt.Sprintf("--alter --zookeeper %s --partitions %d --topic %s", this.config.ZookeeperAddr, partitions, topic)
		script := fmt.Sprintf("%s/kafka/bin/kafka-topics.sh %s", kafkaLibPath, params)
		exec.Command("sh", "-c", script).Output()
	}
	return true
}

//删除topic
func (this *KafkaManager) DeleteTopic(topic string) bool {
	var out []byte
	var err error
	if runtime.GOOS == "windows" {
		params := fmt.Sprintf("--delete --zookeeper %s --topic %s", this.config.ZookeeperAddr, topic)
		script := fmt.Sprintf("%s\\kafka\\bin\\windows\\kafka-topics.bat %s", kafkaLibPath, params)
		out, err = exec.Command("cmd", "/C", script).Output()
	} else {
		params := fmt.Sprintf("--delete --zookeeper %s --topic %s", this.config.ZookeeperAddr, topic)
		script := fmt.Sprintf("%s/kafka/bin/kafka-topics.sh %s", kafkaLibPath, params)
		out, err = exec.Command("sh", "-c", script).Output()
	}
	log.Infof("delete topic:%s, result:\n", topic, string(out))
	if err != nil {
		panic(err)
	}
	return true
}

//获取所有topic
func (this *KafkaManager) GetTopics() []string {
	topics, err := this.zookeeperCoordinator.GetAllTopics()
	if err != nil {
		log.Warnf("get topics error, err:%s", err)
	}
	return topics
}

//判断topic是否存在
func (this *KafkaManager) ExistTopic(topic string) bool {
	result := false
	topics, _ := this.zookeeperCoordinator.GetAllTopics()
	for _, t := range topics {
		if strings.EqualFold(t, topic) {
			result = true
			break
		}
	}
	return result
}

//该函数要慎用，耗时很长
func (this *KafkaManager) GetTopicLength(topic string) int64 {
	partitionIds := this.getTopicPartitionIds(topic)
	offsetMap := this.getPartitionLogSize(topic, partitionIds)
	size := int64(0)
	for _, v := range offsetMap {
		size += int64(v)
	}
	return size
}

func (this *KafkaManager) getTopicPartitionIds(topic string) []int32 {
	result, _ := this.zookeeperCoordinator.GetPartitionsForTopics([]string{topic})
	return result[topic]
}

func (this *KafkaManager) getPartitionLogSize(topic string, partitionIds []int32) map[string]int {
	partitions := strconv.Itoa(int(partitionIds[0]))
	for i := 1; i < len(partitionIds); i++ {
		partitions += "," + strconv.Itoa(int(partitionIds[i]))
	}
	var out []byte
	if runtime.GOOS == "windows" {
		params := fmt.Sprintf("kafka.tools.GetOffsetShell --broker-list %s --topic %s --partition %s --time %d", this.config.BrokerAddr, topic, partitions, -1)
		script := fmt.Sprintf("%s\\kafka\\bin\\windows\\kafka-run-class.bat %s", kafkaLibPath, params)
		out, _ = exec.Command("cmd", "/C", script).Output()
	} else {
		params := fmt.Sprintf("kafka.tools.GetOffsetShell --broker-list %s --topic %s --partition %s --time %d", this.config.BrokerAddr, topic, partitions, -1)
		script := fmt.Sprintf("%s/kafka/bin/kafka-run-class.sh %s", kafkaLibPath, params)
		fmt.Println(script)
		out, _ = exec.Command("sh", "-c", script).Output()
	}
	offsetList := strings.Split(string(out), "\n")
	offsetMap := make(map[string]int)
	for _, offset := range offsetList {
		if offset == "" {
			continue
		}
		offsetMap[strings.Split(offset, ":")[1]], _ = strconv.Atoi(strings.Split(offset, ":")[2])
	}
	return offsetMap
}

func (this *KafkaManager) GetTopicCreateTime(topic string) int64 {
	_, stat := this.zkClient.Get(zkTopicsPath + "/" + topic)
	return stat.Ctime / 1000
}

//获取所有brokers节点
func (this *KafkaManager) GetAllBrokers() []string {
	brokers, err := this.zookeeperCoordinator.GetAllBrokers()
	result := make([]string, len(brokers))
	for i, broker := range brokers {
		result[i] = broker.Host + ":" + strconv.Itoa(int(broker.Port))
	}
	if err != nil {
		log.Warnf("get all brokers error, err:%s", err)
	}
	return result
}

//========group相关函数========//

//获取全部group
func (this *KafkaManager) GetGroups() []string {
	groups, _ := this.zkClient.Children(zkConsumersPath)
	return groups
}

//获取某个group的topic
func (this *KafkaManager) GetGroupTopics(group string) []string {
	topics, _ := this.zkClient.Children(zkConsumersPath + "/" + group + "/owners")
	return topics
}

//删除一个group
func (this *KafkaManager) DeleteGroup(group string) bool {
	return this.zkClient.DeleteRec(zkConsumersPath + "/" + group)
}

//获取所有group对应的topic，返回map，key=group，value=[topic]
func (this *KafkaManager) GetAllGroupTopics() map[string][]string {
	topicWithGroupMap := make(map[string][]string)
	groups := this.GetGroups()
	for _, group := range groups {
		topicWithGroupMap[group] = this.GetGroupTopics(group)
	}
	return topicWithGroupMap
}

//删除某个group下的topic
func (this *KafkaManager) DeleteGroupTopic(group string, topic string) bool {
	owners := zkConsumersPath + "/" + group + "/owners/" + topic
	offsets := zkConsumersPath + "/" + group + "/offsets/" + topic
	if this.zkClient.Exists(owners) {
		this.zkClient.DeleteRec(owners)
	}
	if this.zkClient.Exists(offsets) {
		this.zkClient.DeleteRec(offsets)
	}
	return true
}

//获取某个group在某个topic的消费位置
func (this *KafkaManager) GetGroupTopicOffset(group string, topic string) map[int32]int64 {
	groupTopicOffset := make(map[int32]int64)
	partitionsIds := this.getTopicPartitionIds(topic)
	for _, id := range partitionsIds {
		offset, err := this.zookeeperCoordinator.GetOffset(group, topic, id)
		if err != nil {
			log.Warnf("get group topic offset err, group:%s, topic:%s, partition:%d, err:%s", group, topic, id, err)
			offset = 0
		}
		groupTopicOffset[id] = offset
	}
	return groupTopicOffset
}
