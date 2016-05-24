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
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/weibocom/wqs/engine/zookeeper"
	"github.com/weibocom/wqs/log"
	"github.com/weibocom/wqs/utils"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
)

const (
	brokersIds             = "/brokers/ids"
	brokerTopics           = "/brokers/topics"
	topicConfigs           = "/config/topics"
	adminDeleteTopicPath   = "/admin/delete_topics"
	groupMetadataTopicName = "__consumer_offsets"
	defaultVersion         = 1
)

type brokerConfig struct {
	JmxPort   int32    `json:"jmx_port,omitempty"`
	TimeStamp int64    `json:"timestamp,string"`
	EndPoints []string `json:"endpoints"`
	Host      string   `json:"host"`
	Version   int32    `json:"version"`
	Port      int32    `json:"port"`
}

type topicConfig struct {
	// {"segment.bytes":"104857600","compression.type":"uncompressed","cleanup.policy":"compact"}}
	// empty object by default
}

type topicInfo struct {
	Version int32       `json:"version"`
	Config  topicConfig `json:"config"`
}

type partitonAssignment map[string][]int32

type topicPatitionConfig struct {
	Version    int32              `json:"version"`
	Partitions partitonAssignment `json:"partitions"`
}

type Manager struct {
	client      sarama.Client
	zkClient    *zookeeper.ZkClient
	kafkaRoot   string
	brokerAddrs []string
	brokersList []int32
	mu          sync.Mutex
}

func getBrokerAddrs(zkClient *zookeeper.ZkClient, kafkaRoot string) ([]string, []int32, error) {
	brokerAddrs, brokersList := make([]string, 0), make([]int32, 0)

	brokers, _, err := zkClient.Children(fmt.Sprintf("%s%s", kafkaRoot, brokersIds))
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	for _, broker := range brokers {
		data, _, err := zkClient.Get(fmt.Sprintf("%s%s/%s", kafkaRoot, brokersIds, broker))
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		var config brokerConfig
		err = json.Unmarshal(data, &config)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		brokerAddrs = append(brokerAddrs, fmt.Sprintf("%s:%d", config.Host, config.Port))
		brokerID, err := strconv.ParseInt(broker, 10, 32)
		if err != nil {
			continue
		}
		brokersList = append(brokersList, int32(brokerID))
	}
	sort.Sort(utils.Int32Slice(brokersList))

	return brokerAddrs, brokersList, nil
}

func replicationIndex(firstReplicaIndex, secondReplicaShift, replicaIndex, nBrokers int32) int32 {
	shift := 1 + (secondReplicaShift+replicaIndex)%(nBrokers-1)
	return (firstReplicaIndex + shift) % nBrokers
}

/**
 * There are 2 goals of replica assignment:
 * 1. Spread the replicas evenly among brokers.
 * 2. For partitions assigned to a particular broker, their other replicas are spread over the other
 * brokers.
 *
 * To achieve this goal, we:
 * 1. Assign the first replica of each partition by round-robin, starting from a random position in
 * the broker list.
 * 2. Assign the remaining replicas of each partition with an increasing shift.
 *
 * Here is an example of assigning
 * broker-0  broker-1  broker-2  broker-3  broker-4
 * p0        p1        p2        p3        p4       (1st replica)
 * p5        p6        p7        p8        p9       (1st replica)
 * p4        p0        p1        p2        p3       (2nd replica)
 * p8        p9        p5        p6        p7       (2nd replica)
 * p3        p4        p0        p1        p2       (3nd replica)
 * p7        p8        p9        p5        p6       (3nd replica)
 */
func assignReplicasToBrokers(brokersList []int32,
	patitions, replications, fixedStartIndex, startPartitionId int32) (partitonAssignment, error) {

	nBrokers := int32(len(brokersList))
	if patitions < 1 || replications < 1 {
		return nil, errors.NotValidf("patitions: %d, replications: %d", patitions, replications)
	}
	if replications > nBrokers {
		return nil, errors.NotSupportedf("replications great than nums of brokers")
	}

	startIndex := fixedStartIndex
	nextReplicaShift := fixedStartIndex
	if fixedStartIndex < 0 {
		startIndex = rand.Int31n(nBrokers)
		nextReplicaShift = rand.Int31n(nBrokers)
	}
	currentPartitionId := startPartitionId
	if currentPartitionId < 0 {
		currentPartitionId = 0
	}

	assignment := make(partitonAssignment)
	for i := int32(0); i < patitions; i++ {
		if currentPartitionId > 0 && (currentPartitionId%nBrokers == 0) {
			nextReplicaShift += 1
		}
		firstReplicaIndex := (currentPartitionId + startIndex) % nBrokers
		replicaList := make([]int32, replications)
		replicaList[0] = brokersList[firstReplicaIndex]
		for j := int32(0); j < replications-1; j++ {
			replicaList[j+1] = brokersList[replicationIndex(firstReplicaIndex, nextReplicaShift, j, nBrokers)]
		}
		assignment[strconv.FormatInt(int64(currentPartitionId), 10)] = replicaList
		currentPartitionId++
	}

	return assignment, nil
}

func NewManager(zkAddrs []string, kafkaRoot string, conf *sarama.Config) (*Manager, error) {

	if kafkaRoot == "/" {
		kafkaRoot = ""
	}

	zkClient, err := zookeeper.NewZkClient(zkAddrs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	brokerAddrs, brokersList, err := getBrokerAddrs(zkClient, kafkaRoot)
	if err != nil {
		return nil, errors.Trace(err)
	}

	client, err := sarama.NewClient(brokerAddrs, conf)
	if err != nil {
		return nil, errors.Trace(err)
	}

	manager := &Manager{
		client:      client,
		zkClient:    zkClient,
		kafkaRoot:   kafkaRoot,
		brokerAddrs: brokerAddrs,
		brokersList: brokersList,
	}

	err = manager.RefreshMetadata()
	if err != nil {
		return nil, errors.Trace(err)
	}

	return manager, nil
}

func (m *Manager) RefreshMetadata() error {

	brokerAddrs, brokersList, err := getBrokerAddrs(m.zkClient, m.kafkaRoot)
	if err != nil {
		return errors.Trace(err)
	}

	m.mu.Lock()
	m.brokerAddrs, m.brokersList = brokerAddrs, brokersList
	m.mu.Unlock()
	return m.client.RefreshMetadata()
}

func (m *Manager) BrokerAddrs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.brokerAddrs
}

func (m *Manager) createOrUpdateTopicPartitionAssignmentPathInZK(topic string,
	assignment partitonAssignment, update bool) error {

	topicPath := fmt.Sprintf("%s%s/%s", m.kafkaRoot, brokerTopics, topic)
	if !update {
		exist, _, err := m.zkClient.Exists(topicPath)
		if err != nil {
			return errors.Trace(err)
		}
		if exist {
			return errors.AlreadyExistsf("topic : %q", topic)
		}
		// 由于命令接口前段对queue名称进行了限制，所以暂时不需要下面这几步校验
		//		topics, err := m.client.Topics()
		//		if err != nil {
		//			return errors.Trace(err)
		//		}
		//		for _, noTopic := range topics {
		//			if strings.Replace(topic, ".", "_", -1) == strings.Replace(noTopic, ".", "_", -1) {
		//				return errors.NotValidf("topic : %q", topic)
		//			}
		//		}
		info := &topicInfo{Version: defaultVersion}
		topicConfigPath := fmt.Sprintf("%s%s/%s", m.kafkaRoot, topicConfigs, topic)
		configData, err := json.Marshal(&info)
		if err != nil {
			return errors.Trace(err)
		}
		err = m.zkClient.Create(topicConfigPath, string(configData), 0)
		if err != nil {
			return errors.Trace(err)
		}
	}
	partitionConfig := topicPatitionConfig{
		Version:    defaultVersion,
		Partitions: assignment,
	}
	partitionConfigData, err := json.Marshal(&partitionConfig)
	if err != nil {
		return errors.Trace(err)
	}
	if update {
		err = m.zkClient.Set(topicPath, string(partitionConfigData))
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		err = m.zkClient.Create(topicPath, string(partitionConfigData), 0)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (m *Manager) CreateTopic(topic string, replications int32, partitions int32) error {

	m.mu.Lock()
	brokersList := m.brokersList
	m.mu.Unlock()

	assignment, err := assignReplicasToBrokers(brokersList, partitions, replications, -1, -1)
	if err != nil {
		return errors.Trace(err)
	}

	err = m.createOrUpdateTopicPartitionAssignmentPathInZK(topic, assignment, false)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (m *Manager) UpdateTopic(topic string, partitions int) error {

	if topic == groupMetadataTopicName {
		return errors.NotValidf("cannot modify internal topic")
	}

	topicPath := fmt.Sprintf("%s%s/%s", m.kafkaRoot, brokerTopics, topic)
	topicAssignData, _, err := m.zkClient.Get(topicPath)
	if err != nil {
		return errors.Trace(err)
	}

	partitionConfig := topicPatitionConfig{}
	err = json.Unmarshal(topicAssignData, &partitionConfig)
	if err != nil {
		return errors.Trace(err)
	}

	partitionsToAdd := partitions - len(partitionConfig.Partitions)
	if partitionsToAdd <= 0 {
		return errors.Errorf("partition can only be increased")
	}

	//partition start with "0"
	replicationList := partitionConfig.Partitions["0"]
	if len(replicationList) == 0 {
		return errors.Errorf("exist replication is 0")
	}

	m.mu.Lock()
	brokersList := m.brokersList
	m.mu.Unlock()

	newAssignment, err := assignReplicasToBrokers(brokersList, int32(partitionsToAdd),
		int32(len(replicationList)), replicationList[0], int32(len(partitionConfig.Partitions)))
	if err != nil {
		return errors.Trace(err)
	}

	// check if new assignment has the right replication factor
	for partition, assign := range newAssignment {
		if len(assign) != len(replicationList) {
			return errors.NotValidf("new replication assignment %v", newAssignment)
		}
		partitionConfig.Partitions[partition] = assign
	}

	err = m.createOrUpdateTopicPartitionAssignmentPathInZK(topic, partitionConfig.Partitions, true)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (m *Manager) DeleteTopic(topic string) error {

	deleteTopicPath := fmt.Sprintf("%s%s/%s", m.kafkaRoot, adminDeleteTopicPath, topic)
	err := m.zkClient.Create(deleteTopicPath, "", 0)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (m *Manager) GetTopics() ([]string, error) {
	topics, err := m.client.Topics()
	if err != nil {
		log.Warnf("get topics err : %s", err)
	}
	return topics, err
}

func (m *Manager) ExistTopic(topic string) (bool, error) {
	topics, err := m.GetTopics()
	if err != nil {
		return false, errors.Trace(err)
	}
	for _, t := range topics {
		if strings.EqualFold(t, topic) {
			return true, nil
		}
	}
	return false, nil
}

func (m *Manager) FetchTopicSize(topic string) (int64, error) {
	count := int64(0)
	topicOffsets, err := m.FetchTopicOffsets(topic)
	if err != nil {
		return -1, err
	}
	for _, offset := range topicOffsets {
		// topic 的offset是下次要写入的位置，而不是最后一条消息的位置，所以在这里要减1
		offset--
		count += offset
	}
	return count, nil
}

func (m *Manager) FetchTopicOffsets(topic string) (map[int32]int64, error) {
	partitions, err := m.client.Partitions(topic)
	offsets := make(map[int32]int64, len(partitions))
	if err != nil {
		return nil, err
	}
	for partition := range partitions {
		offset, err := m.client.GetOffset(topic, int32(partition), -1)
		if err != nil {
			return nil, err
		} else {
			offsets[int32(partition)] = offset
		}
	}
	return offsets, err
}

func (m *Manager) FetchGroupOffsets(topic, group string) (map[int32]int64, error) {
	partitions, err := m.client.Partitions(topic)
	offsets := make(map[int32]int64, len(partitions))
	req := &sarama.OffsetFetchRequest{
		Version:       1,
		ConsumerGroup: group,
	}
	m.client.RefreshCoordinator(group)
	broker, err := m.client.Coordinator(group)
	if err != nil {
		return nil, err
	}
	for _, partition := range partitions {
		req.AddPartition(topic, partition)
	}
	resp, err := broker.FetchOffset(req)
	if err != nil {
		return nil, err
	}
	for _, partition := range partitions {
		block := resp.GetBlock(topic, partition)
		if block == nil {
			return nil, sarama.ErrIncompleteResponse
		}
		if block.Err == sarama.ErrNoError {
			offsets[partition] = block.Offset
		} else {
			return nil, block.Err
		}
	}
	return offsets, nil
}

func (m *Manager) Accumulation(topic, group string) (int64, int64, error) {
	topicCount := int64(0)
	groupCount := int64(0)
	topicOffsets, err := m.FetchTopicOffsets(topic)
	if err != nil {
		return -1, -1, err
	}
	groupOffsets, err := m.FetchGroupOffsets(topic, group)
	if err != nil {
		return -1, -1, err
	}
	if len(topicOffsets) != len(groupOffsets) {
		return -1, -1, errors.Errorf("the num of partition not matched")
	}
	for partition, offset := range topicOffsets {
		// topic 的offset是下次要写入的位置，而不是最后一条消息的位置，所以在这里要减1
		if offset > 0 {
			offset--
		}
		topicCount += offset
		if groupOffsets[partition] < 0 {
			groupCount += offset
		} else {
			groupCount += groupOffsets[partition]
		}
	}
	return topicCount, groupCount, nil
}

func (m *Manager) Close() error {
	return m.client.Close()
}
