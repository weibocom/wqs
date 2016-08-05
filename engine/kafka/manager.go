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
	"math/rand"
	"sort"
	"strconv"
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
	kafkaVersion           = 1
)

type Manager struct {
	kClient     sarama.Client
	zkConn      *zookeeper.Conn
	kafkaRoot   string
	brokerAddrs []string
	brokersList []int32
	mu          sync.Mutex
	ops         sync.Mutex
}

// get online brokers
func getBrokerAddrs(zkConn *zookeeper.Conn, kafkaRoot string) ([]string, []int32, error) {

	brokerAddrs, brokersList := make([]string, 0), make([]int32, 0)
	brokers, _, err := zkConn.Children(fmt.Sprintf("%s%s", kafkaRoot, brokersIds))
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	for _, broker := range brokers {
		data, _, err := zkConn.Get(fmt.Sprintf("%s%s/%s", kafkaRoot, brokersIds, broker))
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		config := brokerConfig{}
		if err := config.LoadFromBytes(data); err != nil {
			return nil, nil, errors.Trace(err)
		}

		id, err := strconv.ParseInt(broker, 10, 32)
		if err != nil {
			log.Warnf("get invalid kafka broker %q and omit it", broker)
			continue
		}
		brokerAddrs = append(brokerAddrs, fmt.Sprintf("%s:%d", config.Host, config.Port))
		brokersList = append(brokersList, int32(id))
	}

	if len(brokersList) == 0 || len(brokerAddrs) == 0 {
		return nil, nil, errors.NotFoundf("brokers from zookeeper")
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

// new a kafka manager by give zookeeper address of kafka
func NewManager(zkAddrs []string, kafkaRoot string, conf *sarama.Config) (*Manager, error) {

	if kafkaRoot == "/" {
		kafkaRoot = ""
	}

	zkConn, err := zookeeper.NewConnect(zkAddrs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	brokerAddrs, brokersList, err := getBrokerAddrs(zkConn, kafkaRoot)
	if err != nil {
		return nil, errors.Trace(err)
	}

	kClient, err := sarama.NewClient(brokerAddrs, conf)
	if err != nil {
		return nil, errors.Trace(err)
	}

	manager := &Manager{
		kClient:     kClient,
		zkConn:      zkConn,
		kafkaRoot:   kafkaRoot,
		brokerAddrs: brokerAddrs,
		brokersList: brokersList,
	}

	if err = manager.RefreshMetadata(); err != nil {
		return nil, errors.Trace(err)
	}

	return manager, nil
}

//refresh the available metadata of kafka
func (m *Manager) RefreshMetadata() error {

	brokerAddrs, brokersList, err := getBrokerAddrs(m.zkConn, m.kafkaRoot)
	if err != nil {
		return errors.Trace(err)
	}
	//TODO 校验broker是否发生变化，是否需要根据broker变化做响应动作？
	m.mu.Lock()
	m.brokerAddrs, m.brokersList = brokerAddrs, brokersList
	m.mu.Unlock()
	return m.kClient.RefreshMetadata()
}

// get broker address from manager's cached data
func (m *Manager) BrokerAddrs() []string {
	m.mu.Lock()
	brokerAddrs := make([]string, len(m.brokerAddrs))
	copy(brokerAddrs, m.brokerAddrs)
	m.mu.Unlock()
	return brokerAddrs
}

// get broker id list from manager's cached data
func (m *Manager) BrokersList() []int32 {
	m.mu.Lock()
	brokersList := make([]int32, len(m.brokersList))
	copy(brokersList, m.brokersList)
	m.mu.Unlock()
	return brokersList
}

// create topic internal function
func (m *Manager) createOrUpdateTopicPartitionAssignmentPathInZK(topic string,
	assignment partitonAssignment, update bool) error {

	topicPath := fmt.Sprintf("%s%s/%s", m.kafkaRoot, brokerTopics, topic)
	if !update {
		exist, _, err := m.zkConn.Exists(topicPath)
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
		info := &topicInfo{Version: kafkaVersion}
		topicConfigPath := fmt.Sprintf("%s%s/%s", m.kafkaRoot, topicConfigs, topic)
		if err = m.zkConn.Create(topicConfigPath, info.String(), 0); err != nil {
			return errors.Trace(err)
		}
	}

	partitionConfig := topicPatitionConfig{
		Version:    kafkaVersion,
		Partitions: assignment,
	}

	if update {
		if err := m.zkConn.Set(topicPath, partitionConfig.String()); err != nil {
			return errors.Trace(err)
		}
	} else {
		if err := m.zkConn.Create(topicPath, partitionConfig.String(), 0); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// create a topic by given name
func (m *Manager) CreateTopic(topic string, replications int32, partitions int32) error {
	m.ops.Lock()
	defer m.ops.Unlock()

	brokersList := m.BrokersList()
	assignment, err := assignReplicasToBrokers(brokersList, partitions, replications, -1, -1)
	if err != nil {
		return errors.Trace(err)
	}

	if err = m.createOrUpdateTopicPartitionAssignmentPathInZK(topic, assignment, false); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// add partitions to a topic
func (m *Manager) UpdateTopic(topic string, partitions int) error {
	m.ops.Lock()
	defer m.ops.Unlock()

	if topic == groupMetadataTopicName {
		return errors.NotValidf("cannot modify internal topic")
	}

	topicPath := fmt.Sprintf("%s%s/%s", m.kafkaRoot, brokerTopics, topic)
	topicAssignData, _, err := m.zkConn.Get(topicPath)
	if err != nil {
		return errors.Trace(err)
	}

	partitionConfig := topicPatitionConfig{}
	if err = partitionConfig.LoadFromBytes(topicAssignData); err != nil {
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

	brokersList := m.BrokersList()

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

	if err = m.createOrUpdateTopicPartitionAssignmentPathInZK(topic, partitionConfig.Partitions, true); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// mark given topic to delete
func (m *Manager) DeleteTopic(topic string) error {

	deleteTopicPath := fmt.Sprintf("%s%s/%s", m.kafkaRoot, adminDeleteTopicPath, topic)

	if err := m.zkConn.Create(deleteTopicPath, "", 0); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Topics returns the set of available topics as retrieved from cluster metadata.
func (m *Manager) Topics() (topics []string, err error) {
	return m.kClient.Topics()
}

// test given topic whether exists.
func (m *Manager) ExistTopic(topic string) (bool, error) {
	topics, err := m.Topics()
	if err != nil {
		return false, errors.Trace(err)
	}
	for _, t := range topics {
		if t == topic {
			return true, nil
		}
	}
	return false, nil
}

// 目前只能用于新建group时的offset置位，当group join-group后，Kafka server需要检查
// OffsetCommitRequest的ConsumerGroupGeneration、ConsumerID是否有效，这样，该API
// 就会返回失败。当有新的需求时，应当考虑该API是否需要重新设计。
func (m *Manager) CommitOffset(topic, group string, offsets map[int32]int64) error {
	m.kClient.RefreshCoordinator(group)

	broker, err := m.kClient.Coordinator(group)
	if err != nil {
		return errors.Trace(err)
	}

	request := &sarama.OffsetCommitRequest{
		Version:                 2,
		ConsumerGroup:           group,
		ConsumerGroupGeneration: -1,
		ConsumerID:              "",
		RetentionTime:           -1,
	}

	for partition, offset := range offsets {
		if offset > -1 {
			request.AddBlock(topic, partition, offset, 0, "")
		}
	}

	response, err := broker.CommitOffset(request)
	if err != nil {
		broker.Close()
		return err
	}

	for t, partitions := range response.Errors {
		for partition, err := range partitions {
			if err != sarama.ErrNoError {
				return errors.Annotatef(err, "at commit offset topic %s partition %d", t, partition)
			}
		}
	}
	return nil
}

// 得到的offset为该topic将要写入消息的offset
func (m *Manager) FetchTopicOffsets(topic string, time int64) (map[int32]int64, error) {
	partitions, err := m.kClient.Partitions(topic)
	if err != nil {
		return nil, err
	}

	offsets := make(map[int32]int64, len(partitions))
	for _, partition := range partitions {
		offset, err := m.kClient.GetOffset(topic, partition, time)
		if err != nil {
			return nil, err
		}
		offsets[partition] = offset
	}
	return offsets, err
}

func (m *Manager) FetchGroupOffsets(topic, group string) (map[int32]int64, error) {
	partitions, err := m.kClient.Partitions(topic)
	if err != nil {
		return nil, err
	}

	m.kClient.RefreshCoordinator(group)
	broker, err := m.kClient.Coordinator(group)
	if err != nil {
		return nil, err
	}

	req := &sarama.OffsetFetchRequest{
		Version:       1,
		ConsumerGroup: group,
	}
	for _, partition := range partitions {
		req.AddPartition(topic, partition)
	}

	offsets := make(map[int32]int64, len(partitions))
	response, err := broker.FetchOffset(req)
	if err != nil {
		broker.Close()
		return nil, err
	}

	for _, partition := range partitions {
		block := response.GetBlock(topic, partition)
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

// 获得指定topic, group堆积的消息的信息
func (m *Manager) Accumulation(topic, group string) (int64, int64, error) {
	totalCount := int64(0)
	consumedCount := int64(0)

	topicOffsets, err := m.FetchTopicOffsets(topic, sarama.OffsetNewest)
	if err != nil {
		return 0, 0, err
	}
	groupOffsets, err := m.FetchGroupOffsets(topic, group)
	if err != nil {
		return 0, 0, err
	}

	if len(topicOffsets) != len(groupOffsets) {
		return 0, 0, errors.Errorf("the num of partition not matched")
	}

	for partition, offset := range topicOffsets {
		totalCount += offset
		consumed := groupOffsets[partition]
		if consumed > 0 {
			consumedCount += consumed
		}
	}
	return totalCount, consumedCount, nil
}

// close manager
func (m *Manager) Close() error {
	return m.kClient.Close()
}
