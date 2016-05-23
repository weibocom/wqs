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
	"os/exec"
	"runtime"
	"strings"
	"sync"

	"github.com/weibocom/wqs/engine/zookeeper"
	"github.com/weibocom/wqs/log"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
)

const (
	brokersIds = "/brokers/ids"
)

type brokerConfig struct {
	JmxPort   int32    `json:"jmx_port,omitempty"`
	TimeStamp int64    `json:"timestamp,string"`
	EndPoints []string `json:"endpoints"`
	Host      string   `json:"host"`
	Version   int32    `json:"version"`
	Port      int32    `json:"port"`
}

type Manager struct {
	client      sarama.Client
	zkClient    *zookeeper.ZkClient
	libPath     string
	kafkaRoot   string
	brokerAddrs []string
	mu          sync.Mutex
}

func getBrokerAddrs(zkClient *zookeeper.ZkClient, kafkaRoot string) ([]string, error) {
	brokerAddrs := make([]string, 0)

	brokers, _, err := zkClient.Children(fmt.Sprintf("%s%s", kafkaRoot, brokersIds))
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, broker := range brokers {
		data, _, err := zkClient.Get(fmt.Sprintf("%s%s/%s", kafkaRoot, brokersIds, broker))
		if err != nil {
			return nil, errors.Trace(err)
		}

		var config brokerConfig
		err = json.Unmarshal(data, &config)
		if err != nil {
			return nil, errors.Trace(err)
		}
		brokerAddrs = append(brokerAddrs, fmt.Sprintf("%s:%d", config.Host, config.Port))
	}

	return brokerAddrs, nil
}

func NewManager(zkAddrs []string, libPath string, kafkaRoot string, conf *sarama.Config) (*Manager, error) {

	if kafkaRoot == "/" {
		kafkaRoot = ""
	}

	zkClient, err := zookeeper.NewZkClient(zkAddrs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	brokerAddrs, err := getBrokerAddrs(zkClient, kafkaRoot)
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
		libPath:     libPath,
		kafkaRoot:   kafkaRoot,
		brokerAddrs: brokerAddrs,
	}

	err = manager.RefreshMetadata()
	if err != nil {
		return nil, errors.Trace(err)
	}

	return manager, nil
}

func (m *Manager) RefreshMetadata() error {

	brokerAddrs, err := getBrokerAddrs(m.zkClient, m.kafkaRoot)
	if err != nil {
		return errors.Trace(err)
	}

	m.mu.Lock()
	m.brokerAddrs = brokerAddrs
	m.mu.Unlock()
	return m.client.RefreshMetadata()
}

func (m *Manager) BrokerAddrs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.brokerAddrs
}

func (m *Manager) CreateTopic(topic string, replications int, partitions int, zkAddr string) error {
	var out []byte
	var err error
	if runtime.GOOS == "windows" {
		params := fmt.Sprintf("--create --zookeeper %s --replication-factor %d --partitions %d --topic %s", zkAddr, replications, partitions, topic)
		script := fmt.Sprintf("%s\\bin\\windows\\kafka-topics.bat %s", m.libPath, params)
		out, err = exec.Command("cmd", "/C", script).Output()
	} else {
		params := fmt.Sprintf("--create --zookeeper %s --replication-factor %d --partitions %d --topic %s", zkAddr, replications, partitions, topic)
		script := fmt.Sprintf("%s/bin/kafka-topics.sh %s", m.libPath, params)
		out, err = exec.Command("sh", "-c", script).Output()
	}
	log.Debugf("create topic:%s, result:%s", topic, string(out))
	if err != nil {
		log.Errorf("create topic error, topic:%s, result:%s, err:%v", topic, string(out), err)
	}
	return err
}

func (m *Manager) UpdateTopic(topic string, partitions int, zkAddr string) error {
	var out []byte
	var err error
	if runtime.GOOS == "windows" {
		params := fmt.Sprintf("--alter --zookeeper %s --partitions %d --topic %s", zkAddr, partitions, topic)
		script := fmt.Sprintf("%s\\bin\\windows\\kafka-topics.bat %s", m.libPath, params)
		out, err = exec.Command("cmd", "/C", script).Output()
	} else {
		params := fmt.Sprintf("--alter --zookeeper %s --partitions %d --topic %s", zkAddr, partitions, topic)
		script := fmt.Sprintf("%s/bin/kafka-topics.sh %s", m.libPath, params)
		out, err = exec.Command("sh", "-c", script).Output()
	}
	log.Debugf("update topic:%s, result:%s", topic, string(out))
	if err != nil {
		log.Errorf("update topic error, topic:%s, result:%s, err:%v", topic, string(out), err)
	}
	return err
}

func (m *Manager) DeleteTopic(topic string, zkAddr string) error {
	var out []byte
	var err error
	if runtime.GOOS == "windows" {
		params := fmt.Sprintf("--delete --zookeeper %s --topic %s", zkAddr, topic)
		script := fmt.Sprintf("%s\\bin\\windows\\kafka-topics.bat %s", m.libPath, params)
		out, err = exec.Command("cmd", "/C", script).Output()
	} else {
		params := fmt.Sprintf("--delete --zookeeper %s --topic %s", zkAddr, topic)
		script := fmt.Sprintf("%s/bin/kafka-topics.sh %s", m.libPath, params)
		out, err = exec.Command("sh", "-c", script).Output()
	}
	log.Debugf("delete topic:%s, result:%s", topic, string(out))
	if err != nil {
		log.Errorf("delete topic error, topic:%s, result:%s, err:%v", topic, string(out), err)
	}
	return err
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
