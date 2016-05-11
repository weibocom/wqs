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
	"strings"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/weibocom/wqs/log"
)

type Manager struct {
	client  sarama.Client
	libPath string
}

func NewManager(brokerAddrs []string, libPath string, conf *sarama.Config) (*Manager, error) {
	client, err := sarama.NewClient(brokerAddrs, conf)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &Manager{client, libPath}, nil
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
		log.Warnf("get topics error, err:%s", err)
	}
	return topics, err
}

//It will refresh metadata and double check when refrsh is true.
func (m *Manager) ExistTopic(topic string, refresh bool) (bool, error) {
	topics, err := m.GetTopics()
	if err != nil {
		return false, errors.Trace(err)
	}
	for _, t := range topics {
		if strings.EqualFold(t, topic) {
			return true, nil
		}
	}
	if refresh {
		m.client.RefreshMetadata()
		return m.ExistTopic(topic, false)
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
		offset--
		topicCount += offset
		if groupOffsets[partition] < 0 {
			groupCount += offset
		}
		groupCount += groupOffsets[partition]

	}
	return topicCount, groupCount, nil
}
