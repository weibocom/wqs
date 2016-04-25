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
	log "github.com/cihub/seelog"
)

type Manager struct {
	client  sarama.Client
	broker  *sarama.Broker
	libPath string
}

func NewManager(addrs []string, libPath string) *Manager {
	config := sarama.NewConfig()
	client, err := sarama.NewClient(addrs, config)
	if err != nil {
		log.Errorf("kafka manager init failed, addrs:%s, err:%v", addrs, err)
	}
	broker := sarama.NewBroker(addrs[0])
	broker.Open(config)
	return &Manager{client, broker, libPath}
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
		fmt.Println(script)
		out, err = exec.Command("sh", "-c", script).Output()
	}
	log.Infof("create topic:%s, result:%s", topic, string(out))
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
	log.Infof("update topic:%s, result:%s", topic, string(out))
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
	log.Infof("delete topic:%s, result:%s", topic, string(out))
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

func (m *Manager) ExistTopic(topic string) bool {
	result := false
	topics, err := m.GetTopics()
	if err != nil {
		return result
	}
	for _, t := range topics {
		if strings.EqualFold(t, topic) {
			result = true
			break
		}
	}
	return result
}

func (m *Manager) TopicSize(topic string) (int64, error) {
	var size int64 = 0
	var err error
	partitions, err := m.client.Partitions(topic)
	if err != nil {
		return size, err
	}
	for partition := range partitions {
		temp, err := m.client.GetOffset(topic, int32(partition), -1)
		if err != nil {
			return size, err
		}
		size += temp
	}
	return size, err
}
