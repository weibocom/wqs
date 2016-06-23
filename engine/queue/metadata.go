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
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/weibocom/wqs/config"
	"github.com/weibocom/wqs/engine/kafka"
	"github.com/weibocom/wqs/engine/zookeeper"
	"github.com/weibocom/wqs/log"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/samuel/go-zookeeper/zk"
)

const (
	groupConfigPathSuffix = "/wqs/metadata/groupconfig"
	queuePathSuffix       = "/wqs/metadata/queue"
	servicePathPrefix     = "/wqs/metadata/service"
	operationPathPrefix   = "/wqs/metadata/operation"
	root                  = "/"
)

type Metadata struct {
	config          *config.Config
	zkClient        *zookeeper.ZkClient
	manager         *kafka.Manager
	groupConfigPath string
	queuePath       string
	servicePath     string
	operationPath   string
	queueConfigs    map[string]QueueConfig
	closeCh         chan struct{}
	rw              sync.RWMutex
}

func NewMetadata(config *config.Config, sconfig *sarama.Config) (*Metadata, error) {
	zkClient, err := zookeeper.NewZkClient(strings.Split(config.MetaDataZKAddr, ","))
	if err != nil {
		return nil, errors.Trace(err)
	}

	zkRoot := config.MetaDataZKRoot
	if strings.EqualFold(zkRoot, root) {
		zkRoot = ""
	}

	groupConfigPath := fmt.Sprintf("%s%s", zkRoot, groupConfigPathSuffix)
	queuePath := fmt.Sprintf("%s%s", zkRoot, queuePathSuffix)
	servicePath := fmt.Sprintf("%s%s", zkRoot, servicePathPrefix)
	operationPath := fmt.Sprintf("%s%s", zkRoot, operationPathPrefix)

	err = zkClient.CreateRec(groupConfigPath, "", 0)
	if err != nil && err != zk.ErrNodeExists {
		return nil, errors.Trace(err)
	}

	err = zkClient.CreateRec(queuePath, "", 0)
	if err != nil && err != zk.ErrNodeExists {
		return nil, errors.Trace(err)
	}

	err = zkClient.CreateRec(servicePath, "", 0)
	if err != nil && err != zk.ErrNodeExists {
		return nil, errors.Trace(err)
	}

	err = zkClient.CreateRec(operationPath, "", 0)
	if err != nil && err != zk.ErrNodeExists {
		return nil, errors.Trace(err)
	}

	manager, err := kafka.NewManager(strings.Split(config.KafkaZKAddr, ","), config.KafkaZKRoot, sconfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	metadata := &Metadata{
		config:          config,
		zkClient:        zkClient,
		manager:         manager,
		groupConfigPath: groupConfigPath,
		queuePath:       queuePath,
		servicePath:     servicePath,
		operationPath:   operationPath,
		queueConfigs:    make(map[string]QueueConfig),
		closeCh:         make(chan struct{}),
	}

	err = metadata.RefreshMetadata()
	if err != nil {
		return nil, errors.Trace(err)
	}

	go func(m *Metadata) {
		timeout := time.NewTicker(sconfig.Metadata.RefreshFrequency)
		for {
			select {
			case <-timeout.C:
				err := m.RefreshMetadata()
				if err != nil {
					log.Warnf("timeout refresh metadata err : %s", err)
				}
			case <-m.closeCh:
				timeout.Stop()
				return
			}
		}
	}(metadata)

	return metadata, nil
}

func (m *Metadata) RegisterService(id int, data string) error {
	err := m.zkClient.Create(fmt.Sprintf("%s/%d", m.servicePath, id), data, zk.FlagEphemeral)
	if err != nil {
		if err == zk.ErrNodeExists {
			return errors.AlreadyExistsf("service %d", id)
		}
		return errors.Trace(err)
	}
	return nil
}

//Get a proxy's config
func (m *Metadata) GetProxyConfigByID(id int) (string, error) {

	data, _, err := m.zkClient.Get(fmt.Sprintf("%s/%d", m.servicePath, id))
	if err != nil {
		if err == zk.ErrNoNode {
			return "", errors.NotFoundf("proxy %d", id)
		}
		return "", errors.Trace(err)
	}

	info := proxyInfo{}
	err = json.Unmarshal(data, &info)
	if err != nil {
		return "", errors.Trace(err)
	}
	return info.Config, nil
}

func (m *Metadata) GetProxys() (map[string]string, error) {

	proxys := make(map[string]string)
	ids, _, err := m.zkClient.Children(m.servicePath)
	if err != nil {
		return proxys, errors.Trace(err)
	}

	for _, id := range ids {
		data, _, err := m.zkClient.Get(fmt.Sprintf("%s/%s", m.servicePath, id))
		if err != nil {
			return proxys, errors.Trace(err)
		}

		info := proxyInfo{}
		err = json.Unmarshal(data, &info)
		if err != nil {
			return proxys, errors.Trace(err)
		}

		proxys[id] = info.Host
	}
	return proxys, nil
}

func (m *Metadata) RefreshMetadata() error {
	queueConfigs := make(map[string]QueueConfig)

	err := m.manager.RefreshMetadata()
	if err != nil {
		return errors.Trace(err)
	}

	queues, _, err := m.zkClient.Children(m.queuePath)
	if err != nil {
		return errors.Trace(err)
	}

	for _, queue := range queues {
		_, stat, err := m.zkClient.Get(m.buildQueuePath(queue))
		if err != nil {
			log.Errorf("refresh err : %s", err)
			return errors.Trace(err)
		}

		exist, err := m.manager.ExistTopic(queue)
		if err != nil {
			log.Errorf("refresh err : %s", err)
			return errors.Trace(err)
		}
		if !exist {
			log.Errorf("queue : %q has metadata, but has no topic", queue)
			continue
		}

		queueConfigs[queue] = QueueConfig{
			Queue:  queue,
			Ctime:  stat.Ctime / 1e3,
			Length: 0,
			Groups: make(map[string]GroupConfig),
		}
	}

	groupKeys, _, err := m.zkClient.Children(m.groupConfigPath)
	if err != nil {
		return errors.Trace(err)
	}

	for _, groupKey := range groupKeys {
		tokens := strings.Split(groupKey, ".")
		if len(tokens) != 2 {
			continue
		}
		queueName, groupName := tokens[1], tokens[0]
		queue, ok := queueConfigs[queueName]
		if !ok {
			continue
		}

		groupDataPath := fmt.Sprintf("%s/%s", m.groupConfigPath, groupKey)
		groupData, _, err := m.zkClient.Get(groupDataPath)
		if err != nil {
			log.Warnf("get %s err: %s", groupDataPath, err)
			continue
		}

		groupConfig := GroupConfig{}
		err = json.Unmarshal(groupData, &groupConfig)
		if err != nil {
			log.Warnf("Unmarshal %s data err: %s", groupDataPath, err)
			continue
		}

		queue.Groups[groupName] = groupConfig
	}

	m.rw.Lock()
	m.queueConfigs = queueConfigs
	m.rw.Unlock()
	return nil
}

func (m *Metadata) AddGroupConfig(group string, queue string,
	write bool, read bool, url string, ips []string) error {

	mutex := zk.NewLock(m.zkClient.Conn, m.operationPath, zk.WorldACL(zk.PermAll))
	if err := mutex.Lock(); err != nil {
		return errors.Trace(err)
	}
	defer mutex.Unlock()

	if err := m.RefreshMetadata(); err != nil {
		return errors.Trace(err)
	}

	if exist := m.ExistGroup(queue, group); exist {
		return errors.AlreadyExistsf("queue : %q, group : %q", queue, group)
	}

	path := m.buildConfigPath(group, queue)
	groupConfig := GroupConfig{
		Group: group,
		Queue: queue,
		Write: write,
		Read:  read,
		Url:   url,
		Ips:   ips,
	}
	data := groupConfig.String()
	log.Debugf("add group config, zk path:%s, data:%s", path, data)
	if err := m.zkClient.CreateRec(path, data, 0); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (m *Metadata) DeleteGroupConfig(group string, queue string) error {

	mutex := zk.NewLock(m.zkClient.Conn, m.operationPath, zk.WorldACL(zk.PermAll))
	if err := mutex.Lock(); err != nil {
		return errors.Trace(err)
	}
	defer mutex.Unlock()

	if err := m.RefreshMetadata(); err != nil {
		return errors.Trace(err)
	}

	if exist := m.ExistGroup(queue, group); !exist {
		return errors.NotFoundf("queue : %q, group : %q", queue, group)
	}

	path := m.buildConfigPath(group, queue)
	log.Debugf("delete group config, zk path:%s", path)
	if err := m.zkClient.DeleteRec(path); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (m *Metadata) UpdateGroupConfig(group string, queue string,
	write bool, read bool, url string, ips []string) error {

	mutex := zk.NewLock(m.zkClient.Conn, m.operationPath, zk.WorldACL(zk.PermAll))
	if err := mutex.Lock(); err != nil {
		return errors.Trace(err)
	}
	defer mutex.Unlock()

	if err := m.RefreshMetadata(); err != nil {
		return errors.Trace(err)
	}

	if exist := m.ExistGroup(queue, group); !exist {
		return errors.NotFoundf("queue : %q, group: %q", queue, group)
	}

	path := m.buildConfigPath(group, queue)
	groupConfig := GroupConfig{
		Group: group,
		Queue: queue,
		Write: write,
		Read:  read,
		Url:   url,
		Ips:   ips,
	}
	data := groupConfig.String()
	log.Debugf("update group config, zk path:%s, data:%s", path, data)
	if err := m.zkClient.Set(path, data); err != nil {
		return errors.Trace(err)
	}
	return nil
}

//TODO 回头修改HTTP API时同时修改返回的数据结构，能够最大化简化逻辑
func (m *Metadata) GetQueueConfig(queues ...string) ([]*QueueInfo, error) {
	m.rw.RLock()
	defer m.rw.RUnlock()

	queueInfos := make([]*QueueInfo, 0)
	for _, queue := range queues {
		queueConfig, ok := m.queueConfigs[queue]
		if !ok {
			return queueInfos, errors.NotFoundf("queue: %q", queue)
		}

		queueInfo := QueueInfo{
			Queue:  queue,
			Ctime:  queueConfig.Ctime,
			Length: queueConfig.Length,
			Groups: make([]*GroupConfig, 0),
		}

		for _, groupConfig := range queueConfig.Groups {
			queueInfo.Groups = append(queueInfo.Groups, &groupConfig)
		}
		queueInfos = append(queueInfos, &queueInfo)
	}

	return queueInfos, nil
}

func (m *Metadata) GetGroupConfig(group string, queue string) (*GroupConfig, error) {
	m.rw.RLock()
	defer m.rw.RUnlock()

	queueConfig, ok := m.queueConfigs[queue]
	if !ok {
		return nil, errors.NotFoundf("queue: %q", queue)
	}

	groupConfig, ok := queueConfig.Groups[group]
	if !ok {
		return nil, errors.NotFoundf("group: %q", group)
	}
	return &groupConfig, nil
}

//Get queue names of per group
func (m *Metadata) GetGroupMap() map[string][]string {
	groupmap := make(map[string][]string)
	queuemap := m.GetQueueMap()
	for queue, groups := range queuemap {
		for _, group := range groups {
			groupmap[group] = append(groupmap[group], queue)
		}
	}
	return groupmap
}

//Get group names of per queue
func (m *Metadata) GetQueueMap() map[string][]string {
	queuemap := make(map[string][]string)

	m.rw.RLock()
	defer m.rw.RUnlock()

	for queue, queueConfig := range m.queueConfigs {
		groups := make([]string, 0)
		for group := range queueConfig.Groups {
			groups = append(groups, group)
		}
		queuemap[queue] = groups
	}
	return queuemap
}

//Add a queue by name
func (m *Metadata) AddQueue(queue string) error {

	mutex := zk.NewLock(m.zkClient.Conn, m.operationPath, zk.WorldACL(zk.PermAll))
	if err := mutex.Lock(); err != nil {
		return errors.Trace(err)
	}
	defer mutex.Unlock()

	if err := m.RefreshMetadata(); err != nil {
		return errors.Trace(err)
	}

	if exist := m.ExistQueue(queue); exist {
		return errors.AlreadyExistsf("CreateQueue queue:%s ", queue)
	}

	if err := m.manager.CreateTopic(queue, int32(m.config.KafkaReplications),
		int32(m.config.KafkaPartitions)); err != nil {
		return errors.Trace(err)
	}

	path := m.buildQueuePath(queue)
	data := ""
	log.Debugf("add queue, zk path:%s, data:%s", path, data)

	if err := m.zkClient.CreateRec(path, data, 0); err != nil {
		return errors.Trace(err)
	}
	return nil
}

//Delete a queue by name
func (m *Metadata) DelQueue(queue string) error {

	mutex := zk.NewLock(m.zkClient.Conn, m.operationPath, zk.WorldACL(zk.PermAll))
	if err := mutex.Lock(); err != nil {
		return errors.Trace(err)
	}
	defer mutex.Unlock()

	if err := m.RefreshMetadata(); err != nil {
		return errors.Trace(err)
	}

	can, err := m.canDeleteQueue(queue)
	if err != nil {
		return errors.Trace(err)
	}
	if !can {
		return errors.NotValidf("DeleteQueue queue:%s has one or more group", queue)
	}

	path := m.buildQueuePath(queue)
	log.Debugf("del queue, zk path:%s", path)
	if err := m.zkClient.DeleteRec(path); err != nil {
		return errors.Trace(err)
	}
	delete(m.queueConfigs, queue)
	if err := m.manager.DeleteTopic(queue); err != nil {
		return errors.Trace(err)
	}
	return nil
}

//Get all queues' name
func (m *Metadata) GetQueues() (queues []string) {
	m.rw.RLock()
	for queue := range m.queueConfigs {
		queues = append(queues, queue)
	}
	m.rw.RUnlock()
	return
}

//Test a queue exist
func (m *Metadata) ExistQueue(queue string) bool {
	m.rw.RLock()
	_, exist := m.queueConfigs[queue]
	m.rw.RUnlock()
	return exist
}

//Test a group exist
func (m *Metadata) ExistGroup(queue, group string) bool {
	m.rw.RLock()
	queueConfig, exist := m.queueConfigs[queue]
	if !exist {
		m.rw.RUnlock()
		return false
	}
	_, exist = queueConfig.Groups[group]
	m.rw.RUnlock()
	return exist
}

//test a queue can be delete
func (m *Metadata) canDeleteQueue(queue string) (bool, error) {
	m.rw.RLock()
	defer m.rw.RUnlock()

	queueConfig, ok := m.queueConfigs[queue]
	if !ok {
		return false, errors.NotFoundf("queue: %q", queue)
	}

	return len(queueConfig.Groups) == 0, nil
}

func (m *Metadata) Accumulation(queue, group string) (int64, int64, error) {
	return m.manager.Accumulation(queue, group)
}

func (m *Metadata) buildConfigPath(group string, queue string) string {
	return m.groupConfigPath + "/" + group + "." + queue
}

func (m *Metadata) buildQueuePath(queue string) string {
	return m.queuePath + "/" + queue
}

func (m *Metadata) Close() {
	close(m.closeCh)
	m.zkClient.Close()
	m.manager.Close()
}
