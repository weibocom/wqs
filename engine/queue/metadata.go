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

	"github.com/weibocom/wqs/engine/zookeeper"
	"github.com/weibocom/wqs/log"

	"github.com/juju/errors"
)

const (
	groupConfigPathSuffix = "/wqs/metadata/groupconfig"
	queuePathSuffix       = "/wqs/metadata/queue"
	root                  = "/"
)

type Metadata struct {
	zkClient        *zookeeper.ZkClient
	groupConfigPath string
	queuePath       string
}

func NewMetadata(zkAddrs []string, zkRoot string) (*Metadata, error) {
	zk, err := zookeeper.NewZkClient(zkAddrs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if strings.EqualFold(zkRoot, root) {
		zkRoot = ""
	}
	groupConfigPath := fmt.Sprintf("%s%s", zkRoot, groupConfigPathSuffix)
	queuePath := fmt.Sprintf("%s%s", zkRoot, queuePathSuffix)

	exist, _, err := zk.Exists(groupConfigPath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !exist {
		zk.CreateRec(groupConfigPath, "")
	}

	exist, _, err = zk.Exists(queuePath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !exist {
		zk.CreateRec(queuePath, "")
	}

	return &Metadata{
		zkClient:        zk,
		groupConfigPath: groupConfigPath,
		queuePath:       queuePath,
	}, nil
}

//========extend配置相关函数========//

func (m *Metadata) AddGroupConfig(group string, queue string,
	write bool, read bool, url string, ips []string) error {

	path := m.buildConfigPath(group, queue)
	groupConfig := GroupConfig{
		Group: group,
		Queue: queue,
		Write: write,
		Read:  read,
		Url:   url,
		Ips:   ips,
	}
	data := groupConfig.ToJson()
	log.Debugf("add group config, zk path:%s, data:%s", path, data)
	err := m.zkClient.CreateRec(path, data)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (m *Metadata) DeleteGroupConfig(group string, queue string) error {
	path := m.buildConfigPath(group, queue)
	log.Debugf("delete group config, zk path:%s", path)
	err := m.zkClient.DeleteRec(path)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (m *Metadata) UpdateGroupConfig(group string, queue string,
	write bool, read bool, url string, ips []string) error {

	path := m.buildConfigPath(group, queue)
	groupConfig := GroupConfig{
		Group: group,
		Queue: queue,
		Write: write,
		Read:  read,
		Url:   url,
		Ips:   ips,
	}
	data := groupConfig.ToJson()
	log.Debugf("update group config, zk path:%s, data:%s", path, data)
	if err := m.zkClient.Set(path, data); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (m *Metadata) GetGroupConfig(group string, queue string) (*GroupConfig, error) {
	path := m.buildConfigPath(group, queue)
	data, _, err := m.zkClient.Get(path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(data) == 0 {
		log.Debugf("get group config, zk path:%s, data:null, err:%s", path, err)
		return nil, nil
	}

	groupConfig := GroupConfig{}
	err = json.Unmarshal(data, &groupConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Debugf("get group config, zk path:%s, data:%s", path, groupConfig.ToJson())
	return &groupConfig, nil
}

func (m *Metadata) GetAllGroupConfig() (map[string]*GroupConfig, error) {
	allGroupConfig := make(map[string]*GroupConfig)
	keys, _, err := m.zkClient.Children(m.groupConfigPath)
	if err != nil {
		return allGroupConfig, errors.Trace(err)
	}

	for _, key := range keys {
		data, _, err := m.zkClient.Get(fmt.Sprintf("%s/%s", m.groupConfigPath, key))
		if err != nil {
			return allGroupConfig, errors.Trace(err)
		}
		groupConfig := &GroupConfig{}
		err = json.Unmarshal(data, groupConfig)
		if err != nil {
			return allGroupConfig, errors.Trace(err)
		}
		allGroupConfig[key] = groupConfig
	}
	return allGroupConfig, nil
}

func (m *Metadata) GetGroupMap() (map[string][]string, error) {
	groupmap := make(map[string][]string)
	keys, _, err := m.zkClient.Children(m.groupConfigPath)
	if err != nil {
		return groupmap, errors.Trace(err)
	}
	for _, k := range keys {
		group := strings.Split(k, ".")[0]
		queues, ok := groupmap[group]
		if ok {
			queues = append(queues, strings.Split(k, ".")[1])
			groupmap[group] = queues
		} else {
			tempqueues := make([]string, 0)
			tempqueues = append(tempqueues, strings.Split(k, ".")[1])
			groupmap[group] = tempqueues
		}
	}
	return groupmap, nil
}

func (m *Metadata) GetQueueMap() (map[string][]string, error) {
	queuemap := make(map[string][]string)
	keys, _, err := m.zkClient.Children(m.groupConfigPath)
	if err != nil {
		return queuemap, errors.Trace(err)
	}
	queues, _, err := m.zkClient.Children(m.queuePath)
	if err != nil {
		return queuemap, errors.Trace(err)
	}
	for _, queue := range queues {
		queuemap[queue] = make([]string, 0)
	}
	for _, k := range keys {
		queue := strings.Split(k, ".")[1]
		groups, ok := queuemap[queue]
		if ok {
			groups = append(groups, strings.Split(k, ".")[0])
			queuemap[queue] = groups
		}
	}
	return queuemap, nil
}

func (m *Metadata) AddQueue(queue string) error {
	path := m.buildQueuePath(queue)
	data := ""
	log.Debugf("add queue, zk path:%s, data:%s", path, data)
	err := m.zkClient.CreateRec(path, data)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (m *Metadata) DelQueue(queue string) error {
	path := m.buildQueuePath(queue)
	log.Debugf("del queue, zk path:%s", path)
	if err := m.zkClient.DeleteRec(path); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (m *Metadata) GetQueues() ([]string, error) {
	queues, _, err := m.zkClient.Children(m.queuePath)
	return queues, err
}

func (m *Metadata) ExistQueue(queue string) (bool, error) {
	queues, err := m.GetQueues()
	for _, q := range queues {
		if strings.EqualFold(q, queue) {
			return true, nil
		}
	}
	return false, err
}

func (m *Metadata) CanDeleteQueue(queue string) (bool, error) {
	keys, _, err := m.zkClient.Children(m.groupConfigPath)
	if err != nil {
		return false, errors.Trace(err)
	}
	for _, key := range keys {
		if strings.EqualFold(queue, strings.Split(key, ".")[1]) {
			return false, nil
		}
	}
	return true, nil
}

func (m *Metadata) QueueCreateTime(queue string) (int64, error) {
	_, stat, err := m.zkClient.Get(m.buildQueuePath(queue))
	if err != nil {
		return 0, errors.Trace(err)
	}
	return stat.Ctime / 1000, nil
}

func (m *Metadata) buildConfigPath(group string, queue string) string {
	return m.groupConfigPath + "/" + group + "." + queue
}

func (m *Metadata) buildQueuePath(queue string) string {
	return m.queuePath + "/" + queue
}

func (m *Metadata) Close() {
	m.zkClient.Close()
}
