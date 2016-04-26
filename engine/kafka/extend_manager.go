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
	"strings"

	"github.com/weibocom/wqs/engine/zookeeper"
	"github.com/weibocom/wqs/model"

	log "github.com/cihub/seelog"
	"github.com/juju/errors"
)

const (
	groupConfigPathSuffix = "/groupconfig"
	queuePathSuffix       = "/queue"

	emptyString = ""
)

type ExtendManager struct {
	zkClient        *zookeeper.ZkClient
	groupConfigPath string
	queuePath       string
}

func NewExtendManager(zkAddrs []string, zkRoot string) (*ExtendManager, error) {
	zk, err := zookeeper.NewZkClient(zkAddrs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &ExtendManager{
		zkClient:        zk,
		groupConfigPath: fmt.Sprintf("%s%s", zkRoot, groupConfigPathSuffix),
		queuePath:       fmt.Sprintf("%s%s", zkRoot, queuePathSuffix),
	}, nil
}

//========extend配置相关函数========//

func (em *ExtendManager) AddGroupConfig(group string, queue string,
	write bool, read bool, url string, ips []string) error {

	path := em.buildConfigPath(group, queue)
	groupConfig := model.GroupConfig{
		Group: group,
		Queue: queue,
		Write: write,
		Read:  read,
		Url:   url,
		Ips:   ips,
	}
	data := groupConfig.ToJson()
	log.Infof("add group config, zk path:%s, data:%s", path, data)
	err := em.zkClient.CreateRec(path, data)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (em *ExtendManager) DeleteGroupConfig(group string, queue string) error {
	path := em.buildConfigPath(group, queue)
	log.Infof("delete group config, zk path:%s", path)
	err := em.zkClient.DeleteRec(path)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (em *ExtendManager) UpdateGroupConfig(group string, queue string,
	write bool, read bool, url string, ips []string) error {

	path := em.buildConfigPath(group, queue)
	groupConfig := model.GroupConfig{
		Group: group,
		Queue: queue,
		Write: write,
		Read:  read,
		Url:   url,
		Ips:   ips,
	}
	data := groupConfig.ToJson()
	log.Infof("update group config, zk path:%s, data:%s", path, data)
	if err := em.zkClient.Set(path, data); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (em *ExtendManager) GetGroupConfig(group string, queue string) (*model.GroupConfig, error) {
	path := em.buildConfigPath(group, queue)
	data, _, err := em.zkClient.Get(path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(data) == 0 {
		log.Infof("get group config, zk path:%s, data:null, err:%s", path, err)
		return nil, nil
	}

	groupConfig := model.GroupConfig{}
	err = json.Unmarshal(data, &groupConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Infof("get group config, zk path:%s, data:%s", path, groupConfig.ToJson())
	return &groupConfig, nil
}

func (em *ExtendManager) GetAllGroupConfig() (map[string]*model.GroupConfig, error) {

	allGroupConfig := make(map[string]*model.GroupConfig)
	keys, _, err := em.zkClient.Children(em.groupConfigPath)
	if err != nil {
		return allGroupConfig, errors.Trace(err)
	}

	for _, key := range keys {
		data, _, err := em.zkClient.Get(fmt.Sprintf("%s/%s", em.groupConfigPath, key))
		if err != nil {
			return allGroupConfig, errors.Trace(err)
		}
		groupConfig := &model.GroupConfig{}
		err = json.Unmarshal(data, groupConfig)
		if err != nil {
			return allGroupConfig, errors.Trace(err)
		}
		allGroupConfig[key] = groupConfig
	}
	return allGroupConfig, nil
}

func (em *ExtendManager) GetGroupMap() (map[string][]string, error) {
	groupmap := make(map[string][]string)
	keys, _, err := em.zkClient.Children(em.groupConfigPath)
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

func (em *ExtendManager) GetQueueMap() (map[string][]string, error) {
	queuemap := make(map[string][]string)
	keys, _, err := em.zkClient.Children(em.groupConfigPath)
	if err != nil {
		return queuemap, errors.Trace(err)
	}
	queues, _, err := em.zkClient.Children(em.queuePath)
	if err != nil {
		return queuemap, errors.Trace(err)
	}
	for _, k := range keys {
		queue := strings.Split(k, ".")[1]
		groups, ok := queuemap[queue]
		if ok {
			groups = append(groups, strings.Split(k, ".")[0])
			queuemap[queue] = groups
		} else {
			tempgroups := make([]string, 0)
			tempgroups = append(tempgroups, strings.Split(k, ".")[0])
			queuemap[queue] = tempgroups
		}
	}
	for _, queue := range queues {
		_, ok := queuemap[queue]
		if ok {
			continue
		} else {
			queuemap[queue] = make([]string, 0)
		}
	}
	return queuemap, nil
}

func (em *ExtendManager) AddQueue(queue string) error {
	path := em.buildQueuePath(queue)
	data := ""
	log.Infof("add queue, zk path:%s, data:%s", path, data)
	err := em.zkClient.CreateRec(path, data)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (em *ExtendManager) DelQueue(queue string) error {
	path := em.buildQueuePath(queue)
	log.Infof("del queue, zk path:%s", path)
	if err := em.zkClient.DeleteRec(path); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (em *ExtendManager) GetQueues() []string {
	queues, _, _ := em.zkClient.Children(em.queuePath)
	return queues
}

func (em *ExtendManager) buildConfigPath(group string, queue string) string {
	return em.groupConfigPath + "/" + group + "." + queue
}

func (em *ExtendManager) buildQueuePath(queue string) string {
	return em.queuePath + "/" + queue
}
