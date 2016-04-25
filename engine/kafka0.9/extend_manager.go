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
	log "github.com/cihub/seelog"

	"encoding/json"
	"fmt"
	"strings"

	"github.com/weibocom/wqs/engine/zookeeper"
	"github.com/weibocom/wqs/model"
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

func NewExtendManager(zkAddrs []string, zkRoot string) *ExtendManager {
	zkClient := zookeeper.NewZkClient(zkAddrs)
	groupConfigPath := zkRoot + groupConfigPathSuffix
	queuePath := zkRoot + queuePathSuffix
	return &ExtendManager{zkClient, groupConfigPath, queuePath}
}

//========extend配置相关函数========//

func (em *ExtendManager) AddGroupConfig(group string, queue string, write bool, read bool, url string, ips []string) bool {
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
	return em.zkClient.CreateRec(path, data)
}

func (em *ExtendManager) DeleteGroupConfig(group string, queue string) bool {
	path := em.buildConfigPath(group, queue)
	log.Infof("delete group config, zk path:%s", path)
	return em.zkClient.DeleteRec(path)
}

func (em *ExtendManager) UpdateGroupConfig(group string, queue string, write bool, read bool, url string, ips []string) bool {
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
	return em.zkClient.Set(path, data)
}

func (em *ExtendManager) GetGroupConfig(group string, queue string) *model.GroupConfig {
	path := em.buildConfigPath(group, queue)
	data, _ := em.zkClient.Get(path)
	if len(data) == 0 {
		log.Infof("get group config, zk path:%s, data:null", path)
		return nil
	} else {
		groupConfig := model.GroupConfig{}
		json.Unmarshal([]byte(data), &groupConfig)
		log.Infof("get group config, zk path:%s, data:%s", path, groupConfig.ToJson())
		return &groupConfig
	}
}

func (em *ExtendManager) GetAllGroupConfig() map[string]*model.GroupConfig {
	keys, _ := em.zkClient.Children(em.groupConfigPath)
	allGroupConfig := make(map[string]*model.GroupConfig)
	for _, key := range keys {
		data, _ := em.zkClient.Get(em.groupConfigPath + "/" + key)
		groupConfig := model.GroupConfig{}
		json.Unmarshal([]byte(data), &groupConfig)
		allGroupConfig[key] = &groupConfig
	}
	return allGroupConfig
}

func (em *ExtendManager) GetGroupMap() map[string][]string {
	groupmap := make(map[string][]string)
	keys, _ := em.zkClient.Children(em.groupConfigPath)
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
	return groupmap
}

func (em *ExtendManager) GetQueueMap() map[string][]string {
	queuemap := make(map[string][]string)
	keys, _ := em.zkClient.Children(em.groupConfigPath)
	fmt.Println("keys:", keys)
	queues, _ := em.zkClient.Children(em.queuePath)
	fmt.Println("queues:", queues)
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
	return queuemap
}

func (em *ExtendManager) AddQueue(queue string) bool {
	path := em.buildQueuePath(queue)
	data := ""
	log.Infof("add queue, zk path:%s, data:%s", path, data)
	return em.zkClient.CreateRec(path, data)
}

func (em *ExtendManager) DelQueue(queue string) bool {
	path := em.buildQueuePath(queue)
	log.Infof("del queue, zk path:%s", path)
	return em.zkClient.DeleteRec(path)
}

func (em *ExtendManager) GetQueues() []string {
	queues, _ := em.zkClient.Children(em.queuePath)
	return queues
}

func (em *ExtendManager) buildConfigPath(group string, queue string) string {
	return em.groupConfigPath + "/" + group + "." + queue
}

func (em *ExtendManager) buildQueuePath(queue string) string {
	return em.queuePath + "/" + queue
}
