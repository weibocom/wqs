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

	"github.com/weibocom/wqs/config"
)

type QueueInfo struct {
	Queue  string         `json:"queue"`
	Ctime  int64          `json:"ctime"`
	Length int64          `json:"length"`
	Groups []*GroupConfig `json:"groups,omitempty"`
}

type QueueConfig struct {
	Queue  string                 `json:"queue"`
	Ctime  int64                  `json:"ctime"`
	Length int64                  `json:"length"`
	Groups map[string]GroupConfig `json:"groups,omitempty"`
}

type GroupInfo struct {
	Group  string         `json:"group"`
	Queues []*GroupConfig `json:"queues,omitempty"`
}

type GroupConfig struct {
	Group string   `json:"group,omitempty"`
	Queue string   `json:"queue,omitempty"`
	Write bool     `json:"write"`
	Read  bool     `json:"read"`
	Url   string   `json:"url"`
	Ips   []string `json:"ips"`
}

type AccumulationInfo struct {
	Group    string `json:"group,omitempty"`
	Queue    string `json:"queue,omitempty"`
	Total    int64  `json:"total,omitempty"`
	Consumed int64  `json:"consumed,omitempty"`
}

func (queueInfo *QueueInfo) String() string {
	result, _ := json.Marshal(queueInfo)
	return string(result)
}

func (groupInfo *GroupInfo) String() string {
	result, _ := json.Marshal(groupInfo)
	return string(result)
}

func (groupConfig *GroupConfig) String() string {
	result, _ := json.Marshal(groupConfig)
	return string(result)
}

type ServiceInfo struct {
	Host   string         `json:"host"`
	Config *config.Config `json:"config"`
}

func (s *ServiceInfo) String() string {
	data, _ := json.Marshal(s)
	return string(data)
}
