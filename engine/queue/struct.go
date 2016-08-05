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
	"bytes"
	"encoding/json"

	"github.com/weibocom/wqs/config"
)

type QueueInfo struct {
	Queue  string        `json:"queue"`
	Ctime  int64         `json:"ctime"`
	Length int64         `json:"length"`
	Groups []GroupConfig `json:"groups,omitempty"`
}

type queueInfoSlice []*QueueInfo

func (q queueInfoSlice) Len() int {
	return len(q)
}

func (q queueInfoSlice) Less(i, j int) bool {
	return q[i].Queue < q[j].Queue
}

func (q queueInfoSlice) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

type groupSlice []GroupConfig

func (q groupSlice) Len() int {
	return len(q)
}

func (q groupSlice) Less(i, j int) bool {
	return q[i].Group < q[j].Group
}

func (q groupSlice) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

type QueueConfig struct {
	Queue  string                 `json:"queue"`
	Ctime  int64                  `json:"ctime"`
	Length int64                  `json:"length"`
	Groups map[string]GroupConfig `json:"groups,omitempty"`
	Idcs   []string               `json:"idcs,omitempty"`
}

func (q *QueueConfig) String() string {
	d, _ := json.Marshal(q)
	return string(d)
}

func (q *QueueConfig) Parse(data []byte) error {
	return json.Unmarshal(data, q)
}

type AccumulationInfo struct {
	Group    string `json:"group,omitempty"`
	Queue    string `json:"queue,omitempty"`
	Total    int64  `json:"total,omitempty"`
	Consumed int64  `json:"consumed,omitempty"`
}

type GroupInfo struct {
	Group  string         `json:"group"`
	Queues []*GroupConfig `json:"queues,omitempty"`
}

func (i *QueueInfo) String() string {
	data, _ := json.Marshal(i)
	return string(data)
}

func (i *GroupInfo) String() string {
	data, _ := json.Marshal(i)
	return string(data)
}

type GroupConfig struct {
	Group string   `json:"group,omitempty"`
	Queue string   `json:"queue,omitempty"`
	Write bool     `json:"write"`
	Read  bool     `json:"read"`
	Url   string   `json:"url"`
	Ips   []string `json:"ips"`
}

func (c *GroupConfig) Load(data []byte) error {
	return json.Unmarshal(data, c)
}

func (c *GroupConfig) String() string {
	data, _ := json.Marshal(c)
	return string(data)
}

type proxyInfo struct {
	Host   string `json:"host"`
	Config string `json:"config"`
	config *config.Config
}

func (i *proxyInfo) Load(data []byte) error {
	return json.Unmarshal(data, i)
}

func (i *proxyInfo) String() string {
	i.Config = i.config.String()
	buff := &bytes.Buffer{}
	json.NewEncoder(buff).Encode(i)
	return buff.String()
}
