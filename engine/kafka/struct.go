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

import "encoding/json"

type brokerConfig struct {
	JmxPort   int32    `json:"jmx_port,omitempty"`
	TimeStamp int64    `json:"timestamp,string"`
	EndPoints []string `json:"endpoints"`
	Host      string   `json:"host"`
	Version   int32    `json:"version"`
	Port      int32    `json:"port"`
}

func (b *brokerConfig) LoadFromBytes(data []byte) error {
	return json.Unmarshal(data, b)
}

type topicConfig struct {
	// {"segment.bytes":"104857600","compression.type":"uncompressed","cleanup.policy":"compact"}}
	// empty object by default
}

type topicInfo struct {
	Version int32       `json:"version"`
	Config  topicConfig `json:"config"`
}

func (i *topicInfo) String() string {
	data, _ := json.Marshal(i)
	return string(data)
}

type partitonAssignment map[string][]int32

type topicPatitionConfig struct {
	Version    int32              `json:"version"`
	Partitions partitonAssignment `json:"partitions"`
}

func (c *topicPatitionConfig) LoadFromBytes(data []byte) error {
	return json.Unmarshal(data, c)
}

func (c *topicPatitionConfig) String() string {
	data, _ := json.Marshal(c)
	return string(data)
}
