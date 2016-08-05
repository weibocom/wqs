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

import "github.com/weibocom/wqs/config"

type Queue interface {
	Create(queue string, idcs []string) error
	Update(queue string) error
	Delete(queue string) error
	Lookup(queue string, group string) ([]*QueueInfo, error)
	AddGroup(group string, queue string, write bool, read bool, url string, ips []string) error
	UpdateGroup(group string, queue string, write bool, read bool, url string, ips []string) error
	DeleteGroup(group string, queue string) error
	LookupGroup(group string) ([]*GroupInfo, error)
	GetSingleGroup(group string, queue string) (*GroupConfig, error)
	SendMessage(queue string, group string, data []byte, flag uint64) (id string, err error)
	RecvMessage(queue string, group string) (id string, data []byte, flag uint64, err error)
	AckMessage(queue string, group string, id string) error
	AccumulationStatus() ([]AccumulationInfo, error)
	Proxys() (map[string]string, error)
	GetProxyConfigByID(id int) (string, error)
	UpTime() int64
	Version() string
	Close()
}

func NewQueue(config *config.Config, version string) (Queue, error) {
	return newQueue(config, version)
}
