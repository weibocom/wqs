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
	"github.com/weibocom/wqs/config"
	"github.com/weibocom/wqs/metrics"
)

type Queue interface {
	Create(queue string) error
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
	GetSendMetrics(queue string, group string, start int64, end int64, intervalnum int64) (metrics.MetricsObj, error)
	GetReceiveMetrics(queue string, group string, start int64, end int64, intervalnum int64) (metrics.MetricsObj, error)
	AccumulationStatus() ([]AccumulationInfo, error)
	Close()
}

func NewQueue(config *config.Config) (Queue, error) {
	return newQueue(config)
}
