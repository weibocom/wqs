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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

//40 years' milliseconds
const baseTime = 40 * 365 * 24 * 3600 * 1000

var (
	errBadMessageID = errors.New("bad message id")
)

type idGenerator struct {
	t        uint64
	sequence uint64
	id       uint64
	ticker   <-chan time.Time
	mu       sync.Mutex
}

// TODO 待优化，可以有无锁实现
func (i *idGenerator) Get() (id uint64) {
	i.mu.Lock()
Loop:
	for {
		select {
		case t := <-i.ticker:
			i.t = uint64(t.UnixNano()/1e6 - baseTime)
			i.sequence = 0
		default:
			break Loop
		}
	}

	// 0                   1                   2                   3
	// 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
	//+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	//|                  Millisecond TimeStamp                        |
	//+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	//|               |     ID            |  Sequence                 |
	//+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
	id = (i.t << 24) | ((i.id & 0x3FF) << 14) | (i.sequence & 0x3FFF)
	i.sequence++
	i.mu.Unlock()
	return id
}

func newIDGenerator(id uint64) *idGenerator {
	return &idGenerator{
		t:      uint64(time.Now().UnixNano()/1e6 - baseTime),
		id:     id,
		ticker: time.Tick(time.Millisecond),
	}
}

type messageId struct {
	queue     string
	group     string
	idc       string
	partition int32
	offset    int64
	sequence  uint64
}

func (m *messageId) Parse(id string) error {

	tokens := strings.SplitN(id, ":", 6)
	if len(tokens) != 6 {
		return errBadMessageID
	}
	// 目前不需要解析 sequence
	m.queue = tokens[1]
	m.group = tokens[2]
	m.idc = tokens[5]

	partition, err := strconv.ParseInt(tokens[3], 16, 32)
	if err != nil {
		return errBadMessageID
	}
	m.partition = int32(partition)

	m.offset, err = strconv.ParseInt(tokens[4], 16, 64)
	if err != nil {
		return errBadMessageID
	}
	return nil
}

func (m *messageId) String() string {
	return fmt.Sprintf("%x:%s:%s:%x:%x:%s",
		m.sequence, m.queue, m.group, m.partition, m.offset, m.idc)
}
