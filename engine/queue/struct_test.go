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
	"fmt"
	"testing"
)

const (
	testString1 = "11111111111111111"
	testString2 = "2222222222222222222222222"
	testString3 = "33333333333333333333333333333333333"
)

func TestStruct(t *testing.T) {
	groupConfig := GroupConfig{
		Group: "test_group",
		Queue: "test_queue",
		Write: true,
		Read:  true,
		Url:   "test_group.test_queue.xx.com",
		Ips:   []string{"172.0.0.1", "172.0.0.2"},
	}
	fmt.Println(groupConfig.String())

	groups := make([]GroupConfig, 0)
	groups = append(groups, groupConfig)
	queueInfo := QueueInfo{Queue: "test_queue", Ctime: 1234567890, Groups: groups}
	fmt.Println(queueInfo.String())

	groupConfig.Queue = "test_queue"
	groupConfig.Group = ""

	queues := make([]*GroupConfig, 0)
	queues = append(queues, &groupConfig)
	groupInfo := GroupInfo{Group: "test_group", Queues: queues}
	fmt.Println(groupInfo.String())
}

func stringDummy(s string) {

}

func BenchmarkStringSprintf(b *testing.B) {

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := fmt.Sprintf("%s/%s/%s", testString1, testString2, testString3)
		stringDummy(s)
	}
}

func BenchmarkStringPlus(b *testing.B) {

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := testString1 + "/" + testString2 + "/" + testString3
		stringDummy(s)
	}
}
