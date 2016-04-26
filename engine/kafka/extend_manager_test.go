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
	"fmt"
	"testing"
)

func TestExtendManager(t *testing.T) {

	manager := NewExtendManager([]string{"10.77.109.121:2181"}, "/wqs")
	// fmt.Println(manager.AddGroupConfig("test_group", "test_queue", true, true, "test_group.test_queue", []string{"192.168.0.1", "192.168.0.2"}))
	// fmt.Println(manager.AddGroupConfig("test_group", "test_queue1", true, true, "test_group.test_queue", []string{"192.168.0.1", "192.168.0.2"}))
	// fmt.Println(manager.AddGroupConfig("test_group", "test_queue2", true, true, "test_group.test_queue", []string{"192.168.0.1", "192.168.0.2"}))
	// fmt.Println(manager.AddGroupConfig("test_group1", "test_queue", true, true, "test_group1.test_queue1", []string{"192.168.0.1", "192.168.0.2"}))
	// fmt.Println(manager.AddGroupConfig("test_group2", "test_queue", true, true, "test_group1.test_queue1", []string{"192.168.0.1", "192.168.0.2"}))
	// fmt.Println(manager.AddGroupConfig("test_group3", "test_queue", true, true, "test_group1.test_queue1", []string{"192.168.0.1", "192.168.0.2"}))
	fmt.Println(manager.GetAllGroupConfig())
	fmt.Println(manager.GetQueues())
	fmt.Println(manager.GetQueueMap())

}
