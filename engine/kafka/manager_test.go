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
	// "time"
)

func TestManager(t *testing.T) {
	var err error
	manager, _ := NewManager([]string{"localhost:9092"}, "../../kafkalib", nil)
	topics, err := manager.GetTopics()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("topics:%v \n", topics)

	size, err := manager.FetchTopicSize("test-queue")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("topic size %d \n", size)
	topicOffsets, _ := manager.FetchTopicOffsets("queue1")
	for i := int32(0); i < int32(len(topicOffsets)); i++ {
		fmt.Printf("%d:%d ", i, topicOffsets[i])
	}
	fmt.Println()
	groupOffsets, _ := manager.FetchGroupOffsets("queue1", "group1")
	for i := int32(0); i < int32(len(groupOffsets)); i++ {
		fmt.Printf("%d:%d ", i, groupOffsets[i])
	}
	fmt.Println()
	fmt.Println(manager.Accumulation("queue1", "group1"))

	// err = manager.CreateTopic("test-temp-topic", 1, 2, "10.77.109.120:2181")
	// if err != nil {
	// 	// t.Fatal(err)
	// }

	// err = manager.DeleteTopic("test-temp-topic", "10.77.109.120:2181")
	// if err != nil {
	// 	// t.Fatal(err)
	// }

}
