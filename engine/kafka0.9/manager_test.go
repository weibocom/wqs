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
	manager := NewManager([]string{"10.77.109.120:9092"}, "../../kafkalib-0.9")
	topics, err := manager.GetTopics()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("topics:%v \n", topics)

	size, err := manager.TopicSize("test-topic")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("topic size %d \n", size)

	// err = manager.CreateTopic("test-temp-topic", 1, 2, "10.77.109.120:2181")
	// if err != nil {
	// 	// t.Fatal(err)
	// }

	// err = manager.DeleteTopic("test-temp-topic", "10.77.109.120:2181")
	// if err != nil {
	// 	// t.Fatal(err)
	// }

}
