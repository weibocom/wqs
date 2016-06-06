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

package metrics

import (
	"testing"
)

func TestMonitor(t *testing.T) {
	/*
		monitor := NewMonitor("localhost:6379")

		for i := 0; i < 10; i++ {
			monitor.StatisticSend("test_queue", "test_group1", 10)
			time.Sleep(time.Millisecond * 1000)
			end := time.Now().Unix()
			start := end - 60 //1min
			fmt.Println(monitor.GetSendMetrics("test_queue", "test_group1", start, end, 1))
		}
	*/
}
