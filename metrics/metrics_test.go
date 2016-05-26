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

	"github.com/weibocom/wqs/config"
)

func TestAdd(t *testing.T) {
	cfg, err := config.NewConfigFromFile("../config.properties")
	if err != nil {
		t.Fatal(err)
	}
	err = Init(cfg)
	if err != nil {
		t.Fatal(err)
	}

	Add("queue#group-sent", 1, 12)
	Add("queue#group-sent", 1, 1200)
	Add("queue#group-sent", 1, 120)
	Add("queue#group-sent", 1, 1)
	Add("queue#group-sent", 1, 122)
	Add("queue#group-sent", 1, 700)
	StatisticSend("queue1", "default", 12)
	StatisticSend("queue2", "default", 12)
	StatisticSend("queue3", "default", 12)
	StatisticRecv("queue1", "default", 12)
	StatisticRecv("queue2", "default", 12)
	StatisticRecv("queue3", "default", 12)
	StatisticRecv("queue4", "default", 12)
}
