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
package term

import (
	"fmt"
	"sort"
	"time"
)

type Stat struct {
	ts   int64
	data map[string]int64
}

type MetricsStat struct {
	Service  string            `json:"service"`
	Endpoint string            `json:"endpoint"`
	Data     map[string]*Count `json:"data"`
}

type Count struct {
	Cnt int64 `json:"count"`
}

func (s *Stat) List() (ls []string) {
	for k, v := range s.data {
		ls = append(ls, fmt.Sprintf("[%s] %s:%d", time.Unix(s.ts, 0).Format("20106-01-02 15:04:05"), k, v))
	}
	sort.Strings(ls)
	return
}
