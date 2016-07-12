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
	"encoding/json"

	"github.com/rcrowley/go-metrics"
)

func SaveDataToString() string {
	save := make(map[string]int64)
	if reg == nil {
		return ""
	}

	snap := reg.snapshot()
	if snap == nil {
		return ""
	}

	snap.Each(func(key string, i interface{}) {
		if counter, ok := i.(metrics.Counter); ok {
			save[key] = counter.Count()
		}
	})
	data, _ := json.Marshal(save)
	return string(data)
}

func LoadDataFromBytes(s []byte) error {

	if len(s) == 0 {
		return nil
	}

	data := make(map[string]int64)
	if err := json.Unmarshal(s, &data); err != nil {
		return err
	}
	for key, value := range data {
		AddCounter(key, value)
	}
	return nil
}
