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
	"github.com/rcrowley/go-metrics"
	"github.com/weibocom/wqs/log"
)

const (
	profileWriter = "profile"
)

type profile struct {
}

func newProfileWriter() statWriter {
	return &profile{}
}

func (p *profile) Write(snap metrics.Registry) error {

	snap.Each(func(key string, i interface{}) {
		switch m := i.(type) {
		case metrics.Counter:
			log.Profile("%s: %d|Counter", key, m.Count())
		case metrics.Meter:
			log.Profile("%s: %.2f|Meter", key, m.Rate1())
		case metrics.Timer:
			log.Profile("%s: %.2f|Timer", key, m.Rate1())
		case metrics.Gauge:
			log.Profile("%s: %d|Gauge", key, m.Value())
		case metrics.GaugeFloat64:
			log.Profile("%s: %.2f|GaugeFloat64", key, m.Value())
		case metrics.Histogram:
			log.Profile("%s: %.2f|Histogram", key, m.Mean())
		default:
			return
		}
	})
	return nil
}
