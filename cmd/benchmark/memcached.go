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

package main

import (
	"fmt"
	"log"

	"github.com/bradfitz/gomemcache/memcache"
	docopt "github.com/docopt/docopt-go"
	"github.com/juju/errors"
	"github.com/weibocom/wqs/utils"
)

func cmdBenchmarkMC(argv []string) error {
	usage := `usage: benchmark mc (set| get)

options:
	set		test memcached set API Qps;
	get		test memcached get API Qps;
`
	args, err := docopt.Parse(usage, argv, true, "", false)
	if err != nil {
		return errors.Trace(err)
	}

	if args["get"].(bool) {
		return errors.Trace(benchmarkMCGet())
	}

	if args["set"].(bool) {
		return errors.Trace(benchmarkMCSet())
	}

	return nil
}

func benchmarkMCSet() error {

	key := fmt.Sprintf("%s.%s", globalQueue, globalBiz)
	sendString := utils.GenTestMessage(globalMsgLength)
	log.Printf("Test Key: %s, Data: %s", key, sendString)

	mc := memcache.New(globalHost)

	bt := utils.NewBenchmarkTester(globalConcurrentLevel, globalDuration, func(bt *utils.BenchmarkTester, index int) error {

		err := mc.Set(&memcache.Item{Key: key, Value: []byte(sendString)})
		if err != nil {
			return err
		}

		return nil
	}, nil)
	return errors.Trace(bt.Run())
}

func benchmarkMCGet() error {

	key := fmt.Sprintf("%s.%s", globalQueue, globalBiz)
	log.Printf("Test Key: %s", key)

	mc := memcache.New(globalHost)

	bt := utils.NewBenchmarkTester(globalConcurrentLevel, globalDuration, func(bt *utils.BenchmarkTester, index int) error {

		_, err := mc.Get(key)
		if err != nil {
			return err
		}

		return nil
	}, nil)
	return errors.Trace(bt.Run())
}
