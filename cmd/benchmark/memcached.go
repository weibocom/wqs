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

	docopt "github.com/docopt/docopt-go"
	"github.com/juju/errors"
	"github.com/smallfish/memcache"
	"github.com/weibocom/wqs/utils"
)

func cmdBenchmarkMC(argv []string) error {
	usage := `usage: benchmark mc (set| get | mix)

options:
	set		test memcached set API Qps;
	get		test memcached get API Qps;
	mix		test memcached get and set API Qps in the same time;
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

	if args["mix"].(bool) {
		return errors.Trace(benchmarkMCMix())
	}
	return nil
}

func benchmarkMCSet() error {
	var err error
	key := fmt.Sprintf("%s.%s", globalGroup, globalQueue)
	sendString := utils.GenTestMessage(globalMsgLength)
	log.Printf("Test Key: %s, Data: %s", key, sendString)

	conns := make([]*memcache.Connection, globalConcurrentLevel)
	for i := 0; i < globalConcurrentLevel; i++ {
		if conns[i], err = memcache.Connect(globalHost); err != nil {
			return errors.Trace(err)
		}
	}

	defer func() {
		for i := 0; i < globalConcurrentLevel; i++ {
			conns[i].Close()
		}
	}()

	bt := utils.NewBenchmarkTester(globalConcurrentLevel, globalDuration, func(bt *utils.BenchmarkTester, index int) error {
		stored, err := conns[index].Set(key, 0, 0, []byte(sendString))
		if err != nil {
			return err
		}
		if !stored {
			return errors.New("not stored")
		}
		return nil
	}, nil)
	return errors.Trace(bt.Run())
}

func benchmarkMCGet() error {
	var err error
	key := fmt.Sprintf("%s.%s", globalGroup, globalQueue)
	log.Printf("Test Key: %s", key)

	conns := make([]*memcache.Connection, globalConcurrentLevel)
	for i := 0; i < globalConcurrentLevel; i++ {
		if conns[i], err = memcache.Connect(globalHost); err != nil {
			return errors.Trace(err)
		}
	}

	defer func() {
		for i := 0; i < globalConcurrentLevel; i++ {
			conns[i].Close()
		}
	}()

	bt := utils.NewBenchmarkTester(globalConcurrentLevel, globalDuration, func(bt *utils.BenchmarkTester, index int) error {

		if _, err := conns[index].Get(key); err != nil {
			return err
		}
		return nil
	}, nil)
	return errors.Trace(bt.Run())
}

func benchmarkMCMix() error {
	var err error
	key := fmt.Sprintf("%s.%s", globalGroup, globalQueue)
	sendString := utils.GenTestMessage(globalMsgLength)
	log.Printf("Test Key: %s, Data: %s", key, sendString)

	conns := make([]*memcache.Connection, globalConcurrentLevel)
	for i := 0; i < globalConcurrentLevel; i++ {
		if conns[i], err = memcache.Connect(globalHost); err != nil {
			return errors.Trace(err)
		}
	}

	defer func() {
		for i := 0; i < globalConcurrentLevel; i++ {
			conns[i].Close()
		}
	}()

	bt := utils.NewBenchmarkTester(globalConcurrentLevel, globalDuration, func(bt *utils.BenchmarkTester, index int) error {
		if index%2 == 0 {
			stored, err := conns[index].Set(key, 0, 0, []byte(sendString))
			if err != nil {
				return err
			}
			if !stored {
				return errors.New("not stored")
			}
		} else {
			if _, err := conns[index].Get(key); err != nil {
				return err
			}
		}
		return nil
	}, nil)
	return errors.Trace(bt.Run())
}
