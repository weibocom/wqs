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
	"net/http"
	"strings"

	log "github.com/Sirupsen/logrus"
	docopt "github.com/docopt/docopt-go"
	"github.com/juju/errors"
	"github.com/weibocom/wqs/utils"
)

func cmdBenchmarkHttp(argv []string) error {
	usage := `usage: benchmark http (set| get)

options:
	set		test http set API Qps;
	get		test http get API Qps;
`
	args, err := docopt.Parse(usage, argv, true, "", false)
	if err != nil {
		return errors.Trace(err)
	}

	if args["get"].(bool) {
		return errors.Trace(benchmarkHttpGet())
	}

	if args["set"].(bool) {
		return errors.Trace(benchmarkHttpSet())
	}

	return nil
}

func benchmarkHttpSet() error {

	url := fmt.Sprintf("http://%s/msg", globalHost)
	sendString := fmt.Sprintf("action=receive&qname=%s&biz=%s&msg=%s",
		globalQueue, globalBiz, utils.GenTestMessage(globalMsgLength))
	log.Printf("Test URL: %s, Data: %s", url, sendString)

	bt := utils.NewBenchmarkTester(globalConcurrentLevel, globalDuration, func(bt *utils.BenchmarkTester, index int) error {

		body := strings.NewReader(sendString)
		resp, err := http.Post(url, "application/x-www-form-urlencoded", body)
		if err != nil {
			return err
		}
		if resp.StatusCode != 200 {
			return fmt.Errorf("http code %d", resp.StatusCode)
		}
		return nil
	})
	return errors.Trace(bt.Run())
}

func benchmarkHttpGet() error {

	url := fmt.Sprintf("http://%s/msg?action=receive&qname=%s&biz=%s",
		globalHost, globalQueue, globalBiz)
	log.Printf("Test URL: %s", url)

	bt := utils.NewBenchmarkTester(globalConcurrentLevel, globalDuration, func(bt *utils.BenchmarkTester, index int) error {

		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		if resp.StatusCode != 200 {
			return fmt.Errorf("http code %d", resp.StatusCode)
		}

		return nil
	})
	return errors.Trace(bt.Run())
}
