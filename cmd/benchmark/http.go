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
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	docopt "github.com/docopt/docopt-go"
	"github.com/juju/errors"
	"github.com/weibocom/wqs/utils"
)

func cmdBenchmarkHttp(argv []string) error {
	usage := `usage: benchmark http (set| get| latency)

options:
	set		test http set API Qps;
	get		test http get API Qps;
	latency test http API latency;
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

	if args["latency"].(bool) {
		return errors.Trace(benchmarkHttpLatency())
	}

	return nil
}

func benchmarkHttpSet() error {

	url := fmt.Sprintf("http://%s/msg", globalHost)
	sendString := fmt.Sprintf("action=send&queue=%s&group=%s&msg=%s",
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
	}, nil)
	return errors.Trace(bt.Run())
}

func benchmarkHttpGet() error {

	url := fmt.Sprintf("http://%s/msg?action=receive&queue=%s&group=%s",
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
	}, nil)
	return errors.Trace(bt.Run())
}

type message struct {
	Action string `json:"action"`
	Msg    string `json:"msg"`
}

func benchmarkHttpLatency() error {

	getURL := fmt.Sprintf("http://%s/msg?action=receive&queue=%s&group=%s",
		globalHost, globalQueue, globalBiz)
	setURL := fmt.Sprintf("http://%s/msg", globalHost)

	result := new([6]int64)
	statis := func(s int64) {
		switch {
		case s < 2:
			atomic.AddInt64(&(result[0]), 1)
		case s >= 2 && s < 5:
			atomic.AddInt64(&(result[1]), 1)
		case s >= 5 && s < 10:
			atomic.AddInt64(&(result[2]), 1)
		case s >= 10 && s < 20:
			atomic.AddInt64(&(result[3]), 1)
		case s >= 20 && s < 50:
			atomic.AddInt64(&(result[4]), 1)
		default:
			atomic.AddInt64(&(result[5]), 1)
		}
	}

	clean := func(bt *utils.BenchmarkTester) {
		total := int64(0)
		for _, i := range result {
			total += i
		}
		fmt.Printf("Benchmark Latency Result: %d\n", total)
		fmt.Printf("\t%2d%% ops < 2ms\n", result[0]*100/total)
		fmt.Printf("\t%2d%% ops 2-5ms\n", result[1]*100/total)
		fmt.Printf("\t%2d%% ops 5-10ms\n", result[2]*100/total)
		fmt.Printf("\t%2d%% ops 10-20ms\n", result[3]*100/total)
		fmt.Printf("\t%2d%% ops 20-50ms\n", result[4]*100/total)
		fmt.Printf("\t%2d%% ops >50ms\n", result[5]*100/total)
		fmt.Printf("\n\n")
	}

	bt := utils.NewBenchmarkTester(globalConcurrentLevel, globalDuration, func(bt *utils.BenchmarkTester, index int) error {

		if index%2 == 0 {
			//producer
			body := &bytes.Buffer{}
			fmt.Fprintf(body, "action=send&queue=%s&group=%s&msg=%d",
				globalQueue, globalBiz, time.Now().UnixNano())
			resp, err := http.Post(setURL, "application/x-www-form-urlencoded", body)
			if err != nil {
				return err
			}
			if resp.StatusCode != 200 {
				return fmt.Errorf("http code %d", resp.StatusCode)
			}
			return nil
		}

		resp, err := http.Get(getURL)
		if err != nil {
			return err
		}
		if resp.StatusCode != 200 {
			return fmt.Errorf("http code %d", resp.StatusCode)
		}
		recvTime := time.Now().UnixNano()
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("resp read :%s", err)
		}
		msg := &message{}
		err = json.Unmarshal(data, msg)
		if err != nil {
			return err
		}
		sendTime, err := strconv.ParseInt(msg.Msg, 10, 64)
		if err != nil {
			return err
		}
		diff := recvTime - sendTime
		statis(diff / 1000000)
		return nil
	}, clean)
	return errors.Trace(bt.Run())
}
