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
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"strings"
	"time"

	docopt "github.com/docopt/docopt-go"
	"github.com/elodina/go_kafka_client"
	"github.com/elodina/siesta"
	siesta_producer "github.com/elodina/siesta-producer"
	"github.com/juju/errors"
	"github.com/weibocom/wqs/utils"
)

var usage = `usage: go_kafka_client_benchmark [options] <command>

options:
	--cpu=<NUM>              set NCPU 
	--len=<LENGTH>           set message length (default 500)
	--broker=<broker_list>   set broker list IP1:PORT,IP2:PORT
	--zk=<zookeeper_list>    set zookeeper list IP1:PORT,IP2:PORT
	--topic=<topic>          set test topic
	--cc=<concurrent_level>  set concurrent goroutine number
	--time=<time>            set benchmark duration(default 60 second)
	--addr=<http_addr>       set debug http addr

commands:
	set0	    set with ack(fastest way so far)
	set1    set without ack
	get
`

var (
	globalMsgLength       = 500
	globalBrokerList      = ""
	globalZkList          = ""
	globalTopic           = ""
	globalBiz             = ""
	globalConcurrentLevel = 500
	globalDuration        = 60
)

func fatal(msg interface{}) {

	switch msg.(type) {
	case string:
		log.Fatal(msg)
	case error:
		log.Fatal(errors.ErrorStack(msg.(error)))
	}
}

func main() {

	args, err := docopt.Parse(usage, nil, true, "go_kafka_client_benchmark v0.1", true)
	if err != nil {
		fatal(err)
	}

	if v := args["--broker"]; v != nil {
		globalBrokerList = v.(string)
	}

	if v := args["--zk"]; v != nil {
		globalZkList = v.(string)
	}

	if v := args["--topic"]; v != nil {
		globalTopic = v.(string)
	}

	ncpu, err := utils.GetIntFromArgs(args, "--cpu", runtime.NumCPU())
	if err != nil {
		fatal(err)
	}

	globalMsgLength, err = utils.GetIntFromArgs(args, "--len", globalMsgLength)
	if err != nil {
		fatal(err)
	}

	globalConcurrentLevel, err = utils.GetIntFromArgs(args, "--cc", globalConcurrentLevel)
	if err != nil {
		fatal(err)
	}

	globalDuration, err = utils.GetIntFromArgs(args, "--time", globalDuration)
	if err != nil {
		fatal(err)
	}

	runtime.GOMAXPROCS(ncpu)

	if v := args["--addr"]; v != nil {
		httpAddr := v.(string)
		go func() {
			fatal(errors.Trace(http.ListenAndServe(httpAddr, nil)))
		}()
	}
	cmd := args["<command>"].(string)

	err = runCommand(cmd)
	if err != nil {
		fatal(err)
	}
}

func runCommand(cmd string) (err error) {

	switch cmd {
	case "get":
		return errors.Trace(cmdBenchmarkGet())
	case "set0":
		return errors.Trace(cmdBenchmarkSet())
	case "set1":
		return errors.Trace(cmdBenchmarkSetNoAck())
	case "set2":
		return errors.Trace(cmdBenchmarkSet2())
	}
	return errors.NotSupportedf("See 'go_kafka_client_benchmark -h', command %s", cmd)
}

func cmdBenchmarkSet() error {

	if len(globalBrokerList) == 0 {
		return errors.NotValidf("broker list")
	}
	if len(globalTopic) == 0 {
		return errors.NotValidf("Topic")
	}

	sendString := utils.GenTestMessage(globalMsgLength)
	producerConfig := siesta_producer.NewProducerConfig()
	producerConfig.Linger = time.Millisecond
	connConfig := siesta.NewConnectorConfig()
	brokerList := strings.Split(globalBrokerList, ",")
	producerConfig.BrokerList = brokerList
	connConfig.BrokerList = brokerList

	log.Printf("%v", brokerList)
	connector, err := siesta.NewDefaultConnector(connConfig)
	if err != nil {
		return errors.Trace(err)
	}
	//	go func() {
	//		timeout := time.Tick(producerConfig.MetadataExpire / 2)
	//		for {
	//			<-timeout
	//			connector.RefreshMetadata([]string{globalTopic})
	//		}
	//	}()

	producer := siesta_producer.NewKafkaProducer(producerConfig,
		siesta_producer.ByteSerializer,
		siesta_producer.ByteSerializer,
		connector)

	bt := utils.NewBenchmarkTester(globalConcurrentLevel, globalDuration,
		func(bt *utils.BenchmarkTester, index int) error {

			record := &siesta_producer.ProducerRecord{
				Topic: globalTopic,
				Value: []byte(sendString),
			}

			recordMetadata := <-producer.Send(record)
			if recordMetadata.Error == siesta.ErrNoError {
				return nil
			}
			return recordMetadata.Error
		}, nil)
	return errors.Trace(bt.Run())
}

func cmdBenchmarkSetNoAck() error {

	if len(globalBrokerList) == 0 {
		return errors.NotValidf("broker list")
	}
	if len(globalTopic) == 0 {
		return errors.NotValidf("Topic")
	}

	sendString := utils.GenTestMessage(globalMsgLength)
	producerConfig := siesta_producer.NewProducerConfig()
	producerConfig.ClientID = "Benchmark"
	producerConfig.RequiredAcks = 0
	connConfig := siesta.NewConnectorConfig()
	brokerList := strings.Split(globalBrokerList, ",")
	producerConfig.BrokerList = brokerList
	connConfig.BrokerList = brokerList

	log.Printf("%v", brokerList)
	connector, err := siesta.NewDefaultConnector(connConfig)
	if err != nil {
		return errors.Trace(err)
	}

	producer := siesta_producer.NewKafkaProducer(producerConfig,
		siesta_producer.ByteSerializer,
		siesta_producer.ByteSerializer,
		connector)

	bt := utils.NewBenchmarkTester(globalConcurrentLevel, globalDuration,
		func(bt *utils.BenchmarkTester, index int) error {

			record := &siesta_producer.ProducerRecord{
				Topic: globalTopic,
				Value: []byte(sendString),
			}

			recordMetadata := <-producer.Send(record)
			if recordMetadata.Error == siesta.ErrNoError {
				return nil
			}
			return recordMetadata.Error
		}, nil)
	return errors.Trace(bt.Run())
}

func cmdBenchmarkGet() error {

	if len(globalZkList) == 0 {
		return errors.NotValidf("zookeeper list")
	}

	if len(globalTopic) == 0 {
		return errors.NotValidf("Topic")
	}

	message := make(chan *go_kafka_client.Message)
	consumConfig := go_kafka_client.DefaultConsumerConfig()
	consumConfig.Groupid = "BenchmarkGroup-Test"
	consumConfig.AutoOffsetReset = go_kafka_client.SmallestOffset
	zkConfig := go_kafka_client.NewZookeeperConfig()
	zkConfig.ZookeeperConnect = strings.Split(globalZkList, ",")
	consumConfig.Coordinator = go_kafka_client.NewZookeeperCoordinator(zkConfig)

	consumConfig.Strategy =
		func(_ *go_kafka_client.Worker, msg *go_kafka_client.Message,
			id go_kafka_client.TaskId) go_kafka_client.WorkerResult {

			message <- msg
			return go_kafka_client.NewSuccessfulResult(id)
		}

	consumConfig.WorkerFailureCallback =
		func(_ *go_kafka_client.WorkerManager) go_kafka_client.FailedDecision {
			return go_kafka_client.CommitOffsetAndContinue
		}

	consumConfig.WorkerFailedAttemptCallback =
		func(_ *go_kafka_client.Task, _ go_kafka_client.WorkerResult) go_kafka_client.FailedDecision {
			return go_kafka_client.CommitOffsetAndContinue
		}

	consumer := go_kafka_client.NewConsumer(consumConfig)
	topicCountMap := make(map[string]int)
	topicCountMap[globalTopic] = 1
	go consumer.StartStatic(topicCountMap)

	time.Sleep(2e9)
	bt := utils.NewBenchmarkTester(globalConcurrentLevel, globalDuration, func(bt *utils.BenchmarkTester, index int) error {

		<-message
		return nil
	}, nil)

	return errors.Trace(bt.Run())
}
