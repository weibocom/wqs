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
	"strings"

	"github.com/Shopify/sarama"
	docopt "github.com/docopt/docopt-go"
	"github.com/juju/errors"
)

func getBrokers(brokerAddrs string) (map[string]*sarama.Broker, error) {

	brokers := make(map[string]*sarama.Broker)
	config := sarama.NewConfig()
	for _, addr := range strings.Split(brokerAddrs, ",") {
		broker := sarama.NewBroker(addr)
		if err := broker.Open(config); err != nil {
			return brokers, errors.Trace(err)
		}

		brokers[addr] = broker
	}
	return brokers, nil
}

func getConsumerCoordinator(broker *sarama.Broker, group string) (int32, error) {
	request := new(sarama.ConsumerMetadataRequest)
	request.ConsumerGroup = group

	response, err := broker.GetConsumerMetadata(request)

	if err != nil {
		return -1, errors.Trace(err)
	}

	if response.Err != sarama.ErrNoError {
		return -1, errors.Trace(response.Err)
	}
	return response.CoordinatorID, nil
}

func joinGroup(broker *sarama.Broker, group, topic string) error {
	req := &sarama.JoinGroupRequest{
		GroupId:        group,
		MemberId:       "",
		SessionTimeout: 30000,
		ProtocolType:   "consumer",
	}

	meta := &sarama.ConsumerGroupMemberMetadata{
		Version: 1,
		Topics:  []string{topic},
	}
	err := req.AddGroupProtocolMetadata("range", meta)
	if err != nil {
		return errors.Trace(err)
	}
	err = req.AddGroupProtocolMetadata("roundrobin", meta)
	if err != nil {
		return errors.Trace(err)
	}

	resp, err := broker.JoinGroup(req)
	if err != nil {
		return errors.Trace(err)
	} else if resp.Err != sarama.ErrNoError {
		return errors.Trace(resp.Err)
	}
	return nil
}

func cmdKafkaGroup(argv []string) error {
	usage := `usage: states kafka_group [options]

options:
	--brokers=<ADDRS>         kafka brokers address
	--group=<GROUP>           kafka consumer group
	--topic=<TOPIC>           kafka topic
`
	args, err := docopt.Parse(usage, argv, true, "", false)
	if err != nil {
		return errors.Trace(err)
	}

	brokerAddrs, ok := args["--brokers"].(string)
	if !ok || len(brokerAddrs) == 0 {
		return errors.NotValidf("brokers address")
	}

	group, ok := args["--group"].(string)
	if !ok || len(group) == 0 {
		return errors.NotValidf("group")
	}

	topic, ok := args["--topic"].(string)
	if !ok || len(topic) == 0 {
		return errors.NotValidf("topic")
	}

	brokers, err := getBrokers(brokerAddrs)
	if err != nil {
		return errors.Trace(err)
	}

	for _, broker := range brokers {
		id, err := getConsumerCoordinator(broker, group)
		if err != nil {
			fmt.Printf("getConsumerCoordinator error : %v\n", err)
			continue
		}
		fmt.Printf("Get ConsumerCoordinator is %d from %q\n", id, broker.Addr())
		err = joinGroup(broker, group, topic)
		fmt.Printf("send join group request to %q and return error %v\n", broker.Addr(), err)
	}

	return nil
}
