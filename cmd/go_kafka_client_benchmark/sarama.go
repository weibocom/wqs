// sarama.go
package main

import (
	"log"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
	"github.com/weibocom/wqs/utils"
)

func cmdBenchmarkSet2() error {

	conf := sarama.NewConfig()
	conf.Net.KeepAlive = 30 * time.Second
	conf.Metadata.Retry.Backoff = 100 * time.Millisecond
	conf.Metadata.Retry.Max = 5
	conf.Metadata.RefreshFrequency = 3 * time.Minute
	//conf.Producer.RequiredAcks = sarama.WaitForLocal
	conf.Producer.RequiredAcks = sarama.NoResponse //this one high performance than WaitForLocal
	conf.Producer.Partitioner = sarama.NewRandomPartitioner
	conf.Producer.Flush.Frequency = time.Millisecond
	conf.Producer.Flush.MaxMessages = 200
	conf.ClientID = "benchmark"
	conf.ChannelBufferSize = 1024

	if len(globalBrokerList) == 0 {
		return errors.NotValidf("broker list")
	}
	if len(globalTopic) == 0 {
		return errors.NotValidf("Topic")
	}

	sendString := utils.GenTestMessage(globalMsgLength)
	c, err := sarama.NewClient(strings.Split(globalBrokerList, ","), conf)
	if err != nil {
		return errors.Trace(err)
	}
	p, err := sarama.NewSyncProducerFromClient(c)
	if err != nil {
		return errors.Trace(err)
	}
	log.Printf("%v", globalBrokerList)

	bt := utils.NewBenchmarkTester(globalConcurrentLevel, globalDuration,
		func(bt *utils.BenchmarkTester, index int) error {

			msg := &sarama.ProducerMessage{
				Topic: globalTopic,
				Value: sarama.StringEncoder(sendString),
			}
			_, _, err := p.SendMessage(msg)
			if err != nil {
				return err
			}
			return nil
		}, nil)
	return errors.Trace(bt.Run())
}
