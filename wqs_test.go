package main_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/Shopify/sarama"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

const (
	testKafkaData = "/tmp/wqs_testing"
)

var (
	testKafkaRoot           = "kafka_2.11-0.9.0.1"
	testKafkaAddrs          = []string{"127.0.0.1:39092"}
	testTopics              = []string{"topic-a", "topic-b"}
	testClient              sarama.Client
	testKafkaCmd, testZkCmd *exec.Cmd
)

func init() {
	if dir := os.Getenv("KAFKA_DIR"); dir != "" {
		testKafkaRoot = dir
	}
}

func TestWqs(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Wqs-testing")
}

//++++++++++++++  don’t edit below ++++++++++++++
var _ = BeforeSuite(func() {
	testZkCmd = exec.Command(
		testDataDir(testKafkaRoot, "bin", "kafka-run-class.sh"),
		"org.apache.zookeeper.server.quorum.QuorumPeerMain",
		testDataDir("zookeeper.properties"),
	)
	testZkCmd.Env = []string{"KAFKA_HEAP_OPTS=-Xmx512M -Xms512M"}
	// testZkCmd.Stderr = os.Stderr
	// testZkCmd.Stdout = os.Stdout

	testKafkaCmd = exec.Command(
		testDataDir(testKafkaRoot, "bin", "kafka-run-class.sh"),
		"-name", "kafkaServer", "kafka.Kafka",
		testDataDir("server.properties"),
	)
	testKafkaCmd.Env = []string{"KAFKA_HEAP_OPTS=-Xmx1G -Xms1G"}
	// testKafkaCmd.Stderr = os.Stderr
	// testKafkaCmd.Stdout = os.Stdout

	Expect(os.MkdirAll(testKafkaData, 0777)).NotTo(HaveOccurred())
	Expect(testZkCmd.Start()).NotTo(HaveOccurred())
	Expect(testKafkaCmd.Start()).NotTo(HaveOccurred())

	// Wait for client
	Eventually(func() error {
		var err error

		testClient, err = sarama.NewClient(testKafkaAddrs, nil)
		return err
	}, "10s", "1s").ShouldNot(HaveOccurred())

	// Ensure we can retrieve partition info
	Eventually(func() error {
		_, err := testClient.Partitions(testTopics[0])
		return err
	}, "10s", "500ms").ShouldNot(HaveOccurred())

	// Seed a few messages
	Expect(testSeed(1000)).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	_ = testClient.Close()

	_ = testKafkaCmd.Process.Kill()
	_ = testZkCmd.Process.Kill()
	_ = testKafkaCmd.Wait()
	_ = testZkCmd.Wait()
	_ = os.RemoveAll(testKafkaData)
})

func testDataDir(tokens ...string) string {
	tokens = append([]string{"testdata"}, tokens...)
	return filepath.Join(tokens...)
}

// Seed messages
func testSeed(n int) error {
	producer, err := sarama.NewSyncProducerFromClient(testClient)
	if err != nil {
		return err
	}

	for i := 0; i < n; i++ {
		kv := sarama.StringEncoder(fmt.Sprintf("PLAINDATA-%08d", i))
		for _, t := range testTopics {
			msg := &sarama.ProducerMessage{Topic: t, Key: kv, Value: kv}
			if _, _, err := producer.SendMessage(msg); err != nil {
				return err
			}
		}
	}
	return producer.Close()
}
