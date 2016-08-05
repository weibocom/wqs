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

package queue

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/weibocom/wqs/config"
	"github.com/weibocom/wqs/engine/kafka"
	"github.com/weibocom/wqs/engine/zookeeper"
	"github.com/weibocom/wqs/log"

	"github.com/Shopify/sarama"
	"github.com/juju/errors"
)

const (
	groupConfigPathSuffix = "/wqs/metadata/groupconfig"
	queuePathSuffix       = "/wqs/metadata/queue"
	servicePathPrefix     = "/wqs/metadata/service"
	metricsPathPrefix     = "/wqs/metadata/metrics"
	operationPathPrefix   = "/wqs/metadata/operation"
	defaultIdc            = "local"
)

type Metadata struct {
	config          *config.Config
	zkConn          *zookeeper.Conn
	managers        map[string]*kafka.Manager
	groupConfigPath string
	queuePath       string
	servicePath     string
	metricsPath     string
	operationPath   string
	local           string
	partitions      int32
	replications    int32
	stopping        int32
	id              int
	queueConfigs    map[string]QueueConfig
	dying           chan struct{}
	rw              sync.RWMutex
}

// return a new metadata instance
func NewMetadata(config *config.Config, sconfig *sarama.Config) (*Metadata, error) {

	kafkaSection, err := config.GetSection("kafka")
	if err != nil {
		return nil, errors.Trace(err)
	}

	zkConn, err := zookeeper.NewConnect(strings.Split(config.MetaDataZKAddr, ","))
	if err != nil {
		return nil, errors.Trace(err)
	}

	root := config.MetaDataZKRoot
	if root == "/" {
		root = ""
	}

	groupConfigPath := fmt.Sprintf("%s%s", root, groupConfigPathSuffix)
	queuePath := fmt.Sprintf("%s%s", root, queuePathSuffix)
	servicePath := fmt.Sprintf("%s%s", root, servicePathPrefix)
	operationPath := fmt.Sprintf("%s%s", root, operationPathPrefix)
	metricsPath := fmt.Sprintf("%s%s", root, metricsPathPrefix)

	if err = zkConn.CreateRecursiveIgnoreExist(groupConfigPath, "", 0); err != nil {
		return nil, errors.Trace(err)
	}

	if err = zkConn.CreateRecursiveIgnoreExist(queuePath, "", 0); err != nil {
		return nil, errors.Trace(err)
	}

	if err = zkConn.CreateRecursiveIgnoreExist(servicePath, "", 0); err != nil {
		return nil, errors.Trace(err)
	}

	if err = zkConn.CreateRecursiveIgnoreExist(operationPath, "", 0); err != nil {
		return nil, errors.Trace(err)
	}

	kafkaZkAddr, err := kafkaSection.GetString("zookeeper.connect")
	if err != nil {
		return nil, errors.Trace(err)
	}
	kafkaZkRoot, err := kafkaSection.GetString("zookeeper.root")
	if err != nil {
		return nil, errors.Trace(err)
	}

	replications := int32(kafkaSection.GetInt64Must("topic.replications", 0))
	if replications < 1 {
		return nil, errors.NotValidf("kafka.topic.replications")
	}
	partitions := int32(kafkaSection.GetInt64Must("topic.partitions", 0))
	if partitions < 1 {
		return nil, errors.NotValidf("kafka.topic.partitions")
	}

	manager, err := kafka.NewManager(strings.Split(kafkaZkAddr, ","), kafkaZkRoot, sconfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	idc := kafkaSection.GetStringMust("idc", defaultIdc)
	managers := make(map[string]*kafka.Manager)
	managers[idc] = manager

	//解析配置，初始化远端IDC的kafka的Manager
	remoteIdcs := kafkaSection.GetDupByPattern(`^remote\.\w+\.zookeeper\.connect$`)
	for name, addrs := range remoteIdcs {
		if len(addrs) == 0 {
			continue
		}
		kafkaRoot := "/"
		//上面的正则表达式已经保证了name的正确性，所以直接进行Split取值
		idc := strings.Split(name, ".")[1]
		tokens := strings.SplitN(addrs, "/", 2)
		if len(tokens) == 2 {
			addrs = tokens[0]
			kafkaRoot += tokens[1]
		}
		idcKafakManager, err := kafka.NewManager(strings.Split(addrs, ","), kafkaRoot, sconfig)
		if err != nil {
			return nil, errors.Trace(errors.Annotatef(err, "at add idc kafka: %q", idc))
		}
		if _, ok := managers[idc]; ok {
			return nil, errors.AlreadyExistsf("duplicate IDC: %q", idc)
		}
		managers[idc] = idcKafakManager
	}

	metadata := &Metadata{
		config:          config,
		zkConn:          zkConn,
		managers:        managers,
		groupConfigPath: groupConfigPath,
		queuePath:       queuePath,
		servicePath:     servicePath,
		metricsPath:     metricsPath,
		operationPath:   operationPath,
		local:           idc,
		partitions:      partitions,
		replications:    replications,
		id:              config.ProxyId,
		queueConfigs:    make(map[string]QueueConfig),
		dying:           make(chan struct{}),
	}

	if err = metadata.RefreshMetadata(); err != nil {
		return nil, errors.Trace(err)
	}

	go func(m *Metadata) {
		ticker := time.NewTicker(sconfig.Metadata.RefreshFrequency)
		for {
			select {
			case <-ticker.C:
				if err := m.RefreshMetadata(); err != nil {
					log.Errorf("timeout refresh metadata error %s", errors.ErrorStack(err))
				}
			case <-m.dying:
				ticker.Stop()
				return
			}
		}
	}(metadata)

	return metadata, nil
}

// return local IDC kafka manager
func (m *Metadata) LocalManager() *kafka.Manager {
	return m.managers[m.local]
}

// register service to zookeeper
func (m *Metadata) RegisterService(id int, data string) error {
	path := fmt.Sprintf("%s/%d", m.servicePath, id)
	if err := m.zkConn.Create(path, data, zookeeper.Ephemeral); err != nil {
		if zookeeper.IsExistError(err) {
			return errors.AlreadyExistsf("service %d", id)
		}
		return errors.Trace(err)
	}
	return nil
}

//Get a proxy's config
func (m *Metadata) GetProxyConfigByID(id int) (string, error) {

	data, _, err := m.zkConn.Get(fmt.Sprintf("%s/%d", m.servicePath, id))
	if err != nil {
		return "", errors.Trace(err)
	}

	info := proxyInfo{}
	if err = info.Load(data); err != nil {
		return "", errors.Trace(err)
	}

	return info.Config, nil
}

// return proxy map[id]host
func (m *Metadata) Proxys() (map[string]string, error) {

	proxys := make(map[string]string)
	ids, _, err := m.zkConn.Children(m.servicePath)
	if err != nil {
		return proxys, errors.Trace(err)
	}

	for _, id := range ids {
		data, _, err := m.zkConn.Get(fmt.Sprintf("%s/%s", m.servicePath, id))
		if err != nil {
			return proxys, errors.Trace(err)
		}

		info := proxyInfo{}
		if err = info.Load(data); err != nil {
			return proxys, errors.Trace(err)
		}

		proxys[id] = info.Host
	}
	return proxys, nil
}

// refresh metadata from zookeeper
func (m *Metadata) RefreshMetadata() error {
	queueConfigs := make(map[string]QueueConfig)

	for idc, manager := range m.managers {
		if err := manager.RefreshMetadata(); err != nil {
			log.Errorf("metadata RefreshMetadata idc: %q error: %v", idc, err)
		}
	}

	queues, _, err := m.zkConn.Children(m.queuePath)
	if err != nil {
		return errors.Trace(err)
	}

	for _, queue := range queues {
		data, stat, err := m.zkConn.Get(m.buildQueuePath(queue))
		if err != nil {
			log.Errorf("refresh err : %s", err)
			return errors.Trace(err)
		}

		exist, err := m.LocalManager().ExistTopic(queue)
		if err != nil {
			log.Errorf("refresh test ExistTopic err : %s", err)
			return errors.Trace(err)
		}
		if !exist {
			log.Errorf("queue : %q has metadata, but has no topic", queue)
			continue
		}

		config := QueueConfig{}
		// 兼容旧版本元数据
		if err := config.Parse(data); err != nil {
			config.Queue = queue
			config.Ctime = stat.Ctime / 1e3
			config.Length = 0
		}
		if config.Idcs == nil {
			config.Idcs = []string{m.local}
		}
		if config.Groups == nil {
			config.Groups = make(map[string]GroupConfig)
		}

		queueConfigs[queue] = config
	}

	groupKeys, _, err := m.zkConn.Children(m.groupConfigPath)
	if err != nil {
		return errors.Trace(err)
	}

	for _, groupKey := range groupKeys {
		tokens := strings.Split(groupKey, ".")
		if len(tokens) != 2 {
			continue
		}
		queueName, groupName := tokens[1], tokens[0]
		queue, ok := queueConfigs[queueName]
		if !ok {
			continue
		}

		groupDataPath := fmt.Sprintf("%s/%s", m.groupConfigPath, groupKey)
		data, _, err := m.zkConn.Get(groupDataPath)
		if err != nil {
			log.Warnf("get %s err: %s", groupDataPath, err)
			continue
		}

		groupConfig := GroupConfig{}
		if err = groupConfig.Load(data); err != nil {
			log.Warnf("Unmarshal %s data err: %s", groupDataPath, err)
			continue
		}

		queue.Groups[groupName] = groupConfig
	}

	m.rw.Lock()
	m.queueConfigs = queueConfigs
	m.rw.Unlock()
	return nil
}

// reset given queue-group's offset by time
func (m *Metadata) ResetOffset(queue string, group string, time int64) error {
	if err := m.RefreshMetadata(); err != nil {
		return errors.Trace(err)
	}

	for idc, manager := range m.managers {
		offsets, err := manager.FetchTopicOffsets(queue, time)
		if err != nil {
			return errors.Annotatef(err, " at idc %s", idc)
		}
		if err = manager.CommitOffset(queue, group, offsets); err != nil {
			return errors.Annotatef(err, " at reset offset idc %s", idc)
		}
	}
	return nil
}

// add a group to given queue
func (m *Metadata) AddGroup(group string, queue string,
	write bool, read bool, url string, ips []string) error {

	mu := m.zkConn.NewMutex(m.operationPath)
	if err := mu.Lock(); err != nil {
		return errors.Trace(err)
	}
	defer mu.Unlock()

	if err := m.RefreshMetadata(); err != nil {
		return errors.Trace(err)
	}

	if exist := m.ExistGroup(queue, group); exist {
		return errors.AlreadyExistsf("queue : %q, group : %q", queue, group)
	}

	config := GroupConfig{
		Group: group,
		Queue: queue,
		Write: write,
		Read:  read,
		Url:   url,
		Ips:   ips,
	}

	data := config.String()
	path := m.buildConfigPath(group, queue)
	log.Debugf("add group config, zk path:%s, data:%s", path, data)
	if err := m.zkConn.CreateRecursive(path, data, 0); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// delete given group
func (m *Metadata) DeleteGroup(group string, queue string) error {

	mu := m.zkConn.NewMutex(m.operationPath)
	if err := mu.Lock(); err != nil {
		return errors.Trace(err)
	}
	defer mu.Unlock()

	if err := m.RefreshMetadata(); err != nil {
		return errors.Trace(err)
	}

	if exist := m.ExistGroup(queue, group); !exist {
		return errors.NotFoundf("queue : %q, group : %q", queue, group)
	}

	path := m.buildConfigPath(group, queue)
	log.Debugf("delete group config, zk path:%s", path)
	if err := m.zkConn.DeleteRecursive(path); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// update given group config
func (m *Metadata) UpdateGroupConfig(group string, queue string,
	write bool, read bool, url string, ips []string) error {

	mu := m.zkConn.NewMutex(m.operationPath)
	if err := mu.Lock(); err != nil {
		return errors.Trace(err)
	}
	defer mu.Unlock()

	if err := m.RefreshMetadata(); err != nil {
		return errors.Trace(err)
	}

	if exist := m.ExistGroup(queue, group); !exist {
		return errors.NotFoundf("queue : %q, group: %q", queue, group)
	}

	path := m.buildConfigPath(group, queue)
	config := GroupConfig{
		Group: group,
		Queue: queue,
		Write: write,
		Read:  read,
		Url:   url,
		Ips:   ips,
	}
	data := config.String()
	log.Debugf("update group config, zk path:%s, data:%s", path, data)
	if err := m.zkConn.Set(path, data); err != nil {
		return errors.Trace(err)
	}
	return nil
}

//TODO 回头修改HTTP API时同时修改返回的数据结构，能够最大化简化逻辑
func (m *Metadata) GetQueueInfo(queues ...string) ([]*QueueInfo, error) {
	m.rw.RLock()
	defer m.rw.RUnlock()

	queueInfos := make([]*QueueInfo, 0)
	for _, queue := range queues {
		queueConfig, ok := m.queueConfigs[queue]
		if !ok {
			return queueInfos, errors.NotFoundf("queue: %q", queue)
		}

		queueInfo := QueueInfo{
			Queue:  queue,
			Ctime:  queueConfig.Ctime,
			Length: queueConfig.Length,
			Groups: make([]GroupConfig, 0),
		}

		for _, groupConfig := range queueConfig.Groups {
			queueInfo.Groups = append(queueInfo.Groups, groupConfig)
		}
		sort.Sort(groupSlice(queueInfo.Groups))
		queueInfos = append(queueInfos, &queueInfo)
	}

	sort.Sort(queueInfoSlice(queueInfos))

	return queueInfos, nil
}

// 没有深拷贝，目前貌似不需要
func (m *Metadata) GetQueueConfig(queue string) *QueueConfig {
	m.rw.RLock()
	config, ok := m.queueConfigs[queue]
	m.rw.RUnlock()
	if !ok {
		return nil
	}
	return &config
}

func (m *Metadata) GetGroupConfig(group string, queue string) (*GroupConfig, error) {
	m.rw.RLock()
	defer m.rw.RUnlock()

	queueConfig, ok := m.queueConfigs[queue]
	if !ok {
		return nil, errors.NotFoundf("queue: %q", queue)
	}

	groupConfig, ok := queueConfig.Groups[group]
	if !ok {
		return nil, errors.NotFoundf("group: %q", group)
	}
	return &groupConfig, nil
}

//Get queue names of per group
func (m *Metadata) GetGroupMap() map[string][]string {
	groupmap := make(map[string][]string)
	queuemap := m.GetQueueMap()
	for queue, groups := range queuemap {
		for _, group := range groups {
			groupmap[group] = append(groupmap[group], queue)
		}
	}
	return groupmap
}

//Get group names of per queue
func (m *Metadata) GetQueueMap() map[string][]string {
	queuemap := make(map[string][]string)

	m.rw.RLock()
	defer m.rw.RUnlock()

	for queue, queueConfig := range m.queueConfigs {
		groups := make([]string, 0)
		for group := range queueConfig.Groups {
			groups = append(groups, group)
		}
		queuemap[queue] = groups
	}
	return queuemap
}

//Add a queue by name. if want use multi idc, pass idc names in `idcs`
func (m *Metadata) AddQueue(queue string, idcs []string) error {

	mu := m.zkConn.NewMutex(m.operationPath)
	if err := mu.Lock(); err != nil {
		return errors.Trace(err)
	}
	defer mu.Unlock()

	if err := m.RefreshMetadata(); err != nil {
		return errors.Trace(err)
	}

	if exist := m.ExistQueue(queue); exist {
		return errors.AlreadyExistsf("queue: %q ", queue)
	}

	if len(idcs) == 0 {
		idcs = []string{m.local}
	}

	// 检查IDCs中是否包含了本地IDC
	idcs = appendIfNotContains(idcs, m.local)

	// 检查要求的IDC是否都存在
	for _, idc := range idcs {
		_, ok := m.managers[idc]
		if !ok {
			return errors.NotFoundf("idc: %q", idc)
		}
	}

	// 缺乏出错回滚
	for _, idc := range idcs {
		manager := m.managers[idc]
		if exist, _ := manager.ExistTopic(queue); exist {
			continue
		}
		if err := manager.CreateTopic(queue, m.replications, m.partitions); err != nil {
			return errors.Trace(err)
		}
	}

	config := &QueueConfig{
		Queue: queue,
		Ctime: time.Now().Unix(),
		Idcs:  idcs,
	}

	if err := m.zkConn.CreateRecursive(m.buildQueuePath(queue), config.String(), 0); err != nil {
		return errors.Trace(err)
	}
	return nil
}

//Delete a queue by name
func (m *Metadata) DelQueue(queue string) error {

	mu := m.zkConn.NewMutex(m.operationPath)
	if err := mu.Lock(); err != nil {
		return errors.Trace(err)
	}
	defer mu.Unlock()

	if err := m.RefreshMetadata(); err != nil {
		return errors.Trace(err)
	}

	can, err := m.canDeleteQueue(queue)
	if err != nil {
		return errors.Trace(err)
	}
	if !can {
		return errors.NotValidf("DeleteQueue queue:%s has one or more group", queue)
	}

	path := m.buildQueuePath(queue)
	log.Debugf("del queue, zk path:%s", path)
	if err := m.zkConn.DeleteRecursive(path); err != nil {
		return errors.Trace(err)
	}
	delete(m.queueConfigs, queue)
	if err := m.LocalManager().DeleteTopic(queue); err != nil {
		return errors.Trace(err)
	}
	return nil
}

//Get all queues' name
func (m *Metadata) GetQueues() (queues []string) {
	m.rw.RLock()
	for queue := range m.queueConfigs {
		queues = append(queues, queue)
	}
	m.rw.RUnlock()
	return
}

//Test a queue exist
func (m *Metadata) ExistQueue(queue string) bool {
	m.rw.RLock()
	_, exist := m.queueConfigs[queue]
	m.rw.RUnlock()
	return exist
}

//Test a group exist
func (m *Metadata) ExistGroup(queue, group string) bool {
	m.rw.RLock()
	queueConfig, exist := m.queueConfigs[queue]
	if !exist {
		m.rw.RUnlock()
		return false
	}
	_, exist = queueConfig.Groups[group]
	m.rw.RUnlock()
	return exist
}

func (m *Metadata) GetBrokerAddrsByIdc(idcs ...string) map[string][]string {
	brokerAddrs := make(map[string][]string)
	for _, idc := range idcs {
		if manager, ok := m.managers[idc]; ok {
			brokerAddrs[idc] = manager.BrokerAddrs()
		}
	}
	return brokerAddrs
}

//test a queue can be delete
func (m *Metadata) canDeleteQueue(queue string) (bool, error) {
	m.rw.RLock()
	defer m.rw.RUnlock()

	queueConfig, ok := m.queueConfigs[queue]
	if !ok {
		return false, errors.NotFoundf("queue: %q", queue)
	}

	return len(queueConfig.Groups) == 0, nil
}

func (m *Metadata) SaveMetrics(data string) error {
	return m.zkConn.CreateOrUpdate(fmt.Sprintf("%s/%d", m.metricsPath, m.id), data, 0)
}

func (m *Metadata) LoadMetrics() ([]byte, error) {
	data, _, err := m.zkConn.Get(fmt.Sprintf("%s/%d", m.metricsPath, m.id))
	if zookeeper.IsNoNode(err) {
		err = nil
	}
	return data, err
}

func (m *Metadata) Accumulation(queue, group string) (int64, int64, error) {
	return m.LocalManager().Accumulation(queue, group)
}

func (m *Metadata) buildConfigPath(group string, queue string) string {
	return m.groupConfigPath + "/" + group + "." + queue
}

func (m *Metadata) buildQueuePath(queue string) string {
	return m.queuePath + "/" + queue
}

// close and stop metadata
func (m *Metadata) Close() {

	if !atomic.CompareAndSwapInt32(&m.stopping, 0, 1) {
		return
	}
	close(m.dying)
	m.zkConn.Close()
	for _, manager := range m.managers {
		manager.Close()
	}
}

func appendIfNotContains(items []string, it string) []string {
	has := false
	for _, item := range items {
		if item == it {
			has = true
			break
		}
	}
	if !has {
		items = append(items, it)
	}
	return items
}
