## 多IDC部署
WQS目前支持多IDC部署，创建队列时选取响应的配置表明该队列是否需要多IDC功能支持，当WQS集群跨IDC部署时：
- Producer 通过WQS实例写消息会时写到与自身实例部署在同一IDC内的Kafka集群上。
- Consumer 通过WQS实例读消息时，该实例会根据Queue的配置来执行响应的行为。
  - 当Queue未配置多IDC同步时，该实例只会从该实例部署的IDC的Kafka集群中读取消息
  - 当Queue配置多IDC同步时，该实例会从用户所配置的IDC内的Kafka集群上读取消息，且不同IDC的WQS实例读取同一Kafka集群时，会采用相同的consumer-group-ID。

### 相关配置
为兼容老版本配置，则本地IDC的Kafka配置为：
```
kafka.zookeeper.connect=localhost:2181
kafka.zookeeper.root=
#本地IDC的名称，只能是英文字母和数字
kafka.idc=idc
```
其他IDC的配置为：
```
kafka.remote.<your idc name>.zookeeper.connect=xxx.xxx.xxx.xxx:2181/
```
例如远端IDC的名称为`abc`则：
```
kafka.remote.abc.zookeeper.connect=xxx.xxx.xxx.xxx:2181/
```

### 创建队列
如果你要创建一个队列，要支持多IDC数据同步，队列名为`abc`，IDC分别为`idc1`、`idc2`，则创建命令为
```
curl -X PUT "http://127.0.0.1:8080/queues/abc" -d '{"idcs":["idc1","idc2"]}'
```
