# QService
***一个更快更可靠的消息系统.***<br>
## 特性:
  - [x] [多协议支持](#多协议支持)
    - [x] ASCII Memcached协议
    - [x] HTTP协议
    - [ ] Redis协议
    - [ ] Motan协议
  - [x] 高可靠性
  - [x] 消息持久化
  - [x] 一写多读
  - [x] 至少投递一次
  - [x] Ack机制
  - [ ] 多IDC支持
  - [x] 分布式
  - [x] 运维友好
    - [x] 支持横向扩展、纵向扩展
    - [x] 提供管理API、GUI
    - [x] 提供监控API、GUI

## 设计
QService是采用[Golang](https://github.com/golang/go)实现的一款分布式消息队列系统。采用[Zookeeper](https://zookeeper.apache.org)来存储系统元数据。
后端采用[Kafka0.9](https://kafka.apache.org)集群作为消息持久化引擎。

## 多协议支持:
### [ASCII Memcached 协议](https://github.com/memcached/memcached/blob/master/doc/protocol.txt):
<!-- When we use [ascii memcached protocol](https://github.com/memcached/memcached/blob/master/doc/protocol.txt), we can read or write a message to a queue by `get` or `set` command. -->

### HTTP 协议:
详见[HTTP API](https://github.com/weibocom/wqs/blob/master/docs/httpapi.md)
