# ASCII Memcached API

## ASCII Memcached 协议标准
  - [ASCII Memcached 协议](https://github.com/memcached/memcached/blob/master/doc/protocol.txt)
  - QService支持Memcached协议的基础格式，并且扩展出了一些新的操作命令

## Key 定义
  - \[group name\].\[queue name\]
  - 采用“分组名”+“.”+“队列名”作为Memcached协议操作的Key。

## 支持命令
- [x] [get](#get)
- [x] [eget](#eget)
- [x] [set](#set)
- [x] [eset](#eset)
- [x] [ack](#ack)
- [x] [stats](#stats)

### get
读取一条消息(只获得消息体)
* 请求
```
get <key>\r\n
```
* 回复
```
VALUE <key> <flags> <bytes>\r\n
<data block>\r\n
END\r\n
```

### eget
读取一条消息，并获得这个消息的ID，回复的data体中，data block的第一个byte为消息ID的长度N，而后跟着的N个字节为消息ID，再后面跟着的字节为消息体。
* 请求
```
eget <key>\r\n
```
* 回复
```
VALUE <key> <flags> <bytes>\r\n
<data block>\r\n
END\r\n
```

### set
向以key为标识的队列中写入一条消息，data block为消息体
* 请求
```
set <key> <flags> <exptime> <bytes> [noreply]\r\n
<data block>\r\n
```
* 回复(没有noreply时)
```
STORED\r\n
```

### eset
向以key为标识的队列中写入一条消息，data block为消息体，写入后，得到消息ID。
* 请求
```
eset <key> <flags> <exptime> <bytes> [noreply]\r\n
<data block>\r\n
```
* 回复(没有noreply时)
```
<message id> STORED\r\n
```

### ack
向以key为标识的队列ACK一条消息。
* 请求
```
ack <key> <flags> <exptime> <bytes> [noreply]\r\n
<message id>\r\n
```
* 回复(没有noreply时)
```
STORED\r\n
```

### stats
获得QService中每个队列的堆积量。
* 请求
```
stats queue\r\n
```
* 回复
```
STAT <group>.<queue> <消息总量>/<消息消费量>\r\n
STAT <group>.<queue> <消息总量>/<消息消费量>\r\n
...
END\r\n
```
