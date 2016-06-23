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
ack <key> <message id> [noreply]\r\n
```
* 回复(没有noreply时)
```
STORED\r\n
```

### stats
获得QService各项统计指标。
* 请求
```
stats\r\n
```
* 回复
```
STAT pid <进程pid>\r\n
STAT uptime <进程启动到现在过去的秒数>\r\n
STAT time <系统当前时间,单位秒>\r\n
STAT version qservice <版本号>\r\n
STAT pointer_size <当前系统指针大小，64bit系统为64>\r\n
STAT curr_connections <当前连接数>\r\n
STAT total_connections <历史总连接数>\r\n
STAT get_cmds <读消息总次数>\r\n
STAT get_hits <读消息成功的总次数>\r\n
STAT set_cmds <写消息总次数>\r\n
STAT set_hits <写消息成功的总次数>\r\n
STAT bytes_read <读消息总字节数>\r\n
STAT bytes_written <写消息总字节数>\r\n
STAT rusage_user <进程在用户空间用时>\r\n
STAT rusage_system <进程在系统空间用时>\r\n
STAT threads <进程goroutine数量>\r\n
END\r\n
```

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
