# 监控说明

现在数据监控方面支持Profile日志、Graphite、memcached stats命令等多种方式。
在配置文件中`metrics.transport.writers`项用来配置需要开启的监控方式，多项之间用`,`分割。例如:`metrics.transport.writers=graphite,profile`

## 监控指标

具体每项监控指标会以不同的`指标类型`进行记录

| 指标类型 | 值含义 | 单位 |
| ---- | ---- | ----: |
| Counter | 计数器,记录该事件发生的总次数 | 次 |
| Meter | 频次,记录该事件每秒发生的次数 | 次/秒 |
| Timer | 平均耗时,记录该事件每次发生的平均值耗时 | 毫秒/次 |


| 指标 | 指标类型 | 含义 |
| ---- | :----: | ----- |
| ReConn | Counter | 当前连接数 |
| ToConn | Counter | 历史总连接数 |
| BytesRead | Counter | 读取消息的总字节数 |
| BytesWriten | Counter | 写入消息的总字节数 |
| GET | Counter | 读取消息的总条数 |
| SET | Counter | 写入消息的总条数 |
| GETMiss | Counter | 读取消息失败的次数 |
| SETMiss | Counter | 写入消息失败的次数 |
| [queue].[group].GET.ops | Counter | 该queue下该group读消息的次数 |
| [queue].[group].GET.qps | Meter | 该queue下该group读消息次数的QPS |
| [queue].[group].GET.Less10ms | Counter | 该queue下该group读消息耗时小于10ms的次数 |
| [queue].[group].GET.Less50ms | Counter | 该queue下该group读消息耗时小于50ms的次数 |
| [queue].[group].SET.ops | Counter | 该queue下该group写消息的次数 |
| [queue].[group].SET.qps | Meter | 该queue下该group写消息次数的QPS |
| [queue].[group].SET.Less10ms | Counter | 该queue下该group写消息耗时小于10ms的次数 |
| [queue].[group].SET.Less50ms | Counter | 该queue下该group写消息耗时小于50ms的次数 |
| [queue].[group].ACK.ops | Counter | 该queue下该group ACK消息的次数 |
| [queue].[group].ACK.Less10ms | Counter | 该queue下该group ACK消息耗时小于10ms的次数 |


## 监控展示方式
### Profile日志
WQS会将进程中各项统计指标记录到Profile日志中，Profile日志文件在配置文件中的`log.profile`来指定。

在Profile日志中，每一项统计指标会打印为一行。
格式:

`年-月-日 时:分:表 指标Key: 指标Value|指标类型`

示例:

`2016-06-23 11:36:39 queue.group.SET.OPS: 1245|Counter`

### Graphite
当启动Graphite数据发送后，WQS会定时将各项统计指标发送至Graphite，Graphite会根据不同的指标类型来进行聚合。Counter、Meter类型的指标会进行加和，Timer类型的指标会进行取平均值。

### Memcached stats命令
详见: [Memcached API](memcached_cn.md#stats)
