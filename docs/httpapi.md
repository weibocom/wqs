## 队列接口
**http://ip:port/queue** <br>
**参数列表：**<br>

| 参数名 | 是否必填 | 说明 |
| ---- | ---- | ----|
| action | 必填 | 创建/删除/变更/查看队列，参数为create,remove,update,lookup |
| queue | 必填 | 队列名称，查看队列时选填 |

**示例：** <br>
**创建队列：** <br>
curl -d "action=create&queue=menglong\_queue1" "http://127.0.0.1:8080/queue" <br>
{"action":"create","result":true} <br>

**删除队列：** <br>
curl -d "action=remove&queue=menglong\_queue1" "http://127.0.0.1:8080/queue"
{"action":"create","result":true} <br>

**变更队列：** <br>
暂不提供 <br>

**查看队列：** <br>
curl "http://127.0.0.1:8080/queue?action=lookup"<br>
curl "http://127.0.0.1:8080/queue?action=lookup&queue=menglong\_queue1"<br>
curl "http://127.0.0.1:8080/queue?action=lookup&queue=menglong\_queue1&group=menglong\_group1"<br>

## 业务接口
**http://ip:port/group** <br>
**参数列表：** <br>

| 参数名 | 是否必填 | 说明 |
| ---- | ---- | ----|
| action | 必填 | 增加/删除/变更/查看业务方，参数为add,remove,update,lookup |
| group | 必填 | 业务标识，查看业务方时选填 |
| queue | 必填 | 队列名称 |
| write | 选填 | 默认false，增加和变更业务方时使用 |
| read | 选填 | 默认false，增加和变更业务方时使用 |
| url | 选填 | 业务方使用的域名，增加和变更业务方时使用 |
| ips | 选填 | 域名对应的ip，多个ip用逗号分隔，增加和变更业务方时使用 |

**示例：** <br>
**增加业务方：** <br>
curl -d "action=add&group=menglong\_group1&queue=menglong\_queue1&write=true&read=true" "http://127.0.0.1:8080/group" <br>
{"action":"add","result":true} <br>

**删除业务方：** <br>
curl -d "action=remove&group=menglong\_group1&queue=menglong\_queue1" "http://127.0.0.1:8080/group" <br>
{"action":"remove","result":true} <br>

**变更业务方：** <br>
curl -d "action=update&group=menglong\_group1&queue=menglong\_queue1&write=false" "http://127.0.0.1:8080/group"
{"action":"update","result":true}<br>

**查看业务方：** <br>
curl "http://127.0.0.1:8080/group?action=lookup" <br>
curl "http://127.0.0.1:8080/group?action=lookup&group=menglong\_group1"<br>


## 消息接口
**http://ip:port/message**
**参数列表：** <br>

| 参数名 | 是否必填 | 说明 |
| ---- | ---- | ----|
| action | 必填 | 取消息/发消息/确认消息，参数为receive,send,ack |
| queue | 必填 | 队列名称 |
| group | 必填 | 业务名称 |
| msg | 必填 | 消息体，发送消息使用 |

**示例：** <br>
**发送消息：** <br>
curl -d "action=send&queue=remind&group=if&msg=helloworld" "http://127.0.0.1:8080/msg" <br>
{"action":"send","result":true} <br>

**接收消息：** <br>
curl "http://127.0.0.1:8080/msg?action=receive&queue=remind&group=if" <br>
{"action":"receive","msg":"helloworld2"} <br>

**确认消息：** <br>
curl -d "action=ack&queue=remind&group=if&id=xxxx" "http://127.0.0.1:8080/msg" <br>
{"action":"ack","result":true} <br>

## 统计信息接口
/queue/:queue/:group/metrics/:action/:type <br>

| 名称 | 说明 |
| ---- |---- |
| :queue | 队列名称 |
| :group | 分组名称 |
| :action | 统计的动作(sent、recv) |
| :type | 统计的分类(qps、elapsed、ltc) |


**参数列表：** <br>

| 参数名 | 是否必填 | 说明 |
| ---- | ---- | ----|
| start | 必填 | 起始时间(unix seconde) |
| end | 必填 | 结束时间(unix seconde) |
| step | 选填 | 未定义 |

**示例：** <br>

***消息发送QPS：*** <br>
curl "http://127.0.0.1:8080/queue/T1/11/metrics/sent/qps?start=1465972528&end=1465986928" <br>

***消息接收QPS：*** <br>
curl "http://127.0.0.1:8080/queue/T1/11/metrics/recv/qps?start=1465972528&end=1465986928" <br>

<!-- ## 报警接口(定义中)
**http://ip:port/alarm** <br>
type：heap，send.second，receive.second <br> -->
# Proxy API
**Get all online proxies:** <br>
/proxies/ <br>
curl "http://127.0.0.1:8080/proxies/" <br>

**Get a online proxy's config:** <br>
/proxies/:id/config <br>
curl "http://127.0.0.1:8080/proxies/1/config" <br>


# Debug API
### pprof API
/debug/pprof/ <br>
/debug/pprof/cmdline <br>
/debug/pprof/profile <br>
/debug/pprof/symbol <br>
/debug/pprof/trace <br>
