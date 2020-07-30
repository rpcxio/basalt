# 基于Raft的数据一致性分布式的 bitmap 服务

`bitmap`(位图)技术是数据库、大数据和互联网业务等场景下经常使用的一种技术。

- 存在性判断
  - 爬虫url去重
  - 垃圾邮件过滤
  - 用户已阅读
  - 用户已赞
  - ...
- 去重
- 数据库
- 大数据计算

![](examples/basalt.jpg)

## 服务

进入`cmd/server`, 运行`go run server.go`启动一个bitmap服务。

它同时支持三种服务:

- rpcx: 你可以使用rpcx服务获取高性能的网络调用, `cmd/rpcx_client`是一个rpcx客户端的demo
- redis: 你可以使用redis客户端访问Bitmap服务(如果你的redis client支持自定义命令), 方便兼容redis调用代码， `cmd/redis_client`是redis demo
- http: 通过http服务调用，调用简单,支持各种编程语言和脚本，`cmd/http_client/curl.sh`是通过`curl`调用服务

## 集群模式

支持raft集群模式: [basalt集群](https://github.com/rpcxio/basalt/tree/master/cmd/raft_server)

## API接口

basalt位图服务支持三种接口模式：

- HTTP API: 通过http api的方式进行访问
- Redis模式: 扩展了redis命令，可以通过redis client进行访问
- rpcx模式: 可以通过rpcx框架进行访问

### Redis命令

- `ping`: ping-pong消息
- `quit`: 退出连接
- `bmadd name value`: 在名为`name`的bitmap增加一个uint32值`value`
- `bmaddmany name value1 value2 value3...`: 为名为`name`的bitmap增加一批值
- `bmdel name value`: 在名为`name`的bitmap删除一个uint32值`value`
- `bmdrop name`: 删除名为`name`的bitmap
- `bmclear name`: 清空名为`name`的bitmap
- `bmcard name`: 获取为`name`的bitmap包含的元素数
- `bmexists name value`: 检查uint32值`value`是否存在于名为`name`的bitmap中，整数`1`代表存在，`0`代表不存在
- `bminter name1 name2 name3...`: 求几个bitmap的交集，返回交集的uint32整数列表
- `bminterstore dst name1 name2 name3...`: 求几个bitmap(`name1`、`name2`、`name3`...)的交集，并将结果保存到`dst`中
- `bmunion name1 name2 name3...`: 求几个bitmap的并集，返回并集的uint32整数列表
- `bmunionstore dst name1 name2 name3...`: 求几个bitmap(`name1`、`name2`、`name3`...)的并集，并将结果保存到`dst`中
- `bmxor name1 name2.`: 求两个bitmap的`xor`集(双方不共有的集合,相当于并集减交集)，返回`xor`集的uint32整数列表
- `bmxorstore dst name1 name2`: 求两个bitmap的`xor`集，并将结果保存到`dst`中
- `bmdiff name1 name2`: 求`name1`中和`name2`没有交集的数据，返回结果的uint32整数列表
- `bmdiffstore dst name1 name2`: 求`name1`中和`name2`没有交集的数据，并将结果保存到`dst`中
- `bmstats name`: 返回`name`的bitmap的统计信息

### rpcx 服务

查看 [godoc](https://godoc.org/github.com/rpcxio/basalt)以了解提供的rpcx服务

### HTTP 服务

HTTP 服务提供和 redis、rpcx服务相同的功能，通过http调用就可以访问Bitmap服务。

所有的参数都是在路径中提供，路径格式为`/action/param1/param2`。
复数形式`values`、`names`包含多个元素，元素以逗号`,`分隔。

为了简化操作，所有的http服务都是通过`GET`方法提供的。

返回的`HTTP StatusCode`代表的含义如下：

- `200` 代表`OK`、`存在`
- `400` 代表参数不对，比如应该是uint32格式，结果却是无法解析的字符串
- `404` 代表不存在
- `500` 代表内部处理错误


HTTP服务路径列表如下：

- `/add/:name/:value`
- `/addmany/:name/:values`
- `/remove/:name/:value`
- `/drop/:name`
- `/clear/:name`
- `/exists/:name/:value`
- `/card/:name`
- `/inter/:names`
- `/interstore/:dst/:names`
- `/union/:names`
- `/unionstore/:dst/:names`
- `/xor/:name1/:name2`
- `/xorstore/:dst/:name1/:name2`
- `/diff/:name1/:name2`
- `/diffstore/:dst/:name1/:name2`
- `/stats/:name`

## 例子

以微博关注关系数据集做例子，我们使用Bitmap服务来存储某人是否关注了某人，以及两人是否互相关注。

例子参看 [weibo_follow](https://github.com/rpcxio/basalt/tree/master/examples/weibo)


## Roadmap

- [x] Multiple-key Bitmap
- [x] rpcx services for Bitmap
- [x] HTTP services for Bitmap
- [x] Redis services for Bitmap
- [x] Persistence
- [x] Cluster mode

## Credits

- [roaring](https://github.com/RoaringBitmap/roaring)
- [redcon](https://github.com/tidwall/redcon)
- [httprouter](https://github.com/julienschmidt/httprouter)
- [rpcx](https://github.com/smallnest/rpcx)
- [etcd](https://github.com/etcd-io/etcd)