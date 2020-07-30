## 分布式位图服务

基于raft的位图服务，可以搭建一个n个节点的raft集群，保证数据一致性。

位图服务是一个写少读多的服务，所以基于raft的实现可以满足性能的要求。

同时，考虑到位图服务的应用场景并不是严格强一致性的场景， 读操作并不基于raft的线性读或者lease read，而是保证最终一致性。


### 测试集群

以三个节点的集群为例:

```
basalt --id 1 --peers http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --addr :18972 --data bitmaps1.bdb
basalt --id 2 --peers http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --addr :28972 --data bitmaps2.bdb
basalt --id 3 --peers http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --addr :38972 --data bitmaps3.bdb
```


测试在第一个节点增加一个数据:
```sh
 basalt git:(master) ✗ curl -X POST "http://127.0.0.1:18972/add/test/1000"
```

在第二个节点检查这个数据是否存在，正常应该返回`200 OK`
```
➜  basalt git:(master) ✗ curl -v "http://127.0.0.1:28972/exists/test/1000"
< HTTP/1.1 200 OK
```