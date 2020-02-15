## 新浪微博数据集MicroblogPCU

[MicroblogPCU](https://archive.ics.uci.edu/ml/machine-learning-databases/00323/)是数据集原作者从新浪微博采集到的。原本被用于[研究机器学习方法和社会关系研究](https://archive.ics.uci.edu/ml/datasets/microblogPCU)。

这个数据集被原作者用于探索微博中的spammers（发送垃圾信息的人）。他们的demo在[这里](http://sd.skyclass.net:8080/Spammer/dia.jsp)。

我们解析`follower_followee.csv`(关注者-被关注者关系)， 以`follower_id`-`followee_id`作为key进行hash,然后将输入放入`follow` Bitmap中。

随后找一些ID看看是否有关注关系。


你需要解压[microblogPCU数据集](https://archive.ics.uci.edu/ml/machine-learning-databases/00323/microblogPCU.zip)，将其中的`follower_followee.csv`文件复制到本文件夹。


### 运行

1、 首先运行`bitmap`服务

到 `cmd/server`下运行 `go run basalt.go`

2、运行测试程序

到本文件夹(`examples/weibo`)下运行 `go run follow.go`。

> Notice: 确保已复制`follower_followee.csv`到本文件夹