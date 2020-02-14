## 新浪微博数据集MicroblogPCU

https://archive.ics.uci.edu/ml/datasets/microblogPCU

[MicroblogPCU](https://archive.ics.uci.edu/ml/machine-learning-databases/00323/)是从新浪微博採集到的。它能够被用于研究机器学习方法和社会关系研究。

这个数据集被原作者用于探索微博中的spammers（发送垃圾信息的人）。他们的demo在[这里](http://sd.skyclass.net:8080/Spammer/dia.jsp)。

我们解析`follower_followee.csv`(关注者-被关注者关系)， 以`follower_id`-`followee_id`作为key进行hash,然后将输入放入`follow` Bitmap中。

随后找一些ID看看是否有关注关系。