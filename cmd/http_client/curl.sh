#!/bin/sh

curl http://localhost:8972/add/test1/1
curl http://localhost:8972/addmany/test1/2,3,10,11
curl http://localhost:8972/addmany/test2/1,2,3,20,21
curl http://localhost:8972/diffstore/test3/test1/test2
curl http://localhost:8972/exists/test3/10