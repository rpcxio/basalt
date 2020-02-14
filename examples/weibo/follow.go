package main

import (
	"bufio"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"strings"

	"github.com/go-redis/redis"
)

var (
	addr       = flag.String("addr", "127.0.0.1:8972", "the listened address")
	importData = flag.Bool("import-data", true, "need to import data")
)

var x = fnv.New32()

var names = make(map[string]string)

func main() {
	flag.Parse()

	client := redis.NewClient(&redis.Options{
		Addr: *addr,
	})

	importFollowCsv(client)
	fmt.Println("import succeeded")

	// test
	followee_id := "1640571365" // 罗永浩
	follower_id := "1766187712" // 天天动听
	v := hash(follower_id + "-" + followee_id)
	if exists(client, v) {
		log.Printf("%s 关注了 %s", names[follower_id], names[followee_id])
	} else {
		log.Printf("%s 没有关注 %s", names[follower_id], names[followee_id])
	}

	follower_id = "1618051664" // 头条新闻
	v = hash(follower_id + "-" + followee_id)
	if exists(client, v) {
		log.Printf("%s 关注了 %s", names[follower_id], names[followee_id])
	} else {
		log.Printf("%s 没有关注 %s", names[follower_id], names[followee_id])
	}

	followee_id = "1618051664" // 头条新闻
	follower_id = "1640571365" // 罗永浩
	v = hash(follower_id + "-" + followee_id)
	if exists(client, v) {
		log.Printf("%s 关注了 %s", names[follower_id], names[followee_id])
	} else {
		log.Printf("%s 没有关注 %s", names[follower_id], names[followee_id])
	}

	// 检查互相关注
	checkFollowEachOther(client)
}

func exists(client *redis.Client, v uint32) bool {
	res, err := client.Do("bmadd", "follow", v).Result()
	if err != nil {
		return false
	}
	if rt, ok := res.(int64); !ok || rt == 1 {
		return true
	}
	return false
}

func hash(s string) uint32 {
	x.Reset()
	x.Write([]byte(s))
	return x.Sum32()
}

func importFollowCsv(client *redis.Client) {
	file, err := os.Open("follower_followee.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		items := strings.Split(scanner.Text(), ",")
		key := items[2] + "-" + items[4]
		v := hash(key)

		names[items[2]] = items[1]
		names[items[4]] = items[3]

		if *importData {
			res, err := client.Do("bmadd", "follow", v).Result()
			if err != nil {
				log.Printf("failed to bmadd %s: %v", key, err)
			}
			if rt, ok := res.(int64); !ok || rt != 1 {
				log.Printf("failed to bmadd %s because the result is %v", key, res)
			}
		}

	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func checkFollowEachOther(client *redis.Client) {
	file, err := os.Open("follower_followee.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		items := strings.Split(scanner.Text(), ",")
		key := items[4] + "-" + items[2]
		v := hash(key)
		if exists(client, v) {
			log.Printf("%s: %s 和 %s 互相关注", items[0], names[items[2]], names[items[4]])
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}
