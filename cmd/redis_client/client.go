package main

import (
	"flag"
	"log"

	"github.com/go-redis/redis"
)

var (
	addr = flag.String("addr", "127.0.0.1:8972", "the listened address")
)

func main() {
	flag.Parse()

	client := redis.NewClient(&redis.Options{
		Addr: *addr,
	})

	_, err := client.Ping().Result()
	if err != nil {
		log.Fatalf("failed to ping-pong: %v", err)
	}

	_, err = client.Do("bmadd", "test1", 1).Result() // int64
	if err != nil {
		log.Fatalf("failed to bmadd: %v", err)
	}

	_, err = client.Do("bmaddmany", "test1", 2, 3, 10, 11).Result() // int64
	if err != nil {
		log.Fatalf("failed to bmaddmany: %v", err)
	}

	_, err = client.Do("bmaddmany", "test2", 1, 2, 3, 20, 21).Result() // int64
	if err != nil {
		log.Fatalf("failed to bmaddmany: %v", err)
	}

	_, err = client.Do("bmdiffstore", "test3", "test1", "test2").Result() // int64
	if err != nil {
		log.Fatalf("failed to bmaddmany: %v", err)
	}

	res, err := client.Do("bmexists", "test3", 10).Result() // int64
	if err != nil {
		log.Fatalf("failed to bmaddmany: %v", err)
	}
	if res.(int64) != 1 {
		log.Fatalf("expect exists but found none (0)")
	}

	res, err = client.Do("bmexists", "test3", 20).Result() // int64
	if err != nil {
		log.Fatalf("failed to bmaddmany: %v", err)
	}
	if res.(int64) != 0 {
		log.Fatalf("expect not found but found one (1)")
	}
}
