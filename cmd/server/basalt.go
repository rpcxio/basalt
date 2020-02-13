package main

import (
	"flag"
	"log"

	"github.com/rpcxio/basalt"
)

var (
	addr = flag.String("addr", ":8972", "the listened address")
)

func main() {
	flag.Parse()

	bitmaps := basalt.NewBitmaps()
	// TODO: restore from local file

	srv := basalt.NewServer(*addr, bitmaps, nil)
	if err := srv.Serve(); err != nil {
		log.Fatalf("failed to start basalt services:%v", err)
	}
	srv.Close()
}
