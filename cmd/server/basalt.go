package main

import (
	"flag"
	"log"
	"os"

	"github.com/rpcxio/basalt"
)

var (
	addr     = flag.String("addr", ":8972", "the listened address")
	dataFile = flag.String("data", "bitmaps.bdb", "the persisted file")
)

func main() {
	flag.Parse()

	if _, err := os.Stat(*dataFile); os.IsNotExist(err) {
		f, err := os.Create(*dataFile)
		if err != nil {
			log.Fatalf("failed to create file %s: %v", *dataFile, err)
		}
		f.Close()
	}

	bitmaps := basalt.NewBitmaps()

	srv := basalt.NewServer(*addr, bitmaps, nil, *dataFile)
	err := srv.Restore()
	if err != nil {
		log.Fatalf("failed to start basalt services:%v", err)
	} else {
		log.Printf("succeeded to restore bitmaps from %s", *dataFile)
	}

	if err := srv.Serve(); err != nil {
		log.Fatalf("failed to start basalt services:%v", err)
	}
	srv.Close()
}
