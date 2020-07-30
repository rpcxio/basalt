package main

import (
	"flag"
	"log"
	"os"
	"strings"

	"github.com/rpcxio/basalt"
	"github.com/rpcxio/etcd/raft/raftpb"
)

var (
	addr     = flag.String("addr", ":18972", "the listened address")
	dataFile = flag.String("data", "bitmaps.bdb", "the persisted file")

	peers = flag.String("peers", "http://127.0.0.1:12379", "comma separated peers in a cluster")
	id    = flag.Int("id", 1, "node ID")
	join  = flag.Bool("join", false, "join an existing cluster")
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

	// bitmap
	bitmaps := basalt.NewBitmaps()
	srv := basalt.NewServer(*addr, bitmaps, nil, *dataFile)

	// raft
	proposeC := make(chan string)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	var raftServer *basalt.RaftServer
	getSnapshot := func() ([]byte, error) { return raftServer.GetSnapshot() }
	commitC, errorC, snapshotterReady := basalt.NewRaftNode(*id, strings.Split(*peers, ","), *join, getSnapshot, proposeC, confChangeC)

	raftServer = basalt.NewRaftServer(srv, <-snapshotterReady, proposeC, commitC, errorC)

	if err := srv.Serve(); err != nil {
		log.Fatalf("failed to start basalt services:%v", err)
	}
	srv.Close()
}
