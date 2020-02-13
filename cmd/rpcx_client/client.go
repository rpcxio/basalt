package main

import (
	"context"
	"flag"
	"log"

	"github.com/rpcxio/basalt"
	"github.com/smallnest/rpcx/client"
)

var (
	addr = flag.String("addr", "127.0.0.1:8972", "the listened address")
)

func main() {
	flag.Parse()

	d := client.NewPeer2PeerDiscovery("tcp@"+*addr, "")
	xclient := client.NewXClient("Bitmap", client.Failtry, client.RandomSelect, d, client.DefaultOption)
	defer xclient.Close()

	var ok bool

	xclient.Call(context.Background(), "Add", &basalt.BitmapValueRequest{"test1", 1}, &ok)
	xclient.Call(context.Background(), "AddMany", &basalt.BitmapValuesRequest{"test1", []uint32{2, 3, 10, 11}}, &ok)

	xclient.Call(context.Background(), "Add", &basalt.BitmapValueRequest{"test2", 1}, &ok)
	xclient.Call(context.Background(), "AddMany", &basalt.BitmapValuesRequest{"test2", []uint32{2, 3, 20, 21}}, &ok)

	var exist bool
	xclient.Call(context.Background(), "Exists", &basalt.BitmapValueRequest{"test1", 10}, &exist)
	if !exist {
		log.Fatalf("10 not found")
	}

	xclient.Call(context.Background(), "DiffStore", &basalt.BitmapDstAndPairRequest{"test3", "test1", "test2"}, &ok)
	xclient.Call(context.Background(), "Exists", &basalt.BitmapValueRequest{"test3", 10}, &exist)
	if !exist {
		log.Fatalf("10 not found")
	}
}
