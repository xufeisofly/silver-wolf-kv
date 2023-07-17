package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"
)

var PeerMap = map[uint64]string{
	1: "http://127.0.0.1:22210",
	2: "http://127.0.0.1:22220",
	3: "http://127.0.0.1:22230",
}

func main() {
	id := flag.Uint64("id", 1, "node id")
	flag.Parse()
	log.Printf("I'am node %v\n", *id)

	cluster := PeerMap
	n := newRaftNode(*id, cluster)

	// if *id == 1 {
	time.Sleep(5 * time.Second)
	for {
		log.Printf("Propose on node %v\n", *id)
		n.node.Propose(context.TODO(), []byte(fmt.Sprintf("hello %v", *id)))
		time.Sleep(time.Second)
		fmt.Println("=====", n.node.Status().String())
	}

	// }

	select {}

}
