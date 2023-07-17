package main

import (
	"context"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/etcd/etcdserver/stats"
	"github.com/coreos/etcd/pkg/types"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/coreos/etcd/rafthttp"
)

const (
	tickInterval = 100 * time.Millisecond
	tickJitter   = 15 * time.Millisecond
)

type rNode struct {
	id      uint64
	peerMap map[uint64]string

	node        raft.Node
	raftStorage *raft.MemoryStorage

	transport *rafthttp.Transport
}

func newRaftNode(id uint64, peerMap map[uint64]string) *rNode {
	n := &rNode{
		id:          id,
		peerMap:     peerMap,
		raftStorage: raft.NewMemoryStorage(),
	}
	go n.startRaft()
	return n
}

func (rn *rNode) startRaft() {
	peers := []raft.Peer{}
	for i := range rn.peerMap {
		peers = append(peers, raft.Peer{ID: uint64(i)})
	}
	c := &raft.Config{
		ID:              rn.id,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         rn.raftStorage,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}
	rn.node = raft.StartNode(c, peers)
	rn.transport = &rafthttp.Transport{
		ID:          types.ID(rn.id),
		ClusterID:   0x1000,
		Raft:        rn,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(strconv.Itoa(int(rn.id))),
		ErrorC:      make(chan error),
	}
	rn.transport.Start()
	for peer, addr := range rn.peerMap {
		if peer != rn.id {
			rn.transport.AddPeer(types.ID(peer), []string{addr})
		}
	}
	go rn.serveRaft()
	go rn.serveChannels()
}

func (rn *rNode) serveRaft() {
	addr := rn.peerMap[rn.id][strings.LastIndex(rn.peerMap[rn.id], ":"):]
	server := http.Server{
		Addr:    addr,
		Handler: rn.transport.Handler(),
	}
	server.ListenAndServe()
}

func (rn *rNode) serveChannels() {

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rn.node.Tick()
		case rd := <-rn.node.Ready():
			rn.raftStorage.Append(rd.Entries)
			rn.transport.Send(rd.Messages)

			if !raft.IsEmptySnap(rd.Snapshot) {
				rn.raftStorage.ApplySnapshot(rd.Snapshot)
			}
			for _, entry := range rd.CommittedEntries {
				switch entry.Type {
				case raftpb.EntryNormal:
					log.Printf("Receive committed data on node %v: %v\n", rn.id, string(entry.Data))
				case raftpb.EntryConfChange:
					var cc raftpb.ConfChange
					cc.Unmarshal(entry.Data)
					rn.node.ApplyConfChange(cc)
				}
			}
			rn.node.Advance()
		case err := <-rn.transport.ErrorC:
			log.Fatal(err)
		}
	}

}

func (rn *rNode) Process(ctx context.Context, m raftpb.Message) error {
	return rn.node.Step(ctx, m)
}
func (rn *rNode) IsIDRemoved(id uint64) bool                           { return false }
func (rn *rNode) ReportUnreachable(id uint64)                          {}
func (rn *rNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}
