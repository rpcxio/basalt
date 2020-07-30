package basalt

import (
	"bytes"
	"encoding/gob"
	"log"
	"strings"

	"github.com/rpcxio/etcd/etcdserver/api/snap"
)

type RaftServer struct {
	proposeC    chan<- string
	bmServer    *Server
	snapshotter *snap.Snapshotter
}

type operaton struct {
	OP  OP
	Val string
}

func NewRaftServer(bmServer *Server, snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *string, errorC <-chan error) *RaftServer {
	s := &RaftServer{proposeC: proposeC, bmServer: bmServer, snapshotter: snapshotter}
	bmServer.bitmaps.writeCallback = s.Propose
	s.readCommits(commitC, errorC)
	go s.readCommits(commitC, errorC)

	return s
}

func (s *RaftServer) Propose(op OP, value string) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(operaton{op, value}); err != nil {
		log.Fatal(err)
	}

	s.proposeC <- buf.String()
}

func (s *RaftServer) readCommits(commitC <-chan *string, errorC <-chan error) {
	for data := range commitC {
		if data == nil {
			snapshot, err := s.snapshotter.Load()
			if err == snap.ErrNoSnapshot {
				return
			}
			if err != nil {
				log.Panic(err)
			}
			log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
			if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
				log.Panic(err)
			}
			continue
		}

		var op operaton
		dec := gob.NewDecoder(bytes.NewBufferString(*data))
		if err := dec.Decode(&op); err != nil {
			log.Fatalf("raftexample: could not decode message (%v)", err)
		}
		s.processOP(op)
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *RaftServer) processOP(op operaton) {
	switch op.OP {
	case BmOpAdd:
		items := strings.SplitN(op.Val, ",", 2)
		if len(items) != 2 {
			log.Printf("wrong request: %+v", op)
			return
		}
		s.bmServer.add(items[0], items[1], false)
	case BmOpAddMany:
		items := strings.SplitN(op.Val, ",", 2)
		if len(items) != 2 {
			log.Printf("wrong request: %+v", op)
			return
		}
		s.bmServer.addMany(items[0], items[1], false)
	case BmOpRemove:
		items := strings.SplitN(op.Val, ",", 2)
		if len(items) != 2 {
			log.Printf("wrong request: %+v", op)
			return
		}
		s.bmServer.remove(items[0], items[1], false)
	case BmOpDrop:
		s.bmServer.drop(op.Val, false)
	case BmOpClear:
		s.bmServer.clear(op.Val, false)
	}
}

func (s *RaftServer) GetSnapshot() ([]byte, error) {
	var buf bytes.Buffer
	err := s.bmServer.bitmaps.Save(&buf)
	return buf.Bytes(), err
}

func (s *RaftServer) recoverFromSnapshot(snapshot []byte) error {
	var buf = bytes.NewBuffer(snapshot)
	return s.bmServer.bitmaps.Read(buf)
}
