package raft

import (
	"math/rand"
	"sync"
	"time"
)

type RaftNode struct {
	NodeId          uint8
	ClusterNodesIds map[uint8]string

	minElectionTimeout int64
	maxElectionTimeout int64
	electionTimeout    int64

	currentTerm int64

	mu *sync.Mutex

	// channels for communications between background threads
	startElectionChan       chan struct{}
	receivedAppendEntryChan chan struct{} // this channel will notify the ElectionWorker thread either we got new entry or just a normal heartbeat
	receivedRequestVoteChan chan struct{}
}

func NewRaftNode(Id uint8, clusterNodesIds map[uint8]string) *RaftNode {

	server := &RaftNode{
		NodeId:          Id,
		ClusterNodesIds: clusterNodesIds,
		mu:              &sync.Mutex{},
	}

	server.minElectionTimeout = 150
	server.maxElectionTimeout = 300
	server.currentTerm = 0

	server.ResetElectionTimeout()

	// handles the following :
	// - if timeout expired -> start election
	// - if AppendEntry rpc handler received a rpc request -> perform AE logic
	// - if RequestVote rpc handler received a rpc request -> perform RV logic
	go server.RaftBackgroundWorker()

	return server
}

func (r *RaftNode) ResetElectionTimeout() {
	r.mu.Lock()
	defer r.mu.Unlock()
	// return a random timeout = 150 + ( random-number-from [0:150[ ])
	r.electionTimeout = r.minElectionTimeout + int64(rand.Int63n(r.maxElectionTimeout-r.minElectionTimeout))
}

func (r *RaftNode) RaftBackgroundWorker() {
	r.mu.Lock()
	electionTimeout := r.electionTimeout
	r.mu.Unlock()

	electionTimeoutExpiryTimer := time.NewTimer(time.Duration(electionTimeout * time.Hour.Milliseconds()))

	for {
		select {
		case <-electionTimeoutExpiryTimer.C:
			// should it be a go routine ?
			go r.StartElection()

		case <-r.receivedAppendEntryChan:
			go r.HandleAppendEntry()

		case <-r.receivedRequestVoteChan:
			go r.HandleRequestVote()
		}
	}

}

/*
When this function is used ?

  - When the follower didn't receive a heartbeat (AppendEntries) rpc from the leader or a (RequestVote) from any candidate and the electionTimeout timer is expired

Steps of Execution :

  - Lock on node's state

ResetElectionTimeout

  - Increase the current term

  - Vote for yourself

  - Send RequestVote RPC to all nodes
*/
func (r *RaftNode) StartElection() {
	r.mu.Lock()
	defer r.mu.Unlock()

}

func (r *RaftNode) HandleAppendEntry() {}

func (r *RaftNode) HandleRequestVote() {}
