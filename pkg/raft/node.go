package raft

import (
	"math/rand"
	"sync"
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
	// return a random timeout = 150 + ( random-number-from [0:150[ ])
	server.electionTimeout = server.minElectionTimeout + int64(rand.Int63n(server.maxElectionTimeout-server.minElectionTimeout))

	return server
}

func (r *RaftNode) ElectionWorker() {
	r.mu.Lock()
	defer r.mu.Unlock()

}

/*
When this function is used ?

  - When the follower didn't receive a heartbeat (AppendEntries) rpc from the leader or a (RequestVote) from any candidate and the electionTimeout timer is expired

Steps of Execution :

  - Lock on node's state

  - Increase the current term

  - Vote for yourself

  - Send RequestVote RPC to all nodes
*/
func (r *RaftNode) StartElection() {
	r.mu.Lock()
	defer r.mu.Unlock()

}
