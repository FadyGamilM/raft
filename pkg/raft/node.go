package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

type RaftNode struct {
	NodeId          uint8
	ClusterNodesIds map[uint8]string

	state NodeState

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

// the paper described only 3 states for a raft node, so I won't store it as a string to avoid extra space (byte/char) and will represent it using 8 bits (with only 2 used bits)
type NodeState uint8

const (
	Follower  NodeState = 0b00
	Candidate NodeState = 0b01
	Leader    NodeState = 0b10
	Crashed   NodeState = 0b11
)

func (ns NodeState) String() string {
	switch ns {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Crashed:
		return "Crashed"
	default:
		return "UNKNOWN"
	}
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

	r.state = Candidate
	r.currentTerm += 1
	grantedVotes := 1 // voted for myself
	calusterNodesNum := len(r.ClusterNodesIds)
	termVotedFor := r.currentTerm

	RVArgs := &RequestVoteArgs{
		CandidateId: r.NodeId,
		Term:        r.currentTerm,
	}

	for peerId, peerAddress := range r.ClusterNodesIds {
		// because one of the go routines might cause state changing to follower or leader .. we must have a condition within every go routine to check if the state is still candidate to send the request
		go func(peerId uint8, peerAddress string) {
			// TODO : should we acquire lock on the mutex or what ??
			r.mu.Lock()
			currentState := r.state
			r.mu.Unlock()

			if currentState != Candidate {
				log.Printf("RV_for_term_[%v]: current_State_changed_to_[%v]\n", termVotedFor, currentState)
				return
			}

			RVReply, err := r.SendRequestVote(peerId, peerAddress, RVArgs)
			if err != nil {
				log.Printf("RV_for_term_[%v]: error_[%v]_sending_request_vote_to_peer_[%v]\n", termVotedFor, err.Error(), peerId)
				return
			}

			log.Printf("RV_for_term_[%v]: RV_reply_from_peer_[%v]_is_[%v]\n", termVotedFor, peerId, RVReply)

			if RVReply.Term >= int(RVArgs.Term) {
				log.Printf("RV_for_term_[%v]: found_higher_term_than_RV_term_from_RV_reply_from_node_[%v]\n", termVotedFor, peerId)

				r.ToFollower()

				return
			}

			if RVReply.VoteGranted {
				grantedVotes++
			}

			if grantedVotes >= (calusterNodesNum/2)+1 {
				log.Printf("RV_for_term_[%v]: got_quorum_granted_votes_changing_to_leader\n", termVotedFor)

				r.ToLeader()

				return
			}

		}(peerId, peerAddress)
	}

}

func (r *RaftNode) HandleAppendEntry() {}

func (r *RaftNode) HandleRequestVote() {}

func (r *RaftNode) ToFollower() {
	// first step i think is to reset our electionTimeout
}

func (r *RaftNode) ToCandidate() {}

func (r *RaftNode) ToLeader() {}
