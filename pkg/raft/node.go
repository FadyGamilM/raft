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

	votedForNodeId *uint8

	// channels for communications between background threads
	startElectionChan       chan struct{}
	receivedAppendEntryChan chan struct{} // this channel will notify the ElectionWorker thread either we got new entry or just a normal heartbeat
	receivedRequestVoteChan chan struct{}

	logs []LogEntry
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
	server.ResetElectionTimeout()

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

	electionTimeoutExpiryTimer := time.NewTimer(time.Duration(electionTimeout * int64(time.Millisecond)))

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
	if r.state == Leader {
		// we cannot start another election if we are the leader of this term
		r.mu.Unlock()
		return
	}

	r.state = Candidate
	r.currentTerm += 1
	r.votedForNodeId = &r.NodeId
	calusterNodesNum := len(r.ClusterNodesIds)
	termOfthisElection := r.currentTerm
	RVArgs := &RequestVoteArgs{
		CandidateId: r.NodeId,
		Term:        r.currentTerm,
	}

	if len(r.logs) > 0 {

	}

	grantedVotes := 1 // voted for myself
	r.mu.Unlock()

	wg := sync.WaitGroup{}

	for peerId, peerAddress := range r.ClusterNodesIds {
		r.mu.Lock()
		if peerId == r.NodeId {
			continue
		}
		r.mu.Unlock()

		wg.Add(1)
		// because one of the go routines might cause state changing to follower or leader .. we must have a condition within every go routine to check if the state is still candidate to send the request
		go func(peerId uint8, peerAddress string) {
			// TODO : should we acquire lock on the mutex or what ??
			r.mu.Lock()
			currentState := r.state
			r.mu.Unlock()

			if currentState != Candidate {
				log.Printf("RV_for_term_[%v]: current_State_changed_to_[%v]\n", termOfthisElection, currentState)
				return
			}

			RVReply, err := r.SendRequestVote(peerId, peerAddress, RVArgs)
			if err != nil {
				log.Printf("RV_for_term_[%v]: error_[%v]_sending_request_vote_to_peer_[%v]\n", termOfthisElection, err.Error(), peerId)
				return
			}

			log.Printf("RV_for_term_[%v]: RV_reply_from_peer_[%v]_is_[%v]\n", termOfthisElection, peerId, RVReply)

			if RVReply.Term >= int(RVArgs.Term) {
				log.Printf("RV_for_term_[%v]: found_higher_term_than_RV_term_from_RV_reply_from_node_[%v]\n", termOfthisElection, peerId)

				r.ToFollower(int64(RVReply.Term))

				return
			}

			if RVReply.VoteGranted {
				// Thread safe
				r.mu.Lock()
				grantedVotes++
				r.mu.Unlock()
			}

			if grantedVotes >= (calusterNodesNum/2)+1 {
				log.Printf("RV_for_term_[%v]: got_quorum_granted_votes_changing_to_leader\n", termOfthisElection)

				r.ToLeader()

				return
			}

		}(peerId, peerAddress)
	}

	wg.Wait()

}

// Ensure Raft Consistency by only voting for candidate with most up to date log
func (r *RaftNode) isCandidateHasTheUpToDateLog(candidateLastLogIndex, candidateLastLogTerm int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.logs) > 0 {
		// if candidate log and follower log end with the same term, the longer log is the more up to date log
		if r.logs[len(r.logs)-1].term == int(candidateLastLogTerm) {
			return candidateLastLogIndex >= r.logs[len(r.logs)-1].index
		}

		// if not the same term, the higher term log is the up to date one
		return candidateLastLogTerm >= r.logs[len(r.logs)-1].term
	}

	// if my log is empty (length is zero), so if the candidate log have at least one entry, its up to date for me
	return candidateLastLogIndex > 0
}

func (r *RaftNode) HandleAppendEntry() {}

func (r *RaftNode) HandleRequestVote() {}

func (r *RaftNode) ToFollower(term int64) {
	// first step i think is to reset our electionTimeout
	// change the state to follower
}

func (r *RaftNode) ToCandidate() {
	// first step i think is to reset our electionTimeout
	// change the state to candidate
}

func (r *RaftNode) ToLeader() {
	// first step i think is to reset our electionTimeout
	// change the state to leader
}
