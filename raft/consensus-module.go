package raft

import (
	"log"
	"math/rand"
	"slices"
	"sync"
	"time"
)

type LogEntry struct {
	Cmd  string
	Term int
}

type RaftConsensusModule struct {
	MU        sync.Mutex
	Id        int
	NodeState NodeState

	// =====> statses persisted on persistent storage
	CurrentTerm int
	// -> this stores the candidate who is voted for by this consensus module at the current term (if this consensus node is the leader of this term, this value will be the id of the node itself, if its a follower, this value will be the id of the actual leader who won the elction or maybe this node voted for other node which lost the election) :D
	VotedFor int
	// -> this stores the commands which is replicated by the leader to be applied for the state-machine, for example ["1-SET X 5", "2-GET X"] and this is 1 based index not zero based (according to the paper)
	Log []LogEntry

	// =====> statses persisted on volatile storage for all nodes
	// -> last index of the replicated log entry on quorum of nodes
	LastComittedIndex int
	// -> last index of applied log entry applied to the state machine
	LastAppliedLogIndex int

	// =====> statses persisted on volatile storage for leader node only [reInitilized after election]
	ElectionTimeout                     time.Time
	HeartbeatTimeout                    time.Time
	TimeSinceNodeStartedElectionTimeOut time.Time
}

func NewRaftConsensusModule(nodeId int) *RaftConsensusModule {
	return &RaftConsensusModule{
		MU:                                  sync.Mutex{},
		Id:                                  nodeId,
		NodeState:                           Follower,
		TimeSinceNodeStartedElectionTimeOut: time.Now(),
	}
}

type RequestVoteArgs struct {
	CandidateId int
	Term        int // the term we are voting for leadership for
	// the next two fields are for picking the candidate who has the heighest log as discussed in the paper
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// all nodes reply to this rpc by the term number they saw, and the vote result based on the validation
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

/*
Validation:
  - If the received term is less than the term we already saw, we refuse the vote request
  - If the received term is higher than the one we saw:
  - we enforce our state to be Follower node
  - we enforce the term we saw to be the received term
  - we set the votedFor field to be -1
*/
func (rcm *RaftConsensusModule) RequestVote_RPC(req *RequestVoteArgs) (*RequestVoteReply, error) {
	reply := &RequestVoteReply{
		Term:        rcm.CurrentTerm,
		VoteGranted: false,
	}

	if rcm.CurrentTerm > req.Term {
		return reply, nil
	}

	if rcm.CurrentTerm < req.Term {
		// so this node (the follower) knew that there is a candidate for a higher term, so we vote for it
		rcm.NodeState = Follower
		rcm.CurrentTerm = req.Term
		rcm.VotedFor = req.CandidateId
		// reset my election timeout so after a while if we didn't hear from this candidate becoming a leader or any other candidate, we start an election for our own
		rcm.ElectionTimeout = time.Now()
		go rcm.StartElection()
	}

	if rcm.CurrentTerm == req.Term && (rcm.VotedFor == -1 || rcm.VotedFor == req.CandidateId) {
		reply.VoteGranted = true
		reply.Term = rcm.CurrentTerm
	}

	return reply, nil
}

func (rcm *RaftConsensusModule) ShouldGrantVote(candidateTerm, candidateId int) bool {
	if rcm.CurrentTerm == candidateTerm {
		// if we haven't vote for any node at this current term (votedFor will be -1)
		// OR if we voted for this same candidate at this term (maybe the RequestVote from this candidate is lost when we replied to this candidate previously)
		// so its valide to grant vote for this candidate (even if we granted it before)
		if rcm.VotedFor == -1 || rcm.VotedFor == candidateId {
			return true
		}
		return false
	}

	return false
}

func (rcm *RaftConsensusModule) StartElection() {}

// The paper specified that the randomized timeout should be from 150 to 300 ms to avoid indefintely elections
func (rcm *RaftConsensusModule) GetRandomizedElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

// This function is running on the background for all nodes all the lifetime of the ndoes
// any node has a leader election timeout = 150~300ms
// so every 5 ms I will check if:
//   - This node became a leader of this term
//   - If this term is outdated and we need to mark ourself as follower and update our term and reset our leader election timeout
//   - If the leader election timeout has expired and we still didn't hear from the leader any heartbeat or a RV from any candidate so we have to start an election
func (rcm *RaftConsensusModule) ShouldStartLeaderElection() {
	rcm.MU.Lock()
	lastSeenTerm := rcm.CurrentTerm
	rcm.MU.Unlock()

	ticker := time.NewTicker(time.Millisecond * 5)
	electionTimeoutOfThisNode := rcm.GetRandomizedElectionTimeout()

	for {
		<-ticker.C // block 5 ms then do the checking
		// we will read data from the consensus module so we have to lock whenerver we access any state to be thread safe
		rcm.MU.Lock()
		if lastSeenTerm < rcm.CurrentTerm {
			// we have an outdated term
			rcm.NodeState = Follower
			rcm.MU.Unlock()
			log.Printf("Node [%v] has an outdated term [%v] and the current term is [%v]\n", rcm.Id, lastSeenTerm, rcm.CurrentTerm)
			// TODO : should i reset my leader election timeout ?
			return
		}

		notLeaderState := []NodeState{Follower, Candidate}
		if !slices.Contains(notLeaderState, rcm.NodeState) {
			log.Printf("Node [%v] became the leader of term [%v]\n", rcm.Id, rcm.CurrentTerm)
			rcm.MU.Unlock()
			// here we shouldn't reset out timeout because this node is the leader and will keep sending periodically heartbeats in parallel to all followers
			return
		}

		// now we need to check if we haven't receive any heartbeat or RV requests for the entire election timeout
		timeSinceLastLeaderElectionEnded := time.Since(rcm.TimeSinceNodeStartedElectionTimeOut)
		if timeSinceLastLeaderElectionEnded > electionTimeoutOfThisNode {
			// we need to start an election to become a candidate
			rcm.StartElection()
			rcm.MU.Unlock()
			return
		}

		rcm.MU.Unlock()
	}

}
