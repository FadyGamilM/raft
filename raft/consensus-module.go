package raft

import (
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
	ElectionTimeout  time.Time
	HeartbeatTimeout time.Time
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
