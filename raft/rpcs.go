package raft

import "time"

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
	rcm.mu.Lock()
	defer rcm.mu.Unlock()
	reply := &RequestVoteReply{
		Term:        rcm.CurrentTerm,
		VoteGranted: false,
	}

	if rcm.CurrentTerm > req.Term {
		return reply, nil
	}

	if rcm.CurrentTerm < req.Term {
		rcm.Follower(req.Term)
	}

	if rcm.ShouldGrantVote(req.Term, req.CandidateId) {
		rcm.VotedFor = req.CandidateId
		reply.VoteGranted = true
		reply.Term = rcm.CurrentTerm
	}

	return reply, nil
}

type AppendEntryArgs struct {
	term              int
	leaderId          int
	prevLogIndex      int
	prevLogTerm       int
	entries           []LogEntry
	leaderCommitIndex int
}

type AppendEntryReply struct {
	term    int
	success bool
}

func (rcm *RaftConsensusModule) AppendEntry_RPC(args *AppendEntryArgs) (*AppendEntryReply, error) {
	reply := &AppendEntryReply{
		term:    args.term,
		success: false,
	}
	rcm.mu.Lock()
	if rcm.CurrentTerm < args.term {
		rcm.Follower(args.term)
	}

	if rcm.CurrentTerm == args.term {
		// if we are candidate voting for this term we should become a follower because raft allows only one leader per term
		if rcm.NodeState == Candidate {
			rcm.Follower(args.term)
		}

		// reset our election timer because we jsut received a heartbeat
		rcm.electionTimeout = time.Now()

		
		reply.success = true
	}

	rcm.mu.Unlock()
	return reply, nil
}
