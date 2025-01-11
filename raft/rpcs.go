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
	rcm.MU.Lock()
	defer rcm.MU.Unlock()
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
	Term     int
	LeaderId int

	// other fields will be implemented later when we implement the rpelicate log behavior
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

func (rcm *RaftConsensusModule) AppendEntry_RPC(args *AppendEntryArgs) (*AppendEntryReply, error) {
	reply := &AppendEntryReply{
		Term:    args.Term,
		Success: false,
	}
	rcm.MU.Lock()
	if rcm.CurrentTerm < args.Term {
		rcm.Follower(args.Term)
	}

	if rcm.CurrentTerm == args.Term {
		// if we are candidate voting for this term we should become a follower because raft allows only one leader per term
		if rcm.NodeState == Candidate {
			rcm.Follower(args.Term)
		}

		reply.Success = true
		// reset our election timer because we jsut received a heartbeat
		rcm.ElectionTimeout = time.Now()
	}

	rcm.MU.Unlock()
	return reply, nil
}
