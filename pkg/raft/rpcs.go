/*
Conventions I am going to follow in this file :
---------------------------------------------
  - Any method ends with "_RPC" is responsible for the business logic of this rpc, not the network calls.
*/
package raft

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
)

type RequestVoteArgs struct {
	CandidateId int
	Term        int64 // the term we are voting for leadership for
	// the next two fields are for picking the candidate who has the heighest log as discussed in the paper
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// all nodes reply to this rpc by the term number they saw, and the vote result based on the validation
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
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

type LogEntry struct {
	cmd   string
	term  int
	index int
}

// onyl append if we are at the same term
func (r *RaftNode) AppendEntryHandler_RPC(args *AppendEntryArgs, reply *AppendEntryReply) error {
	// TODO: i beleive we sohuld  deifne this channel as a chan of AE_Args not just struct
	// r.receivedAppendEntryChan <- struct{}{}

	r.mu.Lock()
	currentTerm := r.currentTerm
	currentState := r.state
	entryIndexAtPrevLogIndex, entryTermAtPrevLogIndex := -1, -1

	// if me as follower have a log entry at the prevLogIndex && this logEntry at this index has term matches the PrevLogEntry .. so we match the leader logs at this point
	if len(r.logs) >= args.prevLogIndex && args.prevLogIndex > 0 {
		entryIndexAtPrevLogIndex = r.logs[args.prevLogIndex].index
		entryTermAtPrevLogIndex = r.logs[args.prevLogIndex].term
	}

	nodeId := r.NodeId
	r.mu.Unlock()

	reply = &AppendEntryReply{
		term:    int(r.currentTerm),
		success: false,
	}

	if currentState == Leader {
		return nil
	}

	// we should reject appendEntries rpc from previous terms
	if currentTerm > int64(args.term) {
		log.Printf("node_[%v]_received_AppendEntries_rpc_from_leader_[%v]_with_term_[%v]_less_than_current_term_[%v]\n", nodeId, args.leaderId, currentTerm, args.term)
		return nil
	}

	// we should update our term and change our state to follower (in case we were a leader and received this appendEntires rpc with a higher term request)
	if currentTerm < int64(args.term) {
		// reset our states required to represent a follower node
		r.ToFollower(int64(args.term))
	}

	// NOW we have the same term we can negotiate if this leader is valid or not

	// TODO : should we add this validation before any other vlaidaton on the AppendEntries logic and after validation on the term to separate between the heartbeat AE rpc and the regular AE rpc ?
	if len(args.entries) == 0 {
		log.Printf("received_heartbeat_appendEntries_rpc_from_leader_[%v]_at_term_[%v]\n", args.leaderId, args.term)
		r.ToFollower(int64(args.term))
		reply = &AppendEntryReply{
			term:    int(args.term),
			success: true,
		}
		return nil
	}

	// the checking of log consistency after preparing the entryIndexAtPrevLogIndex and entryTermAtPrevLogTerm
	if entryIndexAtPrevLogIndex == -1 {
		log.Printf("node_[%v]_received_AppendEntries_rpc_from_leader_[%v]_but_has_no_log_entry_at_prevLogIndex_[%v]\n", nodeId, args.leaderId, args.prevLogIndex)
		return nil // the leader will decrement his knowledge of our preLogIndex and send it into the next AE rpc
	}
	// so we have log at this index, lets check its term
	if entryTermAtPrevLogIndex != args.prevLogTerm {
		log.Printf("node_[%v]_received_AppendEntries_rpc_from_leader_[%v]_has_log_entry_at_prevLogIndex_[%v]_but_has_term_[%v]_while_prevLogTerm_is_[%v]\n", nodeId, args.leaderId, args.prevLogIndex, entryTermAtPrevLogIndex, args.prevLogTerm)
		return nil // the leader will decrement his knowledge of our preLogIndex and send it into the next AE rpc
	}

	// so we matched a consistent log with the leader at the point of PrevLogIndex
	// so we are ready to append the entries from the prevLogIndex + 1 to the end
	r.mu.Lock()
	r.logs = append(r.logs[:args.prevLogIndex], args.entries...)
	r.mu.Unlock()

	// finally update our local commit index
	// i will set it = the min of leader's commit index or the index of last entry at my log because we might have logs that leader don't know about so we will take the leader's commit index in this case (since we alreay replicated the leader log we are sure that we already covered this leader's commit index)
	r.mu.Lock()
	newCommittedIndex := int64(min(args.leaderCommitIndex, len(r.logs)-1))
	if r.commitIndex < newCommittedIndex {
		latestAppliedCommittedIndexToStateMachine := r.commitIndex
		r.commitIndex = newCommittedIndex
		go r.applyCommittedEntriesToStateMachine(latestAppliedCommittedIndexToStateMachine, r.commitIndex)
	}
	r.mu.Unlock()

	// this handler also should listen on a channel that the worker after processsing the request should return the reply into this channel here so this handler return the reply to the rpc caller
	return nil
}

func (r *RaftNode) RequestVoteHandler_RPC(args *RequestVoteArgs, reply *RequestVoteReply) error {
	// TODO: i beleive we sohuld deifne this channel as a chan of RV_Args not just struct
	// r.receivedRequestVoteChan <- struct{}{}

	r.mu.Lock()
	currentTerm := r.currentTerm
	r.mu.Unlock()

	reply = &RequestVoteReply{
		Term:        int(currentTerm),
		VoteGranted: false,
	}

	// if we have higher term just return because its an invalid RV request
	if currentTerm > args.Term {
		log.Printf("received_requestVote_rpc_with_lower_term_[%v]_from_candidate_[%v]_while_my_current_term_is_[%v]\n", args.Term, args.CandidateId, currentTerm)
		return nil
	}

	// if we have outdated term, we just turn ourself into follower to set our current term to the updated term, them set the reply term with the last term we updtaead ourself with
	if currentTerm < args.Term {
		r.ToFollower(args.Term)
		reply.Term = int(args.Term)
	}

	// NOW we have the same term we can negotiate if this leader is valid or not

	// did we voted for any candidate at this term or not ?
	r.mu.Lock()
	if r.votedForNodeId != nil && r.votedForNodeId != &args.CandidateId {
		log.Printf("we_already_voted_for_another_candidate_with_id_[%v]_for_this_term_[%v]_before\n", *r.votedForNodeId, args.Term)
		return nil
	}
	r.mu.Unlock()

	// so we are at the same term or lower than the candidate term
	// we need to check the received candidate log vs ours, to check if this candidate has the up to date log or not
	isCandidateUpToDate := r.isCandidateHasTheUpToDateLog(args.LastLogIndex, args.LastLogTerm)
	if isCandidateUpToDate {
		r.mu.Lock()
		reply.VoteGranted = true
		r.votedForNodeId = &args.CandidateId
		// reset our election timer because we already heared form a candidate
		r.ResetElectionTimeout()
		r.mu.Unlock()
	}

	return nil
}

// =========== sending rpcs requests (network calls) ============
func (r *RaftNode) SendAppendEntry(peerId int, peerAddress string, req *AppendEntryArgs) (*AppendEntryReply, error) {
	res := &AppendEntryReply{}

	peerClient, err := rpc.Dial("tcp", peerAddress)
	if err != nil {
		return res, fmt.Errorf("failed_to_dial_peer_[%v]_with_address_[%v]_from_node_[%v]_with_error_[%s]", peerId, peerAddress, r.NodeId, err.Error())
	}

	if err := peerClient.Call("RaftNode.AppendEntryHandler_RPC", req, res); err != nil {
		return res, fmt.Errorf("AppendEntry()_from_peer_[%v]_with_address_[%v]_returned_error_[%v]", peerId, peerAddress, err.Error())
	}

	return res, nil
}

func (r *RaftNode) SendRequestVote(peerId int, peerAddress string, req *RequestVoteArgs) (*RequestVoteReply, error) {
	res := &RequestVoteReply{}

	peerClient, err := rpc.Dial("tcp", peerAddress)
	if err != nil {
		return res, fmt.Errorf("failed_to_dial_peer_[%v]_with_address_[%v]_from_node_[%v]_with_error_[%s]", peerId, peerAddress, r.NodeId, err.Error())
	}

	if err := peerClient.Call("RaftNode.RequestVoteHandler_RPC", req, res); err != nil {
		return res, fmt.Errorf("RequestVote()_from_peer_[%v]_with_address_[%v]_returned_error_[%v]", peerId, peerAddress, err.Error())
	}

	return res, nil
}

func StartGrpcServer(node *RaftNode, address string) {
	rpc.Register(node)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Node %v listening on %s\n", node.NodeId, address)
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}
