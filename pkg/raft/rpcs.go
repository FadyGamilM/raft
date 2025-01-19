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
		log.Printf("node_[%v]_received_AppendEntries_rpc_from_leader_[%v]_with_term_[%v]_less_than_current_term_[%v]", nodeId, args.leaderId, currentTerm, args.term)
		return nil
	}

	// we should update our term and change our state to follower (in case we became a leader and received this appendEntires rpc) incase we received a higher term request
	if currentTerm < int64(args.term) {
		r.mu.Lock()
		// reset our states required to represent a follower node
		r.ToFollower(int64(args.term))
		r.mu.Unlock()
	}

	// the checking of log consistency after preparing the entryIndexAtPrevLogIndex and entryTermAtPrevLogTerm
	if entryIndexAtPrevLogIndex == -1 {
		log.Printf("node_[%v]_received_AppendEntries_rpc_from_leader_[%v]_but_has_no_log_entry_at_prevLogIndex_[%v]", nodeId, args.leaderId, args.prevLogIndex)
		return nil // the leader will decrement his knowledge of our preLogIndex and send it into the next AE rpc
	}
	// so we have log at this index, lets check its term
	if entryTermAtPrevLogIndex != args.prevLogTerm {
		log.Printf("node_[%v]_received_AppendEntries_rpc_from_leader_[%v]_has_log_entry_at_prevLogIndex_[%v]_but_has_term_[%v]_while_prevLogTerm_is_[%v]", nodeId, args.leaderId, args.prevLogIndex, entryTermAtPrevLogIndex, args.prevLogTerm)
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
	r.commitIndex = int64(min(args.leaderCommitIndex, len(r.logs)-1))
	r.mu.Unlock()

	// this handler also should listen on a channel that the worker after processsing the request should return the reply into this channel here so this handler return the reply to the rpc caller
	return nil
}

func (r *RaftNode) RequestVoteHandler_RPC(args *RequestVoteArgs, reply *RequestVoteReply) error {

	r.receivedRequestVoteChan <- struct{}{}
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
