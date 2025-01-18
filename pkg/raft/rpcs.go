/*
Conventions I am going to follow in this file :
---------------------------------------------
  - Any method ends with "_RPC" is responsible for the business logic of this rpc, not the network calls.
*/
package raft

import (
	"fmt"
	"net"
	"net/rpc"
)

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

func (r *RaftNode) AppendEntryHandler_RPC(args *AppendEntryArgs, reply *AppendEntryReply) error {
	return nil
}

func (r *RaftNode) RequestVoteHandler_RPC(args *RequestVoteArgs, reply *RequestVoteReply) error {
	return nil
}

// =========== sending rpcs requests (network calls) ============
func (r *RaftNode) SendAppendEntry(peerId uint8, req *AppendEntryArgs) (*AppendEntryReply, error) {
	r.mu.Lock()
	peerAddress, exists := r.ClusterNodesIds[peerId]
	r.mu.Unlock()

	res := &AppendEntryReply{}

	if !exists {
		return res, fmt.Errorf("peer_[%v]_is_not_connected_to_the_node_[%v]_right_now", peerId, r.NodeId)
	}

	peerClient, err := rpc.Dial("tcp", peerAddress)
	if err != nil {
		return res, fmt.Errorf("failed_to_dial_peer_[%v]_with_address_[%v]_from_node_[%v]_with_error_[%s]", peerId, peerAddress, r.NodeId, err.Error())
	}

	if err := peerClient.Call("RaftNode.AppendEntryHandler_RPC", req, res); err != nil {
		return res, fmt.Errorf("AppendEntry()_from_peer_[%v]_with_address_[%v]_returned_error_[%v]", peerId, peerAddress, err.Error())
	}

	return res, nil
}

func (r *RaftNode) SendRequestVote(peerId uint8, req *RequestVoteArgs) (*RequestVoteReply, error) {
	r.mu.Lock()
	peerAddress, exists := r.ClusterNodesIds[peerId]
	r.mu.Unlock()

	res := &RequestVoteReply{}

	if !exists {
		return res, fmt.Errorf("peer_[%v]_is_not_connected_to_the_node_[%v]_right_now", peerId, r.NodeId)
	}

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
