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

type Args struct {
	SenderId uint8
	ReqMsg   string
}

type Reply struct {
	ResMsg string
}

func (r *RaftNode) SendMessage_RPC(args *Args, reply *Reply) error {
	reply.ResMsg = fmt.Sprintf("Node [%v] received your message [%s] \n", r.NodeId, args.ReqMsg)
	return nil
}

func (r *RaftNode) SendMsgToPeer(peerId uint8, Msg *Args) (string, error) {
	response := &Reply{}

	r.mu.Lock()
	peerAddress, exists := r.ClusterNodesIds[peerId]
	r.mu.Unlock()
	if !exists {
		return "", fmt.Errorf("peer [%v] not connected to the node [%v] right now", peerId, r.NodeId)
	}

	// connect to the rpc server on this address
	peerClient, err := rpc.Dial("tcp", peerAddress)
	if err != nil {
		return "", fmt.Errorf("failed to dial peer [%v] from node [%v] right now with error [%s]", peerId, r.NodeId, err.Error())
	}

	if err := peerClient.Call("RaftNode.SendMessage_RPC", Msg, response); err != nil {
		return "", fmt.Errorf("failed to call peer [%v] from node [%v] with error [%s]", peerId, r.NodeId, err.Error())
	}

	return response.ResMsg, nil
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
