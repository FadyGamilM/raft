package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/FadyGamilM/raft/pkg/raft"
)

func main() {
	// Read environment variables
	nodeID := os.Getenv("NODE_ID")
	nodeAddress := os.Getenv("NODE_ADDRESS")
	peerAddress := os.Getenv("PEER_ADDRESS")

	if nodeID == "" || nodeAddress == "" || peerAddress == "" {
		fmt.Println("Please set NODE_ID, NODE_ADDRESS, and PEER_ADDRESS environment variables")
		return
	}

	// Create the node
	node_id, _ := strconv.Atoi(nodeID)

	node := raft.NewRaftNode(uint8(node_id), map[uint8]string{uint8(1): "peer"})

	// Start the RPC server
	go raft.StartGrpcServer(node, nodeAddress)

	// Connect to the peer
	node.ClusterNodesIds[uint8(1)] = peerAddress

	// Send a message to the peer
	reply, err := node.SendMsgToPeer(uint8(1), &raft.Args{
		ReqMsg: "Hello from " + nodeID,
	})
	if err != nil {
		fmt.Println("Error sending message:", err)
	} else {
		fmt.Println("Reply from peer:", reply)
	}

	// Keep the program running
	select {}
}
