package raft

type RaftNode struct {
	NodeId          uint8
	ClusterNodesIds []uint8

	MinElectionTimeout int64
	MaxElectionTimeout int64
	ElectionTimeout    int64
}

func NewRaftNode(Id uint8, clusterNodesIds []uint8) *RaftNode {
	server := &RaftNode{
		NodeId:          Id,
		ClusterNodesIds: clusterNodesIds,
	}

	server.MinElectionTimeout = 150
	server.MaxElectionTimeout = 300

	return server
}
