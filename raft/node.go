package raft

// the paper described only 3 states for a raft node, so I won't store it as a string to avoid extra space (byte/char) and will represent it using 8 bits (with only 2 used bits)
type NodeState uint8

const (
	Follower  NodeState = 0b00
	Candidate NodeState = 0b01
	Leader    NodeState = 0b10
	Crashed   NodeState = 0b11
)

func (ns NodeState) String() string {
	switch ns {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Crashed:
		return "Crashed"
	default:
		return "UNKNOWN"
	}
}

// to handle the matchIndex[] field
// the server should know for each node on the cluster, what is the last replicated log index on it's log (incase that our server is the leader)
type NodeLastReplicatedLogIndex struct {
	NodeId                 int
	LastReplicatedLogIndex int
}

// to handle the nextIndex[] field
// the server should know for each node on the cluster, what is the next log index should be sent (incase that our server is the leader)
type NodeNextLogIndexToSend struct {
	NodeId             int
	NextLogIndexToSend int
}

type ClusterConfigurations struct {
	NodesIds []int
}
