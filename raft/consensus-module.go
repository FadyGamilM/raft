package raft

type RaftConsensusModule struct {
	Id        int
	NodeState NodeState

	// =====> statses persisted on persistent storage
	CurrentTerm int
	// -> this stores the candidate who is voted for by this consensus module at the current term (if this consensus node is the leader of this term, this value will be the id of the node itself, if its a follower, this value will be the id of the actual leader who won the elction or maybe this node voted for other node which lost the election) :D
	VotedFor int
	// -> this stores the commands which is replicated by the leader to be applied for the state-machine, for example ["1-SET X 5", "2-GET X"] and this is 1 based index not zero based (according to the paper)
	LogEntries []string

	// =====> statses persisted on volatile storage for all nodes
	// -> last index of the replicated log entry on quorum of nodes
	LastComittedIndex int
	// -> last index of applied log entry applied to the state machine
	LastAppliedLogIndex int

	// =====> statses persisted on volatile storage for leader node only [reInitilized after election]

}
