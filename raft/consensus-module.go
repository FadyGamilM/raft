package raft

import (
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
	"sync"
	"time"
)

type LogEntry struct {
	Cmd  string
	Term int
}

type RaftConsensusModule struct {
	MU        sync.Mutex
	Id        int
	NodeState NodeState

	// =====> statses persisted on persistent storage
	CurrentTerm int
	// -> this stores the candidate who is voted for by this consensus module at the current term (if this consensus node is the leader of this term, this value will be the id of the node itself, if its a follower, this value will be the id of the actual leader who won the elction or maybe this node voted for other node which lost the election) :D
	VotedFor int
	// -> this stores the commands which is replicated by the leader to be applied for the state-machine, for example ["1-SET X 5", "2-GET X"] and this is 1 based index not zero based (according to the paper)
	Log []LogEntry

	// =====> statses persisted on volatile storage for all nodes
	// -> last index of the replicated log entry on quorum of nodes
	LastComittedIndex int
	// -> last index of applied log entry applied to the state machine
	LastAppliedLogIndex int

	// =====> statses persisted on volatile storage for leader node only [reInitilized after election]
	ElectionTimeout  time.Time
	HeartbeatTimeout time.Duration
	// TimeSinceNodeStartedElectionTimeOut time.Time

	// TODO : we will implement a proper cluster membership algorithm later and i think the type of this NodesIds will change to hold the configurations
	NodesIds []int

	// for network communicaton between nodes
	rpcNodes map[int]*rpc.Client

	matchIndex []*NodeLastReplicatedLogIndex
	nextIndex  []*NodeNextLogIndexToSend
}

func NewRaftConsensusModule(nodeId int, clusterConfigurations ClusterConfigurations) *RaftConsensusModule {
	raftConsensusModule := &RaftConsensusModule{
		MU:               sync.Mutex{},
		Id:               nodeId,
		NodeState:        Follower,
		ElectionTimeout:  time.Now(),
		NodesIds:         clusterConfigurations.NodesIds,
		rpcNodes:         make(map[int]*rpc.Client),
		HeartbeatTimeout: time.Millisecond * 20,
	}

	for _, peerId := range clusterConfigurations.NodesIds {
		if peerId != nodeId {
			// Assuming each node runs on a different port starting from 8000
			rpcClient, err := rpc.Dial("tcp", fmt.Sprintf("localhost:%d", 8000+peerId))
			if err != nil {
				log.Printf("Failed to connect to peer %d: %v", peerId, err)
				continue
			}
			raftConsensusModule.rpcNodes[peerId] = rpcClient
		}
	}

	return raftConsensusModule
}

func (rcm *RaftConsensusModule) ShouldGrantVote(candidateTerm, candidateId int) bool {
	if rcm.CurrentTerm == candidateTerm {
		// if we haven't vote for any node at this current term (votedFor will be -1)
		// OR if we voted for this same candidate at this term (maybe the RequestVote from this candidate is lost when we replied to this candidate previously)
		// so its valide to grant vote for this candidate (even if we granted it before)
		if rcm.VotedFor == -1 || rcm.VotedFor == candidateId {
			return true
		}
		return false
	}

	return false
}

// The paper specified that the randomized timeout should be from 150 to 300 ms to avoid indefintely elections
func (rcm *RaftConsensusModule) GetRandomizedElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

// This function is running on the background for all nodes all the lifetime of the ndoes
// any node has a leader election timeout = 150~300ms
// so every 5 ms I will check if:
//   - This node became a leader of this term
//   - If this term is outdated and we need to mark ourself as follower and update our term and reset our leader election timeout
//   - If the leader election timeout has expired and we still didn't hear from the leader any heartbeat or a RV from any candidate so we have to start an election
func (rcm *RaftConsensusModule) BackgroundLeaderElectionTimer() {
	rcm.MU.Lock()
	lastSeenTerm := rcm.CurrentTerm
	rcm.MU.Unlock()

	ticker := time.NewTicker(time.Millisecond * 5)
	electionTimeoutOfThisNode := rcm.GetRandomizedElectionTimeout()

	for {
		<-ticker.C // block 5 ms then do the checking
		// we will read data from the consensus module so we have to lock whenerver we access any state to be thread safe
		rcm.MU.Lock()
		if lastSeenTerm < rcm.CurrentTerm {
			// we have an outdated term
			rcm.NodeState = Follower
			rcm.MU.Unlock()
			log.Printf("Node [%v] has an outdated term [%v] and the current term is [%v]\n", rcm.Id, lastSeenTerm, rcm.CurrentTerm)
			// TODO : should i reset my leader election timeout ?
			return
		}

		// since we are calling this BackgroundLeaderElectionTimer into go routine at the end of StartElection() rpc incase we didn't won the electon, we need to ensure here that this new election will be triggered if we didn't won the old election that spawn this go routine
		if rcm.NodeState == Leader {
			log.Printf("Node [%v] became the leader of term [%v]\n", rcm.Id, rcm.CurrentTerm)
			rcm.MU.Unlock()
			// here we shouldn't reset out timeout because this node is the leader and will keep sending periodically heartbeats in parallel to all followers
			return
		}

		// now we need to check if we haven't receive any heartbeat or RV requests for the entire election timeout
		timeSinceLastLeaderElectionEnded := time.Since(rcm.ElectionTimeout)
		if timeSinceLastLeaderElectionEnded > electionTimeoutOfThisNode {
			// we need to start an election to become a candidate
			rcm.MU.Unlock() // i beleive we should unlock before starting the election because the election will perform in-parallel rpc calls to all nodes, and these rpc requests might hangout for a while and this will affect liveness of our node state .. maybe this needs more further investgation later
			rcm.StartElection()
			return
		}

		rcm.MU.Unlock()
	}

}

// a follower decided to start an election, which means the follower will become a candidate, and start sending RV requests to all other nodes/
// StartElection method is called from the background timeout election job, and this job already locks on the consensus module states so if we locked here we will fail
func (rcm *RaftConsensusModule) StartElection() {
	rcm.MU.Lock()
	rcm.NodeState = Candidate
	rcm.VotedFor = rcm.Id
	rcm.CurrentTerm += 1
	RVCandidateId := rcm.Id
	RVCandidateTerm := rcm.CurrentTerm
	rcm.ElectionTimeout = time.Now() // reset our election timeout
	rcm.MU.Unlock()

	votesGranted := 1

	for _, nodeId := range rcm.NodesIds {
		go func(nodeId int) {
			// TODO : should we lock on the mutex ??
			rcm.MU.Lock()
			defer rcm.MU.Unlock()

			if nodeId == rcm.Id {
				return
			}

			RVArgs := &RequestVoteArgs{
				CandidateId: RVCandidateId,
				Term:        RVCandidateTerm,
			}
			RVReply := &RequestVoteReply{}
			if err := rcm.rpcNodes[nodeId].Call("RaftConsensusModule.RequestVote_RPC", RVArgs, RVReply); err != nil {
				log.Printf("failed to send RV rpc call to node [%v] for term [%v] where the candidate is [%v] error [%v]\n", nodeId, RVCandidateTerm, RVCandidateId, err.Error())
				return
			}

			// TODO : do we really need this ?
			// we always have to check while we wait for the response that we are still candidate

			// Check if we got this node's vote
			if RVReply.Term > RVCandidateTerm {
				log.Printf("discovered a higher term [%v] from node [%v] while I was candidate for term = [%v]\n", RVReply.Term, nodeId, RVCandidateTerm)
				rcm.Follower(RVReply.Term)
				return
			}

			if RVReply.Term == RVCandidateTerm {
				votesGranted++
				if rcm.IsQuorum(votesGranted) {
					rcm.Leader()
				}

			}

		}(nodeId)
	}

	// always run the background job for another election timeout incase we didn't won this election, if we won, the state checking in the background job method will not trigger the election logic
	go rcm.BackgroundLeaderElectionTimer()
}

func (rcm *RaftConsensusModule) IsQuorum(votesGranted int) bool {
	return votesGranted >= ((len(rcm.NodesIds) / 2) + 1)
}

func (rcm *RaftConsensusModule) Leader() {
	rcm.MU.Lock()
	rcm.NodeState = Leader
	log.Printf("leader of term [%v] is node [%v]\n", rcm.CurrentTerm, rcm.Id)
	heartbeatTickerDuration := rcm.HeartbeatTimeout
	rcm.MU.Unlock()

	heartbeatTicker := time.NewTicker(heartbeatTickerDuration)

	for {
		// first heartbeat is sent immeiedtly, then we send it every 20 ms
		rcm.MU.Lock()
		AppendEntryCurrentTerm := rcm.CurrentTerm
		AppendEntryLeaderId := rcm.Id
		rcm.MU.Unlock()

		for _, nodeId := range rcm.NodesIds {
			go func(nodeId int) {
				AEReply := &AppendEntryReply{}
				AEArgs := &AppendEntryArgs{
					Term:     AppendEntryCurrentTerm,
					LeaderId: AppendEntryLeaderId,
				}
				if err := rcm.rpcNodes[nodeId].Call("RaftConsensusModule.AppendEntry_RPC", AEArgs, AEReply); err != nil {
					log.Printf("failed to heartbeat node [%v] at term [%v]\n", nodeId, AppendEntryCurrentTerm)
					return
				}

				rcm.MU.Lock()
				if AEReply.Success {
					if AEReply.Term > rcm.CurrentTerm {
						rcm.Follower(AEReply.Term)
					}
				}
				rcm.MU.Unlock()
			}(nodeId)
		}

		<-heartbeatTicker.C
		// are we still the leader ?
		rcm.MU.Lock()
		if rcm.NodeState != Leader {
			// if we are not the leader, this for sure because other go routine called Follower() method, so no need to call it again
			rcm.MU.Unlock()
			return
		}
		rcm.MU.Unlock()
	}
}

func (rcm *RaftConsensusModule) Follower(term int) {
	rcm.NodeState = Follower
	rcm.VotedFor = -1
	rcm.ElectionTimeout = time.Now()
	rcm.CurrentTerm = term
	go rcm.BackgroundLeaderElectionTimer()
}
