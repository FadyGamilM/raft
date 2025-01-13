package raft

import (
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
	"sync"
	"time"
)

type RaftConsensusModule struct {
	mu        sync.Mutex
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
	electionTimeout  time.Time
	heartbeatTimeout time.Duration
	// TimeSinceNodeStartedElectionTimeOut time.Time

	// TODO : we will implement a proper cluster membership algorithm later and i think the type of this NodesIds will change to hold the configurations
	NodesIds []int

	// for network communicaton between nodes
	rpcNodes map[int]*rpc.Client

	// these slices will be defined as following :
	// leader knows the cluster configurations and will store that nextIndex[0] is for node 0 and so on and the inside value will contains the {NodeId : 0, Nextindex : bla bla}
	matchIndex []*NodeLastReplicatedLogIndex
	nextIndex  []*NodeNextLogIndexToSend

	// to catch updates on the commit index
	commitIndexUpdatedChan chan struct{}

	commitChan chan LogEntry
}

func NewRaftConsensusModule(nodeId int, clusterConfigurations ClusterConfigurations) *RaftConsensusModule {
	raftConsensusModule := &RaftConsensusModule{
		mu:                     sync.Mutex{},
		Id:                     nodeId,
		NodeState:              Follower,
		electionTimeout:        time.Now(),
		NodesIds:               clusterConfigurations.NodesIds,
		rpcNodes:               make(map[int]*rpc.Client),
		heartbeatTimeout:       time.Millisecond * 20,
		commitIndexUpdatedChan: make(chan struct{}),
		commitChan:             make(chan LogEntry),
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
	rcm.mu.Lock()
	lastSeenTerm := rcm.CurrentTerm
	rcm.mu.Unlock()

	ticker := time.NewTicker(time.Millisecond * 5)
	electionTimeoutOfThisNode := rcm.GetRandomizedElectionTimeout()

	for {
		<-ticker.C // block 5 ms then do the checking
		// we will read data from the consensus module so we have to lock whenerver we access any state to be thread safe
		rcm.mu.Lock()
		if lastSeenTerm < rcm.CurrentTerm {
			// we have an outdated term
			rcm.NodeState = Follower
			rcm.mu.Unlock()
			log.Printf("Node [%v] has an outdated term [%v] and the current term is [%v]\n", rcm.Id, lastSeenTerm, rcm.CurrentTerm)
			// TODO : should i reset my leader election timeout ?
			return
		}

		// since we are calling this BackgroundLeaderElectionTimer into go routine at the end of StartElection() rpc incase we didn't won the electon, we need to ensure here that this new election will be triggered if we didn't won the old election that spawn this go routine
		if rcm.NodeState == Leader {
			log.Printf("Node [%v] became the leader of term [%v]\n", rcm.Id, rcm.CurrentTerm)
			rcm.mu.Unlock()
			// here we shouldn't reset out timeout because this node is the leader and will keep sending periodically heartbeats in parallel to all followers
			return
		}

		// now we need to check if we haven't receive any heartbeat or RV requests for the entire election timeout
		timeSinceLastLeaderElectionEnded := time.Since(rcm.electionTimeout)
		if timeSinceLastLeaderElectionEnded > electionTimeoutOfThisNode {
			// we need to start an election to become a candidate
			rcm.mu.Unlock() // i beleive we should unlock before starting the election because the election will perform in-parallel rpc calls to all nodes, and these rpc requests might hangout for a while and this will affect liveness of our node state .. maybe this needs more further investgation later
			rcm.StartElection()
			return
		}

		rcm.mu.Unlock()
	}

}

// a follower decided to start an election, which means the follower will become a candidate, and start sending RV requests to all other nodes/
// StartElection method is called from the background timeout election job, and this job already locks on the consensus module states so if we locked here we will fail
func (rcm *RaftConsensusModule) StartElection() {
	rcm.mu.Lock()
	rcm.NodeState = Candidate
	rcm.VotedFor = rcm.Id
	rcm.CurrentTerm += 1
	RVCandidateId := rcm.Id
	RVCandidateTerm := rcm.CurrentTerm
	rcm.electionTimeout = time.Now() // reset our election timeout
	rcm.mu.Unlock()

	votesGranted := 1

	for _, nodeId := range rcm.NodesIds {
		go func(nodeId int) {
			// TODO : should we lock on the mutex ??
			rcm.mu.Lock()
			defer rcm.mu.Unlock()

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
	rcm.mu.Lock()
	rcm.NodeState = Leader
	log.Printf("leader of term [%v] is node [%v]\n", rcm.CurrentTerm, rcm.Id)
	heartbeatTickerDuration := rcm.heartbeatTimeout
	rcm.mu.Unlock()

	heartbeatTicker := time.NewTicker(heartbeatTickerDuration)

	for {
		// first heartbeat is sent immeiedtly, then we send it every 20 ms
		rcm.mu.Lock()
		AppendEntryCurrentTerm := rcm.CurrentTerm
		AppendEntryLeaderId := rcm.Id
		rcm.mu.Unlock()

		for _, nodeId := range rcm.NodesIds {
			go func(nodeId int) {
				AEReply := &AppendEntryReply{}
				AEArgs := &AppendEntryArgs{
					term:     AppendEntryCurrentTerm,
					leaderId: AppendEntryLeaderId,
				}
				if err := rcm.rpcNodes[nodeId].Call("RaftConsensusModule.AppendEntry_RPC", AEArgs, AEReply); err != nil {
					log.Printf("failed to heartbeat node [%v] at term [%v]\n", nodeId, AppendEntryCurrentTerm)
					return
				}

				rcm.mu.Lock()
				if AEReply.success {
					if AEReply.term > rcm.CurrentTerm {
						rcm.Follower(AEReply.term)
					}
				}
				rcm.mu.Unlock()
			}(nodeId)
		}

		<-heartbeatTicker.C
		// are we still the leader ?
		rcm.mu.Lock()
		if rcm.NodeState != Leader {
			// if we are not the leader, this for sure because other go routine called Follower() method, so no need to call it again
			rcm.mu.Unlock()
			return
		}
		rcm.mu.Unlock()
	}
}

func (rcm *RaftConsensusModule) Follower(term int) {
	rcm.NodeState = Follower
	rcm.VotedFor = -1
	rcm.electionTimeout = time.Now()
	rcm.CurrentTerm = term
	go rcm.BackgroundLeaderElectionTimer()
}

// TODO : this function should be part of the AppendEntry logic, but lets implement it seprately then see how i can merge the logic with the AppendEntry logic
/*
Send Heartbeat Logic Steps:
	- for each peer:
		- > 1. get the index before the next index of the log entry that should be sent to this peer (the leader knows the next index of each peer)
		- > 2. get the log cmd at this index of the log entry
		- > 3. get all the log cmds after this index (from the next-index of this leader to the end of the log)
		- > 4. call AppendEntry rpc for this pr
		- > 5. if the perr accepted the msg and the term received from this peer is same as our current term :
		- >> 5.1. we need to update our commit-index if we get a quorum of replicating each entry after the current commit index at the follower's log
*/
func (rcm *RaftConsensusModule) SendHeartbeat() {
	rcm.mu.Lock()
	leaderCurrentTerm := rcm.CurrentTerm
	rcm.mu.Unlock()

	// heartbeat all peers
	for _, peerId := range rcm.NodesIds {
		go func(peerId int) {
			rcm.mu.Lock()
			peerNextIndexInfo := rcm.nextIndex[peerId]

			if peerNextIndexInfo.NodeId != peerId {
				log.Printf("something is wrong with the cluster membership configurations, the nextIndex[%v] contains data for Node with id [%v]\n", peerId, peerNextIndexInfo.NodeId)
				return
			}

			LastReplicatedLogIndexForThisPeer := peerNextIndexInfo.NextLogIndexToSend - 1
			lastReplicatedLogTermForThisPeer := -1
			// if this peer already had cmds in it's log
			if LastReplicatedLogIndexForThisPeer >= 0 {
				lastReplicatedLogTermForThisPeer = rcm.Log[LastReplicatedLogIndexForThisPeer].Term
			}
			allEntrieFromLeaderLogsAfterTheLastCommittedIndexOnThisPeer := rcm.Log[peerNextIndexInfo.NextLogIndexToSend:]

			AEArgs := &AppendEntryArgs{
				term:              leaderCurrentTerm,
				leaderId:          rcm.Id,
				prevLogIndex:      LastReplicatedLogIndexForThisPeer,
				prevLogTerm:       lastReplicatedLogTermForThisPeer,
				entries:           allEntrieFromLeaderLogsAfterTheLastCommittedIndexOnThisPeer,
				leaderCommitIndex: rcm.LastComittedIndex,
			}
			AEReply := &AppendEntryReply{}
			rcm.mu.Unlock()

			if err := rcm.rpcNodes[peerId].Call("RaftConsensusModule.AppendEntry_RPC", AEArgs, AEReply); err != nil {
				log.Printf("failed to append entry / send heartbeat for node [%v], error [%v] \n", peerId, err.Error())
				return
			}
			rcm.mu.Lock()

			if !AEReply.success {
				log.Printf("node [%v] failed to ACK the heartbeat reqeust, so i will decremnt its nextIndex by one to try with the prev log entry\n", peerId)
				rcm.nextIndex[peerId].NextLogIndexToSend -= 1
				return
			}

			if AEReply.term > leaderCurrentTerm {
				log.Printf("my tearm [%v] is outdated and discovered a higher term from peer [%v] = [%v]\n", leaderCurrentTerm, peerId, AEReply.term)
				rcm.Follower(AEReply.term)
				return
			}

			if rcm.NodeState != Leader {
				log.Printf("not the leader anymore\n")
				return
			}

			// Leader has the nextIndex info about this peer = 2
			// Leader log indecies is [0, 1, 2, 3, 4]
			// Leader send PrevLogIndex to the req = Nextindex - 1 = 1
			// so the nextIndex of this peer for the next heartbeat will be = 5 because the leader sent to this peer who ACKs this heartbeat succesfully all the entries starting from index 2
			rcm.nextIndex[peerId] = &NodeNextLogIndexToSend{
				NodeId:             peerId,
				NextLogIndexToSend: rcm.nextIndex[peerId].NextLogIndexToSend + len(AEArgs.entries), // in our example this will be 5
			}
			// because this leader Acked all the sent entires, so the last replicated log entry index is the last index in the sent entries which will be in our example = 4  ... complicated
			rcm.matchIndex[peerId] = &NodeLastReplicatedLogIndex{
				NodeId:                 peerId,
				LastReplicatedLogIndex: rcm.nextIndex[peerId].NextLogIndexToSend - 1,
			}
			lastCommitedIndexSoFar := rcm.LastComittedIndex
			rcm.UpdateCommitIndex(lastCommitedIndexSoFar)

			rcm.mu.Unlock()
		}(peerId)
	}
}

func (rcm *RaftConsensusModule) UpdateCommitIndex(lastCommittedIndex int) {
	// check each log entry after the leader last committed entry to check if we got majority who replicated the entries the leader sent to them so the leader cna update its own commit index one step forward
	for currentEntryIdx := rcm.LastComittedIndex + 1; currentEntryIdx < len(rcm.Log); currentEntryIdx++ {
		// we only try to commit and get majority if this entry into our current term (raft safety)
		if rcm.Log[currentEntryIdx].Term != rcm.CurrentTerm {
			continue
		}

		// we committed this index (we are the leader) so just add one for this index commit to collect majority
		nodesCommittedThisLogEntry := 1
		// now check the peers
		for _, peerid := range rcm.NodesIds {
			// if this peer replicated this entry
			// remember we updated the matchIndex of this peer to the last entry into our log if the peer acks the heartbeat of all sent entries, so probably this will be true
			if rcm.matchIndex[peerid].LastReplicatedLogIndex >= currentEntryIdx {
				nodesCommittedThisLogEntry += 1
			}
		}

		// check if we reached the majority
		if rcm.IsQuorum(nodesCommittedThisLogEntry) {
			rcm.LastComittedIndex = currentEntryIdx
		}

		if rcm.LastComittedIndex != lastCommittedIndex {
			rcm.commitIndexUpdatedChan <- struct{}{}
		}
	}
}

// this runs on a separate go routine that runs by the time the node is up
// and this method listens on the <- commitedIndexUpdatedChan
//
//	[0, 1, 2, 3, 4, 5]
//
// a      *
// c            *
//
// entries = [2, 3]
// cmds_indexes := [{2 + 0 + 1}, {2 + 1 + 1}]
func (rcm *RaftConsensusModule) ApplyLogEntiresAfterCommitting() {
	for {
		select {
		case <-rcm.commitIndexUpdatedChan:
			rcm.mu.Lock()
			termWhenThisUpdateOccured := rcm.CurrentTerm
			lastAppliedLogSentToClient := rcm.LastAppliedLogIndex

			entriesToBeAppleidToClient := []LogEntry{}
			if lastAppliedLogSentToClient < rcm.LastComittedIndex {
				entriesToBeAppleidToClient = append(entriesToBeAppleidToClient, rcm.Log[lastAppliedLogSentToClient+1:rcm.LastComittedIndex+1]...)
				rcm.LastAppliedLogIndex = rcm.LastComittedIndex
			}
			rcm.mu.Unlock()

			for idx, entryToBeApplied := range entriesToBeAppleidToClient {
				rcm.commitChan <- LogEntry{
					Cmd:   entryToBeApplied.Cmd,
					Term:  termWhenThisUpdateOccured,            // not the term of the entry because we are applying it to the client at this current term not the term's original entry
					Index: idx + lastAppliedLogSentToClient + 1, // explained at function description
				}
			}
		}
	}
}
