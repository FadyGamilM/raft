package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

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

type RaftNode struct {
	NodeId          int
	ClusterNodesIds map[int]string

	state NodeState

	minElectionTimeout int64
	maxElectionTimeout int64
	electionTimeout    int64
	heartbeatTimeout   int64

	currentTerm int64

	mu *sync.Mutex

	votedForNodeId *int

	// channels for communications between background threads
	startElectionChan       chan struct{}
	receivedAppendEntryChan chan struct{} // this channel will notify the ElectionWorker thread either we got new entry or just a normal heartbeat
	receivedRequestVoteChan chan struct{}

	logs []LogEntry

	// for each peerId, what is the next-index on this peer that we are sending this entry for
	//                           0  1  2  3
	// so if the follower.log = [9, 4, 6,  ] , the nextIndex of this peer is 3
	// TODO ----> IMPORTANT HINT : once the nextIndex of this peer is > len(leader.logs) we are sending the AppendEntry rpc as a heartbeat not appendEntry
	nextIndex map[int]int64

	// for each peerId, what is the index in this peer log that we as a leader completely sure that we and this peer are agreed on this logEntry and this is durable
	matchIndex map[int]int64

	commitIndex int64
}

func NewRaftNode(Id int, clusterNodesIds map[int]string) *RaftNode {
	server := &RaftNode{
		NodeId:                  Id,
		ClusterNodesIds:         clusterNodesIds,
		mu:                      &sync.Mutex{},
		startElectionChan:       make(chan struct{}),
		receivedAppendEntryChan: make(chan struct{}),
		receivedRequestVoteChan: make(chan struct{}),
		state:                   Follower,
		currentTerm:             0,
		commitIndex:             -1,
		nextIndex:               make(map[int]int64),
		matchIndex:              make(map[int]int64),
		logs:                    make([]LogEntry, 0),
	}

	server.minElectionTimeout = 150
	server.maxElectionTimeout = 300
	server.heartbeatTimeout = 50

	server.ResetElectionTimeout()

	// Start background workers
	go server.RaftBackgroundWorker()
	go server.CommitIndexWorker()

	return server
}

func (r *RaftNode) ResetElectionTimeout() {
	r.mu.Lock()
	defer r.mu.Unlock()
	// return a random timeout = 150 + ( random-number-from [0:150[ ])
	r.electionTimeout = r.minElectionTimeout + int64(rand.Int63n(r.maxElectionTimeout-r.minElectionTimeout))
}

func (r *RaftNode) RaftBackgroundWorker() {
	r.mu.Lock()
	electionTimeout := r.electionTimeout
	r.mu.Unlock()

	electionTimeoutExpiryTimer := time.NewTimer(time.Duration(electionTimeout * int64(time.Millisecond)))

	for {
		select {
		case <-electionTimeoutExpiryTimer.C:
			// should it be a go routine ?
			go r.StartElection()

		case <-r.receivedAppendEntryChan:
			go r.HandleAppendEntry()

		case <-r.receivedRequestVoteChan:
			go r.HandleRequestVote()
		}
	}

}

/*
When this function is used ?

  - When the follower didn't receive a heartbeat (AppendEntries) rpc from the leader or a (RequestVote) from any candidate and the electionTimeout timer is expired

Steps of Execution :

  - Lock on node's state

ResetElectionTimeout

  - Increase the current term

  - Vote for yourself

  - Send RequestVote RPC to all nodes
*/
func (r *RaftNode) StartElection() {
	r.mu.Lock()
	if r.state == Leader {
		// we cannot start another election if we are the leader of this term
		r.mu.Unlock()
		return
	}

	r.state = Candidate
	r.currentTerm += 1
	r.votedForNodeId = &r.NodeId
	calusterNodesNum := len(r.ClusterNodesIds)
	termOfthisElection := r.currentTerm
	RVArgs := &RequestVoteArgs{
		CandidateId:  r.NodeId,
		Term:         r.currentTerm,
		LastLogIndex: -1, // inital value
		LastLogTerm:  -1, // initial value
	}

	// if we already have log entries, we can sned our last log index and term
	if len(r.logs) > 0 {
		RVArgs.LastLogIndex = r.logs[len(r.logs)-1].index
		RVArgs.LastLogTerm = r.logs[len(r.logs)-1].term
	}

	r.mu.Unlock()

	// I will use separate mutex for acessing the grantedVotes thread safely because for the ToLeader() ToFollower() ToCandidate() methods and other methods too will need to acuqire lock on the node mutex, so i don't want to stop other logic and cause a deadlock while i am locking on the grantedVote to do some checking
	voteMutex := sync.Mutex{}
	grantedVotes := 1 // voted for myself

	wg := sync.WaitGroup{}

	for peerId, peerAddress := range r.ClusterNodesIds {
		r.mu.Lock()
		if peerId == r.NodeId {
			continue
		}
		r.mu.Unlock()

		wg.Add(1)
		// because one of the go routines might cause state changing to follower or leader .. we must have a condition within every go routine to check if the state is still candidate to send the request
		go func(peerId int, peerAddress string) {
			// defer the ending of the wait group to avoid go routines leaking and the main routine (StartElection go routine will never ends and will keep waiting this leaked go routine)
			defer wg.Done()

			// TODO : should we acquire lock on the mutex or what ??
			r.mu.Lock()
			currentState := r.state
			r.mu.Unlock()

			if currentState != Candidate {
				log.Printf("RV_for_term_[%v]: current_State_changed_to_[%v]\n", termOfthisElection, currentState)
				return
			}

			RVReply, err := r.SendRequestVote(peerId, peerAddress, RVArgs)
			if err != nil {
				log.Printf("RV_for_term_[%v]: error_[%v]_sending_request_vote_to_peer_[%v]\n", termOfthisElection, err.Error(), peerId)
				return
			}

			log.Printf("RV_for_term_[%v]: RV_reply_from_peer_[%v]_is_[%v]\n", termOfthisElection, peerId, RVReply)

			if RVReply.Term > int(termOfthisElection) {
				log.Printf("RV_for_term_[%v]: found_higher_term_than_RV_term_from_RV_reply_from_node_[%v]\n", termOfthisElection, peerId)

				r.ToFollower(int64(RVReply.Term))
				return
			}

			if RVReply.VoteGranted {
				voteMutex.Lock()
				grantedVotes++
				if grantedVotes >= (calusterNodesNum/2)+1 {
					log.Printf("RV_for_term_[%v]: got_quorum_granted_votes_changing_to_leader\n", termOfthisElection)

					r.ToLeader()
					voteMutex.Unlock()
					return
				}
				voteMutex.Unlock()
			}

		}(peerId, peerAddress)
	}

	wg.Wait()

}

// Ensure Raft Consistency by only voting for candidate with most up to date log
func (r *RaftNode) isCandidateHasTheUpToDateLog(candidateLastLogIndex, candidateLastLogTerm int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.logs) > 0 {
		// if candidate log and follower log end with the same term, the longer log is the more up to date log
		if r.logs[len(r.logs)-1].term == int(candidateLastLogTerm) {
			return candidateLastLogIndex >= r.logs[len(r.logs)-1].index // ack this candidate as up-to-date log if only his log length is higher or equales ours if we both have the same lastLogTerm
		}

		// if not the same term, the higher term log is the up to date one
		return candidateLastLogTerm >= r.logs[len(r.logs)-1].term
	}

	// if my log is empty (length is zero), so if the candidate log have at least one entry, its up to date for me
	return candidateLastLogIndex > 0
}

func (r *RaftNode) HandleAppendEntry() {}

func (r *RaftNode) HandleRequestVote() {}

func (r *RaftNode) ToFollower(term int64) {
	r.mu.Lock()
	r.state = Follower
	r.currentTerm = term
	r.votedForNodeId = nil
	r.ResetElectionTimeout()
	r.mu.Unlock()
}

func (r *RaftNode) ToCandidate() {
	// first step i think is to reset our electionTimeout
	// change the state to candidate
}

func (r *RaftNode) ToLeader() {
	// first step i think is to reset our electionTimeout
	// change the state to leader
	r.mu.Lock()
	r.state = Leader
	r.mu.Unlock()

	go r.SendHeartbeatToPeers()
}

func (r *RaftNode) SendHeartbeatToPeers() {
	heartbeatTimer := time.NewTimer(time.Duration(r.heartbeatTimeout * int64(time.Millisecond))) // every 50 ms
	heartbeatTimer.Stop()

	for {
		select {
		case <-heartbeatTimer.C:
			r.mu.Lock()
			if r.state != Leader {
				log.Println("not_leader_to_send_heartbeat_request")
				r.mu.Unlock()
				return
			}
			r.mu.Unlock()

			for peerId, peerAddress := range r.ClusterNodesIds {
				r.mu.Lock()
				if peerId == r.NodeId {
					continue
				}

				//  we should replicate the logs if we have something to send to this peer
				nextIdx := r.nextIndex[peerId]
				prevLogIndex := nextIdx - 1
				prevLogTerm := int64(-1)

				// Find the term for prevLogIndex
				if prevLogIndex >= 0 && int(prevLogIndex) < len(r.logs) {
					prevLogTerm = int64(r.logs[prevLogIndex].term)
				}

				// Prepare entries to send
				var entries []LogEntry
				if nextIdx < int64(len(r.logs)) {
					entries = r.logs[nextIdx:]
				}

				AEArgs := &AppendEntryArgs{
					term:              int(r.currentTerm),
					leaderId:          r.NodeId,
					prevLogIndex:      -1,
					prevLogTerm:       int(prevLogTerm),
					entries:           entries,
					leaderCommitIndex: int(r.commitIndex),
				}
				r.mu.Unlock()

				go func(peerId int, peerAddress string) {
					AEReply, err := r.SendAppendEntry(peerId, peerAddress, AEArgs)
					if err != nil {
						log.Printf("AE_for_term_[%v]: error_[%v]_sending_heartbeat_to_peer_[%v]\n", AEArgs.term, err.Error(), peerId)
						return
					}

					r.mu.Lock()
					if AEReply.term > int(r.currentTerm) {
						log.Printf("AE_for_term_[%v]: found_higher_term_[%v]_from_peer_[%v]_while_my_current_term_is_[%v]_switching_to_follower_now\n", AEArgs.term, AEReply.term, peerId, r.currentTerm)
						r.ToFollower(int64(AEReply.term))
						return
					}
					r.mu.Unlock()

					// if we replicated the log successfully, we should update the nextIndex of this peer
					if AEReply.success {
						log.Printf("AE_for_term_[%v]: peer_[%v]_replied_with_success\n", AEArgs.term, peerId)

						// i set the len(AEArgs.entries) > 0 condition to check if we actually wanted to replicate our logs
						if len(AEArgs.entries) > 0 {
							// if leader log is [ [1:t1], [2:t2], [4:t2], [3:t3] ]
							// if follower log is [ [1:t1], [2:t2] ]
							// so nextIndex of this folllower was 2 before sending the heartbeat
							// so lastLogIndex of this follower before sending the request was 1
							// so entires sent to be replicated are [ [4:t2], [3:t3] ]
							// so after the request nextIndex should be = 4 because follower log is now [ [1:t1], [2:t2], [4:t2], [3:t3] ]
							// and the matchIndex should be = 4-1 = 3
							r.mu.Lock()
							r.nextIndex[peerId] = int64(AEArgs.prevLogIndex) + int64(len(AEArgs.entries)) + 1
							r.matchIndex[peerId] = r.nextIndex[peerId] - 1
							r.mu.Unlock()
						}

						return
					} else {
						// we just failed to replicate the new entries starting from the next indexx, but the matchIndex is already agreed to be repicated so no need to update it
						r.mu.Lock()
						r.nextIndex[peerId]--
						log.Printf("AE_for_term_[%v]: peer_[%v]_replied_with_failure_so_decrementing_the_nextIndex_to_[%v]\n", AEArgs.term, peerId, r.nextIndex[peerId])
						r.mu.Unlock()

					}
				}(peerId, peerAddress)
			}

			// reset our heartbeat timer
			r.mu.Lock()
			heartbeatTimer.Reset(time.Duration(r.heartbeatTimeout * int64(time.Millisecond)))
			r.mu.Unlock()
		}
	}
}

func (r *RaftNode) AppendNewEntry(command string) bool {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return false
	}

	newEntry := LogEntry{
		cmd:   command,
		term:  int(r.currentTerm),
		index: len(r.logs),
	}

	r.logs = append(r.logs, newEntry)

	log.Printf("leader_[%v]: appended_new_entry_[%v]_at_index_[%v]\n", r.NodeId, command, newEntry.index)

	r.mu.Unlock()

	return true
}

// RULE : we only commit entry logs of our current term not a prev term that we weren't it's leader
func (r *RaftNode) CommitIndexWorker() {
	for {
		time.Sleep(10 * time.Millisecond)

		r.mu.Lock()
		if r.state != Leader {
			r.mu.Unlock()
			continue
		}

		// Get current log length
		var lastLogIndex int64
		if len(r.logs) > 0 {
			lastLogIndex = int64(len(r.logs) - 1)
		} else {
			r.mu.Unlock()
			continue
		}

		// count replication status for each entry log after the current commit index
		// why +1 ? because the initial state of the commit index is -1 so if we didn't commit anything yet we will start with the first entry at index 0 and if not = -1, so we always want to try to get majority of aggrement after the current committed index
		for indexOfNewEntryToBeCommitted := int64(r.commitIndex + 1); indexOfNewEntryToBeCommitted <= lastLogIndex; indexOfNewEntryToBeCommitted++ {
			replicationCount := 1 // count ourselves as a node aggreed on this commit index

			// check how many followers have replicated this entry
			for peerId := range r.ClusterNodesIds {
				if peerId != r.NodeId {
					// if we know a match index of this peer = or more than the index of the entry we are trying to get a majority for it, so we count this node as an aggreed one for this entry
					if r.matchIndex[peerId] >= int64(indexOfNewEntryToBeCommitted) {
						replicationCount++
					}
				}
			}

			// If majority have replicated and entry is from current term
			if replicationCount >= (len(r.ClusterNodesIds)/2)+1 && r.logs[indexOfNewEntryToBeCommitted].term == int(r.currentTerm) {
				oldCommitIndex := r.commitIndex
				r.commitIndex = int64(indexOfNewEntryToBeCommitted)
				log.Printf("Leader %d: Advanced commit index from %d to %d",
					r.NodeId, oldCommitIndex, r.commitIndex)
			}
		}
		r.mu.Unlock()
	}
}
