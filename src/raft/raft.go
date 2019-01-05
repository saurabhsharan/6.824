package raft

// My notes:
// - election timeout thread:
// while (true) {
//     acquire mutex;
//     check if not gotten heartbeat from current or future leader or if granted vote in current election term
//     if have, then update got_heartbeat to false, release mutex and return
//     <- what if receive RPC here? what happens if we receive RPC between when we determine we want to run
//     election and we actually start the election?
//     if not, then start election
// }
// If you get AppendEntries heartbeat between when you decide and when you start, should you cancel election? or just let it run for simplicity?

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

// Strawman: hold lock for election leader timeout
// Problem: strawman results in deadlock, as no one can respond to RPCs since they are waiting for reply from others, who are waiting for RPC on someone who is waiting for RPCs...
// Idea: before making blocking RPC call, release lock, and in reply, get lock back and check that state is roughly the same

import (
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	FollowerState  = iota
	CandidateState = iota
	LeaderState    = iota
)

// How often leader should send heartbeats
const HeartbeatIntervalMs = 75

// How long to wait for majority vote before halting election
const ElectionWinTimeoutMs = 100

// Minimum and maximum range to randomly choose election timeouts from
const ElectionTimeoutMinMs = 500
const ElectionTimeoutMaxMs = 800

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	currentTerm        int                // Latest term seen
	currentLeader      int                // Index of leader for current term, or -1 if none elected yet
	votedFor           int                // CandidateId that received vote in current term, or null if none
	commitIndex        int                // Highest log entry known to be committed
	lastApplied        int                // Highest log entry applied to current state machine
	state              int                // Current state: follower, candidate, or leader
	lastHeartbeatNano  int64              // Last timestamp received from a valid leader, in nanoseconds
	electionTimeoutMs  int                // Election timeout, in ms
	heartbeatReplyChan chan HeartbeatInfo // In shared struct so can send heartbeats immediately after becoming leader without waiting for next interval
}

// Stores info about results of RequestVote invocation
type RequestVoteContext struct {
	ok     bool              // Whether RPC returned ok
	reply  *RequestVoteReply // reply data
	term   int               // Term RPC was sent in
	peerId int               // Peer RPC was sent to
}

type HeartbeatInfo struct {
	ok     bool                // Whether RPC returned ok
	reply  *AppendEntriesReply // reply data
	term   int                 // Term RPC was sent in
	peerId int                 // Peer RPC was sent to
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == LeaderState
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true iff candidate received vote
}

type AppendEntriesArgs struct {
	Term     int // sending leader's term
	LeaderId int // sending leader's ID
}

type AppendEntriesReply struct {
	Term int // later term for leader to update itself
}

func (rf *Raft) HeartbeatTimeout() {
	// QUESTION: should we wait here for reply/timeout from all heartbeat requests? or, should we also move on to next heartbeat timeout? => yes, because the followers that did reply will trigger election timeout if another follower hasn't replied

	heartbeatTimeChan := make(chan int)
	go func(heartbeatTimeChan chan int) {
		for {
			time.Sleep(time.Duration(HeartbeatIntervalMs) * time.Millisecond)
			heartbeatTimeChan <- 1
		}
	}(heartbeatTimeChan)

	// Wait for both heartbeat intervals and heartbeat replies in the same wait loop
	// This allows us to send heartbeats at the next interval, even if we haven't gotten back all replies
	//  from previous interval of sending heartbeats
	for {
		select {
		case <-heartbeatTimeChan:
			rf.mu.Lock()

			// Not leader, so don't do anything
			if rf.state != LeaderState {
				break
			}

			// Send AppendEntries heartbeat to every peer
			for i, _ := range rf.peers {
				if i == rf.me {
					continue
				}

				// DPrintf("[%d] sending heartbeat to %d", rf.me, i)
				go func(currentTerm int, peer *labrpc.ClientEnd, peerId int, me int, heartbeatChan chan HeartbeatInfo) {
					args := AppendEntriesArgs{currentTerm, me}
					var reply AppendEntriesReply
					ok := peer.Call("Raft.AppendEntries", &args, &reply)
					heartbeatInfo := HeartbeatInfo{ok, &reply, currentTerm, peerId}
					heartbeatChan <- heartbeatInfo
				}(rf.currentTerm, rf.peers[i], i, rf.me, rf.heartbeatReplyChan)
			}
		case heartbeatInfo := <-rf.heartbeatReplyChan:
			rf.mu.Lock()

			// We are in old epoch, so update epoch and revert to follower
			if heartbeatInfo.reply.Term > rf.currentTerm {
				rf.currentTerm = heartbeatInfo.reply.Term
				rf.currentLeader = -1
				rf.votedFor = -1
				rf.state = FollowerState
			}
		}

		// Release lock before waiting on anything
		rf.mu.Unlock()
	}
}

func (rf *Raft) ElectionTimeout(electionTimeoutMs int) {
	for {
		time.Sleep(time.Duration(electionTimeoutMs/2) * time.Millisecond)

		rf.mu.Lock()

		// If we are leader, nothing to do (sending heartbeat is handled by different goroutine)
		if rf.state == LeaderState {
			rf.mu.Unlock()
			continue
		}

		// If haven't reached election timeout, go back to sleeping
		nanosDiff := time.Now().UnixNano() - rf.lastHeartbeatNano
		if (nanosDiff / 1000000) < int64(electionTimeoutMs) {
			rf.mu.Unlock()
			continue
		}

		// Increment epoch and vote for ourselves
		rf.currentTerm = rf.currentTerm + 1
		rf.state = CandidateState
		rf.votedFor = rf.me

		DPrintf("[%d] hit election timeout, starting election, new term is %d", rf.me, rf.currentTerm)

		// Make channel that goroutines can write their output to
		votesInfoChan := make(chan RequestVoteContext)

		// For every peer, spawn a goroutine that sends a vote request to that peer and writes output to channel
		for i, _ := range rf.peers {
			// Don't send RequestVote to ourselves
			if i == rf.me {
				continue
			}

			go func(currentTerm int, peer *labrpc.ClientEnd, peerId int, me int, lastApplied int, votesInfoChan chan RequestVoteContext) {
				args := RequestVoteArgs{currentTerm, me, lastApplied, currentTerm}
				var reply RequestVoteReply
				ok := peer.Call("Raft.RequestVote", &args, &reply)
				voteContext := RequestVoteContext{ok, &reply, currentTerm, peerId}
				votesInfoChan <- voteContext
			}(rf.currentTerm, rf.peers[i], i, rf.me, rf.lastApplied, votesInfoChan)
		}

		// Create channel and goroutine to stop election if we haven't won within 100 ms
		winTimeoutChan := make(chan int)
		go func() {
			time.Sleep(time.Duration(ElectionWinTimeoutMs) * time.Millisecond)
			winTimeoutChan <- 1
		}()

		votesNeeded := (len(rf.peers) / 2) + 1
		votesReceived := 1
		votesDeclined := 0

		// Release lock after spwaning all RPC goroutines
		rf.mu.Unlock()

		// Flag variables that inner code blocks within select{} may set to indicate that election is over (either won or lost)
		wonElection := false
		lostElection := false

		// Meanwhile, main goroutine will wait on that channel, and on another goroutine/channel to indicate election timeout, and a third channel to indicate that another leader won?
		//   For 3rd option, can actually happen automatically if either of the first 2 happen?
		for {
			select {
			case voteInfo := <-votesInfoChan:
				// Check for any state changes between RPC request and response
				rf.mu.Lock()

				// If no longer in candidate state, then halt election
				// This assumes that any epoch changes would have also resulted in moving away from candidate state. Maybe also include explicit epoch check as well?
				if rf.state != CandidateState {
					// DPrintf("[%d] got RequestVote reply from %d but no longer candidate, so halting election", rf.me, voteInfo.peerId)
					lostElection = true
					break
				}

				// Peer has higher epoch, so halt election process and move back to follower
				if voteInfo.reply.Term > rf.currentTerm {
					DPrintf("[%d] got vote reply from %d but it has higher epoch, so halting election", rf.me, voteInfo.peerId)
					rf.currentTerm = voteInfo.reply.Term
					rf.state = FollowerState
					rf.votedFor = -1
					lostElection = true
					break
				}

				if !voteInfo.ok {
					// RPC failed, so consider it vote declined
					votesDeclined = votesDeclined + 1
				} else if voteInfo.reply.VoteGranted {
					// Got positive vote
					votesReceived = votesReceived + 1
				} else {
					// Got negative vote
					votesDeclined = votesDeclined + 1
				}

				if votesDeclined >= votesNeeded {
					// DPrintf("[%d] got too many vote declines, so halting election", rf.me)
					lostElection = true
				} else if votesReceived >= votesNeeded {
					wonElection = true
				}
			case <-winTimeoutChan:
				rf.mu.Lock()
				lostElection = true
			}

			if wonElection || lostElection {
				break
			}

			rf.mu.Unlock()
		}

		if wonElection && lostElection {
			DPrintf("INTERNAL ERROR: both wonElection and lostElection are true")
		}

		// Become the leader
		if wonElection {
			rf.state = LeaderState
			DPrintf("[%d] received %d votes, becoming the leader", rf.me, votesReceived)

			// Send heartbeat to every peer immediately after becoming leader
			for i, _ := range rf.peers {
				if i == rf.me {
					continue
				}

				DPrintf("new leader [%d] sending heartbeat to %d", rf.me, i)
				go func(currentTerm int, peer *labrpc.ClientEnd, peerId int, me int, heartbeatChan chan HeartbeatInfo) {
					args := AppendEntriesArgs{currentTerm, me}
					var reply AppendEntriesReply
					ok := peer.Call("Raft.AppendEntries", &args, &reply)
					heartbeatInfo := HeartbeatInfo{ok, &reply, currentTerm, peerId}
					heartbeatChan <- heartbeatInfo
				}(rf.currentTerm, rf.peers[i], i, rf.me, rf.heartbeatReplyChan)
			}
		}

		// Didn't get enough votes, so go back to follower
		if lostElection {
			rf.state = FollowerState
			DPrintf("[%d] didn't get enough votes, going back to follower", rf.me)
		}

		rf.mu.Unlock()
	}
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		// Reject requests from earlier epoch (in particular, requests from old leaders)
		reply.Term = rf.currentTerm
		return
	} else if args.Term == rf.currentTerm {
		// If during current epoch and we are candidate, then we are in leader election, so convert to follower
		if rf.currentLeader == -1 {
			DPrintf("[%d] now knows that leader is %d in term %d", rf.me, args.LeaderId, rf.currentTerm)
			rf.state = FollowerState
			rf.currentLeader = args.LeaderId
		} else if rf.currentLeader != args.LeaderId {
			DPrintf("ERROR: Received AppendEntries in epoch %d with leader %d, but thought leader was %d", rf.currentTerm, args.LeaderId, rf.currentLeader)
		}
	} else if args.Term > rf.currentTerm {
		DPrintf("[%d] got heartbeat from newer term %d and leader %d, moving to follower", rf.me, args.Term, args.LeaderId)
		// If from later epoch, update our epoch and move to follower state
		rf.currentTerm = args.Term
		rf.currentLeader = args.LeaderId
		rf.votedFor = -1
		rf.state = FollowerState
	}

	// DPrintf("[%d] got heartbeat from leader %d in term %d", rf.me, rf.currentLeader, rf.currentTerm)
	rf.lastHeartbeatNano = time.Now().UnixNano()
}

// RequestVote RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reject requests from earlier epoch
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// Save current term, which is compared to args.LastLogTerm (since we don't store that in rf state)
	ourLastLogTerm := rf.currentTerm

	// If from later epoch, update our epoch and move to follower state
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.currentLeader = -1
		rf.state = FollowerState
	}

	grantVote := false
	// Check if we haven't voted for anyone or already for this candidate, and if there isn't already a leader we know about
	if rf.currentLeader == -1 && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		// Check that candidate's log is at least as up-to-date as receiver's log, grant vote
		if args.LastLogTerm > ourLastLogTerm {
			grantVote = true
		}
		if args.LastLogTerm == ourLastLogTerm && args.LastLogIndex >= rf.commitIndex {
			grantVote = true
		}
	}

	if grantVote {
		DPrintf("[%d] voted for %d in term %d", rf.me, args.CandidateId, rf.currentTerm)
		rf.votedFor = args.CandidateId
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = grantVote
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.currentLeader = -1
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.state = FollowerState
	rf.lastHeartbeatNano = time.Now().UnixNano()
	// can disable rand.seed() for more determinism
	rand.Seed(time.Now().UnixNano())
	rf.electionTimeoutMs = rand.Intn(ElectionTimeoutMaxMs-ElectionTimeoutMinMs) + ElectionTimeoutMinMs
	DPrintf("[%d] has election timeout %d", rf.me, rf.electionTimeoutMs)
	rf.heartbeatReplyChan = make(chan HeartbeatInfo)
	go rf.ElectionTimeout(rf.electionTimeoutMs)
	go rf.HeartbeatTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
