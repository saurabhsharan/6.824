package raft

import (
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"math/rand"
	"os"
	"sync"
	"time"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
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

// How often leader should send heartbeats, must be no more than 10/sec
const HeartbeatIntervalMs = 100

// How long candidate should wait for majority vote before halting election
const ElectionWinTimeoutMs = 200

// Minimum and maximum range to randomly choose election timeouts from
const ElectionTimeoutMinMs = 500
const ElectionTimeoutMaxMs = 1200

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	applyChan chan ApplyMsg       // channel to send committed messages to

	state int // Current state, one of {FollowerState, CandidateState, LeaderState}

	// Election-related state
	currentTerm       int   // Latest term seen
	currentLeader     int   // Index of leader for current term, or -1 if none elected yet
	votedFor          int   // CandidateId that received vote in current term, or -1 if none voted for yet
	electionTimeoutMs int   // Election timeout, in ms
	lastHeartbeatNano int64 // Timestamp of last heartbeat received from a valid leader, in unix time nanoseconds

	// Log state
	log         []LogEntry // Log
	commitIndex int        // Highest log entry known to be committed
	lastApplied int        // Highest log entry applied to current state machine

	// Leader state
	nextIndex  []int // Next index to insert new entries on each follower's log. This is decremented if AppendEntries consistency check fails, and increased as AppendEntries calls succeed. Initialize to the last log index + 1 (i.e. start by assuming that all followers logs are consistent).
	matchIndex []int // Highest matching log index for each follower. This is monotonically increasing, and only changes (increases) when AppendEntries from leader to follower succeed. Initialized to - 1 (i.e. assume ).
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// Stores info about results of RequestVote invocation
type RequestVoteContext struct {
	ok     bool              // Whether RPC returned ok
	reply  *RequestVoteReply // reply data
	term   int               // Term RPC was sent in
	peerId int               // Peer RPC was sent to
}

type AppendEntriesContext struct {
	ok     bool                // Whether RPC returned ok
	args   *AppendEntriesArgs  // args/request data
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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func Min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		return
	} else {
		// IPrintf("[%d] being restarted to term %d", rf.me, currentTerm)
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// RPC field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true iff candidate received vote
}

type AppendEntriesArgs struct {
	Term              int        // sending leader's term
	LeaderId          int        // sending leader's ID
	PrevLogIndex      int        // index of log entry preceding new one
	PrevLogTerm       int        // term of log entry preceding new one
	Entries           []LogEntry // new log entries
	LeaderCommitIndex int        // leader's index of latest commit entry
}

type AppendEntriesReply struct {
	Term               int  // later term for leader to update itself
	Success            bool // true if follower had matching previous log entry
	SuggestedPrevIndex int
}

// Precondition: rf.mu is locked
func (rf *Raft) becomeLeader() {
	rf.state = LeaderState
	rf.currentLeader = rf.me

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = -1
	}

	// Send heartbeat to every peer immediately after becoming leader
	// rf.sendMissingEntries()
	go rf.RunHeartbeat(rf.currentTerm)
}

// Precondition: rf.mu is locked
func (rf *Raft) becomeFollower(newTerm int) {
	if rf.state == LeaderState {
		IPrintf("[%d] moving from leader to follower in term %d", rf.me, newTerm)
	}
	rf.state = FollowerState
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.currentLeader = -1
	// go rf.ElectionTimeout(rf.electionTimeoutMs)
}

// Precondition: rf.mu is locked
func (rf *Raft) shouldStartElection(electionTimeoutMs int) bool {
	nanosDiff := time.Now().UnixNano() - rf.lastHeartbeatNano
	return (nanosDiff / 1000000) >= int64(electionTimeoutMs)
}

// Precondition: rf.mu is locked
func (rf *Raft) sendAllVotes(votesInfoChan chan RequestVoteContext) {
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		lastLogIndex := len(rf.log) - 1
		lastLogTerm := -1
		if lastLogIndex >= 0 {
			lastLogTerm = rf.log[lastLogIndex].Term
		}

		go func(currentTerm int, peer *labrpc.ClientEnd, peerId int, me int, lastLogIndex int, lastLogTerm int, votesInfoChan chan RequestVoteContext) {
			args := RequestVoteArgs{currentTerm, me, lastLogIndex, lastLogTerm}
			var reply RequestVoteReply
			ok := peer.Call("Raft.RequestVote", &args, &reply)
			voteContext := RequestVoteContext{ok, &reply, currentTerm, peerId}
			votesInfoChan <- voteContext
		}(rf.currentTerm, rf.peers[i], i, rf.me, lastLogIndex, lastLogTerm, votesInfoChan)
	}
}

// Blocks, so should be run as goroutine
func (rf *Raft) RunHeartbeat(currentTerm int) {
	for {
		time.Sleep(time.Duration(HeartbeatIntervalMs) * time.Millisecond)

		rf.mu.Lock()

		if rf.currentTerm != currentTerm {
			rf.mu.Unlock()
			return
		}

		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.SendAppendEntriesToPeer(i, rf.currentTerm)
		}

		rf.mu.Unlock()
	}
}

// Precondition: rf.mu is locked
func (rf *Raft) logDebugString() string {
	if len(rf.log) < 2 {
		return fmt.Sprintf("%v", rf.log)
	}
	entry1 := rf.log[0]
	entry2 := rf.log[len(rf.log)-1]
	return fmt.Sprintf("%v..%v", entry1, entry2)
}

// Precondition: rf.mu is locked
func (rf *Raft) commitRemaining() {
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied += 1
		// DPrintf("[%d] committing entry %d", rf.me, rf.lastApplied)
		msg := ApplyMsg{true, rf.log[rf.lastApplied].Command, rf.lastApplied + 1}
		rf.applyChan <- msg
	}
}

func (rf *Raft) ElectionTimeout(electionTimeoutMs int) {
	for {
		time.Sleep(time.Duration(electionTimeoutMs) * time.Millisecond)

		rf.mu.Lock()

		if rf.state == LeaderState {
			rf.mu.Unlock()
			continue
		}

		if !rf.shouldStartElection(electionTimeoutMs) {
			rf.mu.Unlock()
			continue
		}

		rf.currentTerm = rf.currentTerm + 1
		electionStartTerm := rf.currentTerm

		IPrintf("[%d] hit election timeout, starting election, new term is %d", rf.me, rf.currentTerm)

		rf.state = CandidateState
		rf.votedFor = rf.me

		votesInfoChan := make(chan RequestVoteContext)
		rf.sendAllVotes(votesInfoChan)

		// Create channel and goroutine to stop election if we haven't won within 100 ms
		winTimeoutChan := make(chan int)
		go func() {
			time.Sleep(time.Duration(ElectionWinTimeoutMs) * time.Millisecond)
			winTimeoutChan <- 1
		}()

		votesNeeded := (len(rf.peers) / 2) + 1
		votesReceived := 1
		votesDeclined := 0
		// responsesRemaining := len(rf.peers) - 1
		// defer func() {
		//     for i := 0; i < responsesRemaining; i++ {
		//         <-votesInfoChan
		//     }
		// }()

		// Flag variables that inner code blocks within select{} may set to indicate that election is over (either won or lost)
		wonElection := false
		lostElection := false

		for {
			rf.mu.Unlock()
			select {
			case voteInfo := <-votesInfoChan:
				rf.mu.Lock()

				// responsesRemaining -= 1

				if rf.state != CandidateState || rf.currentTerm != electionStartTerm {
					// DPrintf("[%d] got RequestVote reply from %d but no longer candidate, so halting election", rf.me, voteInfo.peerId)
					lostElection = true
					break
				}

				// Peer has higher epoch, so halt election process and move back to follower
				if voteInfo.reply.Term > rf.currentTerm {
					DPrintf("[%d] got vote reply from %d but it has higher epoch, so halting election", rf.me, voteInfo.peerId)
					rf.becomeFollower(voteInfo.reply.Term)
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
		}

		if wonElection && lostElection {
			EPrintf("FATAL ERROR: both wonElection and lostElection are true")
			os.Exit(-1)
		}

		if wonElection {
			IPrintf("[%d, t %d] received %d votes, becoming the leader, log = %s", rf.me, rf.currentTerm, votesReceived, rf.logDebugString())
			rf.becomeLeader()
		}

		if lostElection {
			rf.state = FollowerState
			rf.lastHeartbeatNano = time.Now().UnixNano()
			DPrintf("[%d, t %d] didn't get enough votes, going back to follower", rf.me, rf.currentTerm)
		}

		rf.mu.Unlock()
	}
}

// Blocks, so should be called as goroutine
func (rf *Raft) SendAppendEntriesToPeer(peer int, currentTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm != currentTerm {
		return
	}

	if rf.nextIndex[peer] >= (len(rf.log)+1) || rf.nextIndex[peer] < 0 {
		IPrintf("[%d t %d] LOOK HERE have out-of-bounds nextIndex = %d for peer %d, our log size = %d", rf.me, rf.currentTerm, rf.nextIndex[peer], peer, len(rf.log))
		IPrintf("[%d t %d] rf.nextIndex = %v", rf.me, rf.currentTerm, rf.nextIndex)
	}
	entriesToSend := rf.log[(rf.nextIndex[peer]):]
	prevLogIndex := rf.nextIndex[peer] - 1
	prevLogTerm := -1
	if prevLogIndex >= 0 {
		prevLogTerm = rf.log[prevLogIndex].Term
	}
	args := AppendEntriesArgs{rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, entriesToSend, rf.commitIndex}
	var reply AppendEntriesReply

	rf.mu.Unlock()
	ok := rf.peers[peer].Call("Raft.AppendEntries", &args, &reply)
	rf.mu.Lock()

	// Make sure we are still in the same epoch after RPC call finished
	if rf.currentTerm != currentTerm {
		return
	}

	if !ok {
		return
	}

	// If reply is from future epoch, move forward our epoch and transition to follower
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}

	// If response is for request from previous epoch, ignore
	// if reply.Term < rf.currentTerm {
	//     break
	// }

	// Response is rejection since request was stale and got rejected
	// if appendEntriesContext.term < response.Term {
	//     break
	// }

	if reply.Success {
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		if newMatchIndex > rf.matchIndex[peer] {
			rf.matchIndex[peer] = newMatchIndex
			rf.nextIndex[peer] = newMatchIndex + 1
			DPrintf("[%d t %d] A rf.nextIndex[%d] is now %d", rf.me, rf.currentTerm, peer, rf.nextIndex[peer])
		}
	} else {
		// if args.PrevLogIndex < rf.matchIndex[peer] {
		if reply.SuggestedPrevIndex != -1 && reply.SuggestedPrevIndex < rf.nextIndex[peer]-1 {
			rf.nextIndex[peer] = reply.SuggestedPrevIndex + 1
			DPrintf("[%d t %d] B rf.nextIndex[%d] is now %d", rf.me, rf.currentTerm, peer, rf.nextIndex[peer])
		} else if rf.nextIndex[peer] > 0 {
			rf.nextIndex[peer] -= 1
			DPrintf("[%d t %d] C rf.nextIndex[%d] is now %d", rf.me, rf.currentTerm, peer, rf.nextIndex[peer])
		}
		go rf.SendAppendEntriesToPeer(peer, currentTerm)
		// }
	}

	for logIndex := len(rf.log) - 1; logIndex > rf.commitIndex; logIndex-- {
		numMatching := 0
		// DPrintf("[%d] matchIndex: %v", rf.me, rf.matchIndex)
		for peer, _ := range rf.peers {
			if peer == rf.me {
				numMatching += 1
				continue
			}
			if rf.matchIndex[peer] >= logIndex {
				numMatching += 1
			}
		}
		if numMatching >= ((len(rf.peers)/2)+1) && rf.log[logIndex].Term == rf.currentTerm {
			IPrintf("[%d t %d] commiting entries through %d", rf.me, rf.currentTerm, logIndex)
			rf.commitIndex = logIndex
			rf.commitRemaining()
			break
		}
	}
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// Reject requests from earlier epoch (in particular, requests from old leaders) and reply with our epoch
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term == rf.currentTerm {
		rf.lastHeartbeatNano = time.Now().UnixNano()

		if rf.currentLeader != -1 && rf.currentLeader != args.LeaderId {
			// Some other node has conflicting leader for the same epoch; for now, consider this a fatal error,but could have better recovery strategy
			EPrintf("[%d t %d] FATAL: Received AppendEntries with leader %d, but current node leader was %d", rf.me, rf.currentTerm, args.LeaderId, rf.currentLeader)
			os.Exit(-1)
		}

		if rf.state == CandidateState {
			rf.becomeFollower(rf.currentTerm)
			rf.currentLeader = args.LeaderId
		}
	} else if args.Term > rf.currentTerm {
		// Request is from later epoch, so move forward our epoch and transition to follower state
		rf.becomeFollower(args.Term)
		rf.currentLeader = args.LeaderId
	}

	reply.Term = rf.currentTerm

	// No log entry at args.prevLogIndex, so return failure
	if args.PrevLogIndex >= len(rf.log) {
		DPrintf("[%d t %d] [AppendEntries from leader %d] doesn't have entry at %d, log = %s", rf.me, rf.currentTerm, args.LeaderId, args.PrevLogIndex, rf.logDebugString())
		reply.Success = false
		reply.SuggestedPrevIndex = len(rf.log) - 1
		return
	}

	// Terms don't match, hence entries don't match, so return failure
	if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("[%d t %d] [AppendEntries from leader %d] terms don't match at position %d, log = %s", rf.me, rf.currentTerm, args.LeaderId, args.PrevLogIndex, rf.logDebugString())
		reply.SuggestedPrevIndex = -1
		conflictingTerm := rf.log[args.PrevLogIndex].Term
		for i := 0; i < len(rf.log); i++ {
			if rf.log[i].Term == conflictingTerm {
				reply.SuggestedPrevIndex = i
				break
			}
		}
		reply.Success = false
		return
	}

	// Return if no data to append (i.e. was only heartbeat request)
	if len(args.Entries) == 0 {
		if args.LeaderCommitIndex > rf.commitIndex {
			rf.commitIndex = Min(len(rf.log)-1, args.LeaderCommitIndex)
			rf.commitRemaining()
		}

		reply.Success = true
		return
	}

	logIndex := args.PrevLogIndex + 1
	newEntriesIndex := 0

	for logIndex < len(rf.log) && newEntriesIndex < len(args.Entries) {
		if rf.log[logIndex].Term != args.Entries[newEntriesIndex].Term {
			break
		}
		logIndex += 1
		newEntriesIndex += 1
	}

	// Check for conflicts between existing and new entries
	for i := args.PrevLogIndex + 1; i < len(rf.log); i++ {
		entryIndex := i - (args.PrevLogIndex + 1)
		if entryIndex < len(args.Entries) && rf.log[i].Term != args.Entries[entryIndex].Term {
			rf.log = rf.log[:i]
			break
		}
	}

	// Add new entries not in log
	numMissing := len(args.Entries) - (len(rf.log) - (args.PrevLogIndex + 1))
	if numMissing > 0 {
		rf.log = append(rf.log, args.Entries[(len(args.Entries)-numMissing):]...)
	}

	if args.LeaderCommitIndex > rf.commitIndex {
		rf.commitIndex = Min(len(rf.log)-1, args.LeaderCommitIndex)
		rf.commitRemaining()
	}

	if numMissing > 0 {
		DPrintf("[%d t %d] [AppendEntries] stored %d entries starting at %d, log = %s", rf.me, rf.currentTerm, numMissing, args.PrevLogIndex+1, rf.logDebugString())
	}

	reply.Success = true
}

// RequestVote RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// Reject requests from earlier epoch and reply with our epoch
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		// Request is from later epoch, so move forward our epoch and transition to follower
		rf.becomeFollower(args.Term)
	}

	lastLogTerm := -1
	if len(rf.log) > 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}

	noCurrentLeader := rf.currentLeader == -1
	canVoteForCandidate := rf.votedFor == -1 || rf.votedFor == args.CandidateId
	validLogVersion := (args.LastLogTerm > lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= (len(rf.log)-1))
	grantVote := noCurrentLeader && canVoteForCandidate && validLogVersion

	if grantVote {
		DPrintf("[%d] voted for %d in term %d (args.LastLogTerm = %d, ourLastLogTerm = %d, log = %s)", rf.me, args.CandidateId, rf.currentTerm, args.LastLogTerm, lastLogTerm, rf.logDebugString())
		rf.votedFor = args.CandidateId
		rf.lastHeartbeatNano = time.Now().UnixNano()
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = grantVote
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader := rf.state == LeaderState

	if !isLeader {
		// IPrintf("[%d t %d] rejecting %v since not leader", rf.me, rf.currentTerm, command)
		return -1, -1, isLeader
	}

	defer rf.persist()

	index := len(rf.log)
	term := rf.currentTerm

	rf.log = append(rf.log, LogEntry{term, command})

	IPrintf("[%d leader %d] adding new log entry at index %d, value %v", rf.me, rf.currentTerm, index, command)

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.SendAppendEntriesToPeer(i, rf.currentTerm)
	}

	// Tests expect 1-indexed log as consistent with Raft paper
	return index + 1, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.applyChan = applyCh
	rf.me = me

	rf.log = make([]LogEntry, 0)
	rf.commitIndex = -1
	rf.lastApplied = -1

	// No need to initialize since initialization will happen after leader election
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.currentTerm = 0
	rf.state = FollowerState
	rf.votedFor = -1
	rf.currentLeader = -1
	rf.lastHeartbeatNano = time.Now().UnixNano()
	// can disable rand.seed() for more determinism
	rand.Seed(time.Now().UnixNano())
	rf.electionTimeoutMs = rand.Intn(ElectionTimeoutMaxMs-ElectionTimeoutMinMs) + ElectionTimeoutMinMs

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.ElectionTimeout(rf.electionTimeoutMs)

	DPrintf("[%d] CREATED with election timeout %d, log = %s", rf.me, rf.electionTimeoutMs, rf.logDebugString())

	return rf
}
