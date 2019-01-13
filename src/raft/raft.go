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

// Overall locking philosophy: lock in scopes as large as possible (entire RPC method, RPC response handler, timer expiration), but don't lock while waiting (e.g. while calling `select` statement) and give up lock as close to blocking and as soon as possible after unblocking
// Every blocking operation should be in its own goroutine

// In future version: use channel as semaphore to limit number of outgoing RPC requests to prevent network  congestion/overload (needs to be priority-based though?)

import (
	"bytes"
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
const ElectionTimeoutMinMs = 800
const ElectionTimeoutMaxMs = 1500

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	applyChan chan ApplyMsg       // channel to send committed messages to

	state int // Current state, one of {FollowerState, CandidateState, LeaderState}

	// Election-related state
	currentTerm               int                       // Latest term seen
	currentLeader             int                       // Index of leader for current term, or -1 if none elected yet
	votedFor                  int                       // CandidateId that received vote in current term, or -1 if none voted for yet
	electionTimeoutMs         int                       // Election timeout, in ms
	lastHeartbeatNano         int64                     // Timestamp of last heartbeat received from a valid leader, in unix time nanoseconds
	heartbeatReplyChan        chan AppendEntriesContext // In shared struct so can send heartbeats immediately after becoming leader without waiting for next interval
	appendEntriesResponseChan chan AppendEntriesContext // Single goroutine that handles all responses

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
	Term    int  // later term for leader to update itself
	Success bool // true if follower had matching previous log entry
}

// Precondition: rf.mu is locked; rf.state = LeaderState
func (rf *Raft) sendAllHeartbeats() {
	// Send AppendEntries heartbeat to every peer
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(currentTerm int, peer *labrpc.ClientEnd, peerId int, me int, commitIndex int, heartbeatChan chan AppendEntriesContext) {
			args := AppendEntriesArgs{currentTerm, me, -1, -1, make([]LogEntry, 0), commitIndex}
			var reply AppendEntriesReply
			ok := peer.Call("Raft.AppendEntries", &args, &reply)
			heartbeatInfo := AppendEntriesContext{ok, &args, &reply, currentTerm, peerId}
			heartbeatChan <- heartbeatInfo
		}(rf.currentTerm, rf.peers[i], i, rf.me, rf.commitIndex, rf.heartbeatReplyChan)
	}
}

// Precondition: rf.mu is locked
func (rf *Raft) sendMissingEntries() {
	// DPrintf("[%d] sendMissingEntries", rf.me)
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		entriesToSend := rf.log[(rf.nextIndex[i]):]
		DPrintf("[%d] sending %v to %d", rf.me, entriesToSend, i)
		// if len(entriesToSend) == 0 {
		//     continue
		// }

		prevLogIndex := rf.nextIndex[i] - 1
		prevLogTerm := -1
		if prevLogIndex >= 0 {
			prevLogTerm = rf.log[prevLogIndex].Term
		}
		args := AppendEntriesArgs{rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, entriesToSend, rf.commitIndex}

		go func(currentTerm int, peer *labrpc.ClientEnd, peerId int, args AppendEntriesArgs, responseChan chan AppendEntriesContext) {
			var reply AppendEntriesReply
			ok := peer.Call("Raft.AppendEntries", &args, &reply)
			appendEntriesContext := AppendEntriesContext{ok, &args, &reply, currentTerm, peerId}
			responseChan <- appendEntriesContext
		}(rf.currentTerm, rf.peers[i], i, args, rf.appendEntriesResponseChan)
	}
}

// Precondition: rf.mu is locked
// Usually called when we get a RPC reply with a higher term, which means we should immediately move to follower state
// Also provide new leader, or -1 if not known (which happens if we got heartbeat back with higher term without new leader info)
func (rf *Raft) moveToNewTerm(newTerm int, newLeader int) {
	rf.currentTerm = newTerm
	rf.currentLeader = newLeader
	rf.votedFor = -1         // since we're moving to this term, we by definition haven't voted for anyone yet
	rf.state = FollowerState // anytime we move to new term we move to follower state, except when we increment term after detecting leader failure and becoming candidate
}

func (rf *Raft) RunHeartbeat() {
	// QUESTION: should we wait here for reply/timeout from all heartbeat requests? or, should we also move on to next heartbeat timeout? => yes, because the followers that did reply will trigger election timeout if another follower hasn't replied

	heartbeatTimeChan := make(chan int, 100)
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
			// DPrintf("[%d] got heartbeatTIMEChan lock", rf.me)

			// Not leader, so don't do anything
			if rf.state != LeaderState {
				break
			}

			rf.sendMissingEntries()
		case heartbeatInfo := <-rf.heartbeatReplyChan:
			rf.mu.Lock()
			// DPrintf("[%d] got heartbeatREPLYchan lock", rf.me)

			// We are in old epoch, so update epoch and revert to follower
			if heartbeatInfo.reply.Term > rf.currentTerm {
				rf.moveToNewTerm(heartbeatInfo.reply.Term, -1)
			}
		}

		// Release lock before waiting on anything
		rf.mu.Unlock()
	}
}

func (rf *Raft) CommitRemaining() {
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied += 1
		DPrintf("[%d] committing entry %d", rf.me, rf.lastApplied)
		msg := ApplyMsg{true, rf.log[rf.lastApplied].Command, rf.lastApplied + 1}
		rf.applyChan <- msg
	}
}

func (rf *Raft) ElectionTimeout(electionTimeoutMs int) {
	for {
		time.Sleep(time.Duration(electionTimeoutMs) * time.Millisecond)

		rf.mu.Lock()
		// DPrintf("[%d] got election timeout lock", rf.me)

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
		votesInfoChan := make(chan RequestVoteContext, 100)

		// For every peer, spawn a goroutine that sends a vote request to that peer and writes output to channel
		for i, _ := range rf.peers {
			// Don't send RequestVote to ourselves
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

		// Create channel and goroutine to stop election if we haven't won within 100 ms
		winTimeoutChan := make(chan int, 100)
		go func() {
			time.Sleep(time.Duration(ElectionWinTimeoutMs) * time.Millisecond)
			winTimeoutChan <- 1
		}()

		votesNeeded := (len(rf.peers) / 2) + 1
		votesReceived := 1
		votesDeclined := 0

		// Release lock after spawning all RPC goroutines
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
				// DPrintf("[%d] got votesInfoChan lock", rf.me)

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
					rf.moveToNewTerm(voteInfo.reply.Term, -1)
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
				// DPrintf("[%d] got winTimeoutChan lock", rf.me)
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
			DPrintf("[%d] received %d votes, becoming the leader", rf.me, votesReceived)
			DPrintf("[%d] log: %v", rf.me, rf.log)
			rf.state = LeaderState

			// Initialize rf.nextIndex and rf.matchIndex
			for i, _ := range rf.peers {
				if i == rf.me {
					continue
				}
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = -1
			}

			// Send heartbeat to every peer immediately after becoming leader
			// rf.sendAllHeartbeats()
			rf.sendMissingEntries()
		}

		// Didn't get enough votes, so go back to follower
		if lostElection {
			rf.state = FollowerState
			rf.lastHeartbeatNano = time.Now().UnixNano()
			DPrintf("[%d] didn't get enough votes, going back to follower", rf.me)
		}

		rf.mu.Unlock()
	}
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// DPrintf("[%d] AppendEntries() pre-lock", rf.me)
	rf.mu.Lock()
	// DPrintf("[%d] AppendEntries() post-lock", rf.me)
	defer rf.mu.Unlock()
	defer rf.persist()

	// Some other node has conflicting leader for the same epoch; for now, consider this a fatal error,but could have better recovery strategy
	if args.Term == rf.currentTerm && rf.currentLeader != -1 && rf.currentLeader != args.LeaderId {
		DPrintf("FATAL: Received AppendEntries in epoch %d with leader %d, but current node leader was %d", rf.currentTerm, args.LeaderId, rf.currentLeader)
		os.Exit(-1)
	}

	// Reject requests from earlier epoch (in particular, requests from old leaders) and reply with our epoch
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		DPrintf("[%d] AppendEntries error A", rf.me)
		reply.Success = false
		return
	}

	if args.Term == rf.currentTerm {
		rf.lastHeartbeatNano = time.Now().UnixNano()
	}

	// If request is from later epoch, move forward our epoch and transition to follower state
	if args.Term > rf.currentTerm {
		DPrintf("[%d] got heartbeat from newer term %d and leader %d, moving to follower", rf.me, args.Term, args.LeaderId)
		rf.moveToNewTerm(args.Term, args.LeaderId)
	}

	reply.Term = rf.currentTerm

	// Update state from heartbeat
	// DPrintf("[%d] now knows that leader is %d in term %d", rf.me, args.LeaderId, rf.currentTerm)

	rf.state = FollowerState
	rf.currentLeader = args.LeaderId

	// No log entry at args.prevLogIndex, so return failure
	if args.PrevLogIndex >= len(rf.log) {
		DPrintf("[%d] AppendEntries error B, args.PrevLogIndex = %d, rf.log = %v", rf.me, args.PrevLogIndex, rf.log)
		reply.Success = false
		return
	}

	// Terms don't match, hence entries don't match, so return failure
	if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("AppendEntries error C")
		reply.Success = false
		return
	}

	// Return if no data to append (i.e. was only heartbeat request)
	if len(args.Entries) == 0 {
		if args.LeaderCommitIndex > rf.commitIndex {
			rf.commitIndex = Min(len(rf.log)-1, args.LeaderCommitIndex)
			DPrintf("CommitRemaining A")
			rf.CommitRemaining()
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
		DPrintf("CommitRemaining B")
		rf.CommitRemaining()
	}

	DPrintf("[%d] accepted/replicated %d entries starting at %d, term %d", rf.me, numMissing, args.PrevLogIndex+1, rf.currentTerm)
	reply.Success = true
}

// RequestVote RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// DPrintf("[%d] just entered RequestVote to respond to %d", rf.me, args.CandidateId)
	rf.mu.Lock()
	// DPrintf("[%d] just got RequestVote lock to respond to %d", rf.me, args.CandidateId)
	defer rf.mu.Unlock()
	defer rf.persist()

	// DPrintf("[%d] received RequestVote from %d", rf.me, args.CandidateId)

	// Reject requests from earlier epoch and reply with our epoch
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// Save current term, which is compared to args.LastLogTerm (since we don't store that in rf state)
	ourLastLogTerm := -1
	if len(rf.log) > 0 {
		ourLastLogTerm = rf.log[len(rf.log)-1].Term
	}

	// If request is from later epoch, move forward our epoch and transition to follower
	if args.Term > rf.currentTerm {
		rf.moveToNewTerm(args.Term, -1)
	}

	noCurrentLeader := rf.currentLeader == -1
	canVoteForCandidate := rf.votedFor == -1 || rf.votedFor == args.CandidateId
	validLogVersion := (args.LastLogTerm > ourLastLogTerm) || (args.LastLogTerm == ourLastLogTerm && args.LastLogIndex >= (len(rf.log)-1))
	grantVote := noCurrentLeader && canVoteForCandidate && validLogVersion

	if grantVote {
		DPrintf("[%d] voted for %d in term %d (args.LastLogTerm = %d, ourLastLogTerm = %d, log = %v)", rf.me, args.CandidateId, rf.currentTerm, args.LastLogTerm, ourLastLogTerm, rf.log)
		rf.votedFor = args.CandidateId
		rf.lastHeartbeatNano = time.Now().UnixNano()
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = grantVote
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
	rf.mu.Lock()
	// DPrintf("[%d] got Start() lock", rf.me)
	defer rf.mu.Unlock()

	isLeader := rf.state == LeaderState

	if !isLeader {
		return -1, -1, isLeader
	}

	index := len(rf.log)
	term := rf.currentTerm

	rf.log = append(rf.log, LogEntry{term, command})

	DPrintf("[%d] matchIndex: %v", rf.me, rf.matchIndex)
	DPrintf("[%d] adding new log entry at index %d, term %d, value %v", rf.me, index, term, command)

	rf.sendMissingEntries()

	// Tests expect 1-indexed log, consistent with Raft paper
	return index + 1, term, isLeader
}

func (rf *Raft) AppendEntriesResponseHandler() {
	for {
		select {
		case appendEntriesContext := <-rf.appendEntriesResponseChan:
			rf.mu.Lock()
			// DPrintf("[%d] acquired lock in AppendEntriesResponseHandler", rf.me)

			if !appendEntriesContext.ok {
				break
			}

			// 3 terms: (A) term request was sent (B) term in response (C) term we are in
			// Note that B >= A based on AppendEntries handler implementation
			//  if B > C: switch to follower; return
			//  if B < C: return
			//  if A < B == C: return, since response is rejected request
			//  if A == B == C: normal handler

			peer := appendEntriesContext.peerId
			args := appendEntriesContext.args
			response := appendEntriesContext.reply

			// If response is from future epoch, move forward our epoch and transition to follower
			if response.Term > rf.currentTerm {
				rf.moveToNewTerm(appendEntriesContext.reply.Term, -1)
				break
			}

			// If response is for request from previous epoch, ignore
			if response.Term < rf.currentTerm {
				break
			}

			// Response is rejection since request was stale and got rejected
			if appendEntriesContext.term < response.Term {
				break
			}

			if response.Success {
				// DPrintf("[%d] got success AppendEntries response from %d", rf.me, peer)
				newMatchIndex := args.PrevLogIndex + len(args.Entries)
				if newMatchIndex > rf.matchIndex[peer] {
					rf.matchIndex[peer] = newMatchIndex
					rf.nextIndex[peer] = newMatchIndex + 1
				}
			} else {
				DPrintf("[%d] got error AppendEntries response from %d", rf.me, peer)
				// Conflict at args.PrevLogIndex (or follower didn't have anything there), so decrement nextIndex[peer] and try again
				// Actually, always safe to send AppendEntries() that won't delete date (since AppendEntries is idempotent) WRONG but actually, not safe to decrement rf.nextIndex[peer]!
				// If there was a conflict, we may have solved it through later RPCs
				// if args.PrevLogIndex < rf.matchIndex[peer] {
				rf.nextIndex[peer] -= 1

				// Resend request with new nextIndex
				entriesToSend := rf.log[(rf.nextIndex[peer]):]
				prevLogIndex := rf.nextIndex[peer] - 1
				prevLogTerm := -1
				if prevLogIndex >= 0 {
					prevLogTerm = rf.log[prevLogIndex].Term
				}
				DPrintf("[%d] re-sending AppendEntries to %d with nextIndex =  %d", rf.me, peer, rf.nextIndex[peer])
				args := AppendEntriesArgs{rf.currentTerm, rf.me, prevLogIndex, prevLogTerm, entriesToSend, rf.commitIndex}
				go func(currentTerm int, peer *labrpc.ClientEnd, peerId int, args AppendEntriesArgs, responseChan chan AppendEntriesContext) {
					var reply AppendEntriesReply
					ok := peer.Call("Raft.AppendEntries", &args, &reply)
					appendEntriesContext := AppendEntriesContext{ok, &args, &reply, currentTerm, peerId}
					responseChan <- appendEntriesContext
				}(rf.currentTerm, rf.peers[peer], peer, args, rf.appendEntriesResponseChan)
				// }
			}

			for logIndex := len(rf.log) - 1; logIndex > rf.commitIndex; logIndex-- {
				numMatching := 0
				DPrintf("[%d] matchIndex: %v", rf.me, rf.matchIndex)
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
					DPrintf("[%d] commiting entries through %d", rf.me, logIndex)
					rf.commitIndex = logIndex
					DPrintf("CommitRemaining C")
					rf.CommitRemaining()
					break
				}
			}
			// default:
			//     rf.mu.Lock()
			//     rf.CommitRemaining()
		}

		rf.mu.Unlock()
	}
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
	rf.applyChan = applyCh
	rf.me = me

	rf.log = make([]LogEntry, 0)
	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.currentTerm = 0
	rf.currentLeader = -1
	rf.votedFor = -1
	rf.state = FollowerState
	rf.lastHeartbeatNano = time.Now().UnixNano()
	// can disable rand.seed() for more determinism
	rand.Seed(time.Now().UnixNano())
	rf.electionTimeoutMs = rand.Intn(ElectionTimeoutMaxMs-ElectionTimeoutMinMs) + ElectionTimeoutMinMs
	DPrintf("[%d] has election timeout %d", rf.me, rf.electionTimeoutMs)
	rf.heartbeatReplyChan = make(chan AppendEntriesContext, 100)
	rf.appendEntriesResponseChan = make(chan AppendEntriesContext, 100)
	go rf.ElectionTimeout(rf.electionTimeoutMs)
	go rf.RunHeartbeat()
	go rf.AppendEntriesResponseHandler()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
