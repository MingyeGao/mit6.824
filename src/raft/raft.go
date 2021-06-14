package raft

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

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

//import "bytes"
import "../labgob"

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
	CommandTerm  int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm              int
	votedFor                 *int
	log                      []raftLog
	commitIndex              int
	lastApplied              int
	nextIndex                []int
	matchIndex               []int
	currentState             ServerState
	timeAlarm                <-chan time.Time
	heartBeatTicker          *time.Ticker
	logger                   Logger
	lab2BLogger              Logger
	lab2CLogger              Logger
	lab3BLogger              Logger
	receivedRpcDuringTimeout bool
	msgCh                    chan ApplyMsg
	isIndexArraysReady       bool

	//3B
	snapshotLastIncludedIndex int
	snapshotLastIncludedTerm  int
}

// 日志的index从1开始，
// 没有日志时，prevLogIndex==0
func (r *Raft) hasLogEntryAtIndex(i int) bool {
	var lastIndex int
	if len(r.log) == 0 {
		lastIndex = r.snapshotLastIncludedIndex
	} else {
		lastIndex = r.log[len(r.log)-1].Index
	}
	if lastIndex >= i {
		return true
	}
	return false
}

func (r *Raft) getIndexInCurrentArray(i int) int {
	return i - r.snapshotLastIncludedIndex - 1
}

func (r *Raft) conflictTermAtIndex(i int, leaderTerm int) bool {
	indexInCurrentArray := r.getIndexInCurrentArray(i)
	if indexInCurrentArray < 0 {
		return false
	}

	logEntryTerm := r.log[indexInCurrentArray].Term
	if logEntryTerm != leaderTerm {
		return true
	}
	return false
}

func (r *Raft) getFirstIndexWithSameTermAsIndexAt(i int) int {
	indexInCurrentArray := r.getIndexInCurrentArray(i)
	LogEntryAtI := r.log[indexInCurrentArray]
	term := LogEntryAtI.Term
	var result int
	for j := i - 1; j >= 0; j-- {
		if r.log[j].Term != term {
			break
		}
		result = j
	}
	return result + 1
}

func (r *Raft) logEntryAtIndex(i int) raftLog {
	return r.log[r.getIndexInCurrentArray(i)]
}

func (r *Raft) isIndexIncludedInSnapshot(i int) bool {
	return r.snapshotLastIncludedIndex >= i
}

func (r *Raft) updateLog(logEntries []raftLog) {
	if len(logEntries) == 0 {
		return
	}
	for _, entry := range logEntries {
		newEntryIndex := entry.Index
		newEntryTerm := entry.Term
		if r.isIndexIncludedInSnapshot(newEntryIndex) {
			continue
		}
		if !r.hasLogEntryAtIndex(newEntryIndex) {
			r.lab2CLogger.Printf("if\n")
			r.log = append(r.log, entry)
		} else {
			r.lab2CLogger.Printf("else\n")
			if r.conflictTermAtIndex(newEntryIndex, newEntryTerm) {
				r.log = r.log[0 : newEntryIndex-1]
				r.log = append(r.log, entry)
			} else {
				r.log[newEntryIndex-1] = entry
			}
		}
	}
}

func (r *Raft) updateCommitIndex(leaderCommit int) {
	if leaderCommit > r.commitIndex {
		tmp := 0
		if leaderCommit < len(r.log)+1 {
			tmp = leaderCommit
		} else {
			tmp = len(r.log)
		}
		for i := r.commitIndex + 1; i <= tmp; i++ {
			var msg ApplyMsg
			if i <= r.snapshotLastIncludedIndex {
				msg = ApplyMsg{
					CommandValid: false,
				}
			} else {
				msg = ApplyMsg{
					CommandValid: true,
					CommandIndex: i,
					Command:      r.logEntryAtIndex(i).Command,
					CommandTerm:  r.logEntryAtIndex(i).Term,
				}
			}

			r.msgCh <- msg
		}
		r.commitIndex = tmp
	}
}

func (r *Raft) lastLogIndex() int {
	if len(r.log) == 0 {
		return r.snapshotLastIncludedIndex
	}
	return r.log[len(r.log)-1].Index
}

func (r *Raft) lastLogTerm() int {
	if len(r.log) == 0 {
		return r.snapshotLastIncludedTerm
	}
	return r.log[len(r.log)-1].Term
}

type Logger interface {
	Printf(format string, v ...interface{})
}

type ServerState int

const (
	Candidate ServerState = 1
	Leader    ServerState = 2
	Follower  ServerState = 3
)

func (state ServerState) String() string {
	switch state {
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Follower:
		return "Follower"
	}
	return ""
}

type raftLog struct {
	Term    int
	Index   int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	//fmt.Printf("%v:[Term %d]Server %d get state %d\n", time.Now().Format(time.RFC3339), rf.currentTerm, rf.me,
	//	rf.currentState)
	term = rf.currentTerm
	isleader = rf.currentState == Leader
	rf.mu.Unlock()
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
	data := rf.buildStateData()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) buildStateData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	votedFor := ""
	if rf.votedFor != nil {
		votedFor = strconv.FormatInt(int64(*rf.votedFor), 10)
	}
	e.Encode(votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	return data
}

func (rf *Raft) BuildSnapshotData(content SnapshotContent) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(content)
	data := w.Bytes()
	return data
}

func (rf *Raft) explainStateData(data []byte) (currentTerm int, votedFor string, log []raftLog) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		panic("readPersist unmarshal failed")
	}
	return
}

func (rf *Raft) ExplainSnapshotData(data []byte) (content SnapshotContent) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	if d.Decode(&content) != nil {
		rf.lab3BLogger.Printf("no snapshot content unmarshalled")
	}
	return
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
	currentTerm, votedFor, log := rf.explainStateData(data)
	rf.currentTerm = currentTerm
	rf.log = log
	if votedFor == "" {
		rf.votedFor = nil
	} else {
		votedForInt, _ := strconv.Atoi(votedFor)
		rf.votedFor = &votedForInt
	}

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []raftLog
	LeaderCommit int
}
type InstallSnapshotArgs struct {
	SnapshotContent   []byte
	LastIncludedIndex int
	LastIncludedTerm  int
	ServerID          int
	Term              int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	ServerID    int
	Term        int
	VoteGranted bool
	Result      bool
}

type AppendEntriesReply struct {
	Term         int
	Success      bool
	ConflictTerm *int
	RetryIndex   int
}

type InstallSnapshotReply struct {
	Term int
}

type SnapshotContent struct {
	Database          map[string]string
	LastIncludedIndex int
	LastIncludedTerm  int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.Printf("Server %d got RequestVote RPC from %d", rf.me, args.CandidateID)
	reply.ServerID = rf.me
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.logger.Printf("[Term%d]Server %d received RequestVote with higher term %d from server %d, "+
			"return to Follower", rf.currentTerm, rf.me, args.Term, args.CandidateID)
		rf.currentState = Follower
		rf.currentTerm = args.Term
		rf.votedFor = nil
	}
	if rf.votedFor != nil {
		rf.logger.Printf("Server %d in term %d, already voted for server %d", rf.me, rf.currentTerm, *rf.votedFor)
	} else {
		if args.LastLogTerm > rf.lastLogTerm() || (args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.
			lastLogIndex()) {
			rf.logger.Printf("Server %d votes for %d, lastLogTerm:%dvs%d, lastLogIndex:%dvs%d", rf.me,
				args.CandidateID, rf.lastLogTerm(), args.LastLogTerm, rf.lastLogIndex(), args.LastLogIndex)
			reply.VoteGranted = true
			reply.Result = true
			rf.votedFor = &args.CandidateID
			rf.receivedRpcDuringTimeout = true
		} else {
			rf.logger.Printf("Server%d didn't voter for %d, lastLogTerm:%dvs%d, lastLogIndex:%dvs%d", rf.me,
				args.CandidateID, rf.lastLogTerm(), args.LastLogTerm, rf.lastLogIndex(), args.LastLogIndex)
		}
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logger.Printf("args.LeaderID=%d, Server%d locked in AppendEntries", args.LeaderID, rf.me)

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}
	rf.receivedRpcDuringTimeout = true
	if args.Term > rf.currentTerm {
		rf.logger.Printf("[Term%d]Server %d received AppendEntries with higher term %d from server %d, "+
			"return to Follower", rf.currentTerm, rf.me, args.Term, args.LeaderID)
		rf.currentState = Follower
		rf.currentTerm = args.Term
		//reply.Term = rf.currentTerm
		//return
	}

	reply.Term = rf.currentTerm
	if rf.currentState == Leader {
		rf.logger.Printf("Server%d is leader", rf.me)
		rf.currentState = Follower
		//return
	}
	// 可以AppendEntries
	rf.lab2CLogger.Printf("in appendEntries, reached here")
	rf.currentState = Follower
	rf.receivedRpcDuringTimeout = true
	rf.logger.Printf("Server %d received AppendEntries from server %d, currentState is %s", rf.me, args.LeaderID,
		rf.currentState)

	rf.lab3BLogger.Printf("snapshotLastIncludedIndex=%d, args.PrevLogIndex=%d, len(log)=%d",
		rf.snapshotLastIncludedIndex, args.PrevLogIndex, len(args.Entries))

	if rf.snapshotLastIncludedIndex >= args.PrevLogIndex + len(args.Entries) {
		//todo
		rf.lab3BLogger.Printf("exceed, snapshotLastIncludedIndex=%d, args.PrevLogIndex=%d, len(log)=%d",
			rf.snapshotLastIncludedIndex, args.PrevLogIndex, len(args.Entries))
		reply.Success = false
		reply.RetryIndex = rf.snapshotLastIncludedIndex + 1
		return
	}

	if !rf.hasLogEntryAtIndex(args.PrevLogIndex) {
		rf.lab2CLogger.Printf("server %d, appendEntries return false, no entry, log=%v, prevLogIndex=%d, "+
			"prevLogTerm=%d", rf.me, rf.log, args.PrevLogIndex, args.PrevLogTerm)
		reply.RetryIndex = len(rf.log) + 1
		reply.Success = false
		return
	}
	if rf.conflictTermAtIndex(args.PrevLogIndex, args.PrevLogTerm) {
		reply.RetryIndex = rf.getFirstIndexWithSameTermAsIndexAt(args.PrevLogIndex)
		rf.lab2CLogger.Printf("server %d, appendEntries return false, conflict entry, log=%v, prevLogIndex=%d, "+
			"prevLogTerm=%d, "+"retryIndex=%d", rf.me, rf.log, args.PrevLogIndex, args.PrevLogTerm, reply.RetryIndex)
		reply.ConflictTerm = &rf.currentTerm
		reply.Success = false
		return
	}
	rf.lab2CLogger.Printf("%d recved append from leader%d, args=%v\n", rf.me, args.LeaderID, args)
	rf.lab2CLogger.Printf("%d before append log=%v\n", rf.me, rf.log)
	rf.updateLog(args.Entries)
	rf.lab2CLogger.Printf("%d after append log=%v\n", rf.me, rf.log)
	rf.persist()
	rf.lab2CLogger.Printf("server%d, log=%v", rf.me, rf.log)
	rf.updateCommitIndex(args.LeaderCommit)
	rf.lab2CLogger.Printf("server%d, commitIndex=%v", rf.me, rf.commitIndex)

	reply.Term = rf.currentTerm
	reply.Success = true
	rf.logger.Printf("[Term%d] Server %d replyed AppendEntries to server %d", rf.currentTerm, rf.me, args.LeaderID)
	rf.logger.Printf("Server%d unlocked in AppendEntries", rf.me)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *AppendEntriesReply) {
	rf.lab3BLogger.Printf("Reached here 1")
	rf.mu.Lock()
	rf.lab3BLogger.Printf("Reached here 2")
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.lab3BLogger.Printf("[Term%d] Server%d recved InstallSnapshot from leader%d with lower term%d]",
			rf.currentTerm, rf.me, args.ServerID, args.Term)
		return
	}

	rf.currentTerm = args.Term
	rf.receivedRpcDuringTimeout = true
	rf.currentState = Follower
	stateData := rf.buildStateData()
	rf.persister.SaveStateAndSnapshot(args.SnapshotContent, stateData)
	rf.dropLogEntry(args.LastIncludedIndex)
	rf.snapshotLastIncludedIndex = args.LastIncludedIndex
	rf.snapshotLastIncludedTerm = args.LastIncludedTerm
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

const rpcTimeout = time.Millisecond * 500

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ch := make(chan bool)
	alarm := time.After(rpcTimeout)
	go func(ch chan bool) {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		ch <- ok
	}(ch)

	var ok bool
	select {
	case <-alarm:
		rf.logger.Printf("sendRequestVote to %d timeout", server)
		ok = false
	case ok = <-ch:
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ch := make(chan bool)
	alarm := time.After(rpcTimeout)
	go func(ch chan bool) {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		ch <- ok
	}(ch)

	var ok bool
	select {
	case <-alarm:
		rf.logger.Printf("sendAppendEntries to %d timeout", server)
		ok = false
	case ok = <-ch:
	}
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ch := make(chan bool)
	alarm := time.After(rpcTimeout)
	go func(ch chan bool) {
		ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
		ch <- ok
	}(ch)

	var ok bool
	select {
	case <-alarm:
		rf.lab3BLogger.Printf("InstallSnapshot to %d timeout", server)
		ok = false
	case ok = <-ch:
	}
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

func (rf *Raft) LeaderSaveSnapshot(snapshotData []byte, lastIncludedIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	stateData := rf.buildStateData()
	rf.persister.SaveStateAndSnapshot(stateData, snapshotData)
	lastIncludedTerm := rf.logEntryAtIndex(lastIncludedIndex).Term
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(snapshotData []byte, lastIncludedIndex, leaderID, term, followerID, lastIncludedTerm int) {
			for {
				args := &InstallSnapshotArgs{
					SnapshotContent:   snapshotData,
					LastIncludedIndex: lastIncludedIndex,
					LastIncludedTerm:  lastIncludedTerm,
					ServerID:          leaderID,
					Term:              term,
				}
				reply := new(InstallSnapshotReply)
				ok := rf.sendInstallSnapshot(followerID, args, reply)
				if ok {
					rf.mu.Lock()
					if lastIncludedIndex > rf.matchIndex[followerID] {
						rf.matchIndex[followerID] = lastIncludedIndex
					}
					if lastIncludedIndex + 1 > rf.nextIndex[followerID] {
						rf.nextIndex[followerID] = lastIncludedIndex + 1
					}
					rf.lab3BLogger.Printf("nextIndex[%d]=%d", followerID, rf.nextIndex[followerID])
					rf.mu.Unlock()
					break
				}
			}

		}(snapshotData, lastIncludedIndex, rf.me, rf.currentTerm, i, lastIncludedTerm)
	}
	rf.dropLogEntry(lastIncludedIndex)
	rf.snapshotLastIncludedIndex = lastIncludedIndex
	rf.snapshotLastIncludedTerm = lastIncludedTerm

}

func (rf *Raft) dropLogEntry(lastIncludedIndex int) (isLeader bool, succeed bool) {
	rf.lab3BLogger.Printf("Reached dropLogEntry")
	if rf.getIndexInCurrentArray(lastIncludedIndex) < 0 {
		panic(fmt.Sprintf("last index %d already dropped", lastIncludedIndex))
	}
	lastIncludedIndexCurrentIndex := rf.getIndexInCurrentArray(lastIncludedIndex)
	rf.lab3BLogger.Printf("before drop, log=%v", rf.log)
	rf.log = rf.log[lastIncludedIndexCurrentIndex+1:]
	rf.lab3BLogger.Printf("after drop, log=%v", rf.log)
	return true, true
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return index, term, false
	}
	rf.lab2CLogger.Printf("server%d got command %v, state is %s", rf.me, command, rf.currentState)
	if rf.currentState != Leader {
		return index, term, false
	}
	for !rf.isIndexArraysReady {
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
	}

	newLogEntry := raftLog{
		Term:    rf.currentTerm,
		Index:   rf.snapshotLastIncludedIndex + len(rf.log) + 1,
		Command: command,
	}
	rf.log = append(rf.log, newLogEntry)
	rf.persist()
	index = newLogEntry.Index
	term = newLogEntry.Term
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentState = Follower
	logFile, err := os.Create(fmt.Sprintf("raft-log-%d.txt", rf.me))
	if err != nil {
		log.Panicf("create log failed, error=%v", err)
	}
	log2BFile, err := os.Create(fmt.Sprintf("raft-log2B-%d.txt", rf.me))
	if err != nil {
		log.Panicf("create log failed, error=%v", err)
	}
	log2CFile, err := os.Create(fmt.Sprintf("raft-log2C-%d.txt", rf.me))
	if err != nil {
		log.Panicf("create log2C falied, error=%v", err)
	}
	log3BFile, err := os.Create(fmt.Sprintf("raft-log3B-%d.txt", rf.me))
	if err != nil {
		log.Panicf("create log3B falied, error=%v", err)
	}
	rf.logger = log.New(logFile, "", log.Ltime|log.Lmicroseconds)
	rf.lab2CLogger = log.New(log2BFile, "", log.Ltime|log.Lmicroseconds)
	rf.lab2CLogger = log.New(log2CFile, "", log.Ltime|log.Lmicroseconds)
	rf.lab3BLogger = log.New(log3BFile, "", log.Ltime|log.Lmicroseconds)
	electionTimeout := genElectionTimeout()
	rf.timeAlarm = time.After(electionTimeout)
	rf.msgCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// lab 3C
	snapshot := rf.ExplainSnapshotData(persister.ReadSnapshot())
	rf.snapshotLastIncludedIndex = snapshot.LastIncludedIndex
	rf.snapshotLastIncludedTerm = snapshot.LastIncludedTerm

	go runServer(rf)
	return rf
}

func genElectionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	timeout := 300 + rand.Intn(800)
	return time.Duration(timeout) * time.Millisecond
}

func runServer(rf *Raft) {
	rf.logger.Printf("Server %d start running", rf.me)
	for {
		if rf.killed() {
			//fmt.Printf("server killed\n")
			return
		}
		switch rf.currentState {
		case Leader:
			runAsLeader(rf)
		case Follower:
			runAsFollower(rf)
		case Candidate:
			runAsCandidate(rf)
		}
	}

}

func (rf *Raft) leaderBuildAppendEntriesRequest(server int) *AppendEntriesArgs {
	args := &AppendEntriesArgs{
		LeaderID:     rf.me,
		Term:         rf.currentTerm,
		PrevLogIndex: rf.nextIndex[server] - 1,
		LeaderCommit: rf.commitIndex,
	}

	if args.PrevLogIndex <= rf.snapshotLastIncludedTerm {
		args.PrevLogIndex = rf.snapshotLastIncludedIndex
	}

	if args.PrevLogIndex != 0 {
		args.PrevLogTerm = rf.logEntryAtIndex(args.PrevLogIndex).Term
	}

	endEntry := args.PrevLogIndex + 10000
	if endEntry > len(rf.log) {
		endEntry = len(rf.log)
	}
	args.Entries = rf.log[args.PrevLogIndex:endEntry]
	rf.lab2CLogger.Printf("Entries=%v", args.Entries)
	return args
}

func (rf *Raft) leaderDealWithSuccessAppendEntries(serverIndex int, args *AppendEntriesArgs) {
	if len(args.Entries) == 0 {
		return
	}
	rf.matchIndex[serverIndex] = args.PrevLogIndex + len(args.Entries)
	rf.nextIndex[serverIndex] = rf.matchIndex[serverIndex] + 1
	rf.lab2CLogger.Printf("SuccessAppendEntries, matchIndex[%d]=%d, nextIndex[%d]=%d", serverIndex,
		rf.matchIndex[serverIndex], serverIndex, rf.nextIndex[serverIndex])
	// 更新commitIndex
	rf.leaderUpdateCommitIndex()
}

func (rf *Raft) leaderUpdateCommitIndex() {
	var matchIndexList []int
	for i := 0; i < len(rf.peers); i++ {
		matchIndexList = append(matchIndexList, rf.matchIndex[i])
	}
	sort.Slice(matchIndexList, func(i, j int) bool {
		return matchIndexList[i] < matchIndexList[j]
	})
	rf.lab2CLogger.Printf("matchIndexList=%v\n", matchIndexList)
	majorityMatch := matchIndexList[len(rf.peers)/2]
	if majorityMatch > rf.commitIndex {
		for i := rf.commitIndex + 1; i <= majorityMatch; i++ {
			entry := rf.logEntryAtIndex(i)
			msg := ApplyMsg{
				CommandValid: true,
				CommandIndex: entry.Index,
				Command:      entry.Command,
				CommandTerm:  entry.Term,
			}
			rf.msgCh <- msg
		}
		rf.commitIndex = majorityMatch
	}
	rf.lab2CLogger.Printf("leader%d, commitIndex=%d", rf.me, rf.commitIndex)
}

func (rf *Raft) leaderDealWithFailAppendEntries(serverIndex int, reply *AppendEntriesReply) {
	//if rf.nextIndex[serverIndex] > 1 {
	//	rf.nextIndex[serverIndex]--
	//}
	if reply.RetryIndex > rf.nextIndex[serverIndex] {
		rf.matchIndex[serverIndex] = reply.RetryIndex - 1
	}
	rf.nextIndex[serverIndex] = reply.RetryIndex
	rf.lab2CLogger.Printf("leader%d, server%d retryIndex=%v", rf.me, serverIndex, reply.RetryIndex)
}

func runAsLeader(rf *Raft) {
	rf.logger.Printf("[Term%d]Server %d run as Leader", rf.currentTerm, rf.me)
	// init data
	rf.mu.Lock()
	rf.votedFor = nil
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.snapshotLastIncludedIndex + 1
		rf.matchIndex[i] = 0
	}
	rf.isIndexArraysReady = true
	rf.mu.Unlock()
	rf.lab2CLogger.Printf("nextIndex and matchIndex init succeed")
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(index int) {
			for {
				rf.mu.Lock()
				if rf.killed() || rf.currentState != Leader {
					rf.mu.Unlock()
					return
				}
				rf.lab2CLogger.Printf("nextIndex[%d]=%v", index, rf.nextIndex[index])
				args := rf.leaderBuildAppendEntriesRequest(index)
				reply := &AppendEntriesReply{}
				rf.lab2CLogger.Printf("[Term%d]Server %d send AppendEntries to server %d", rf.currentTerm, rf.me, index)
				rf.lab2CLogger.Printf("Leader%d sendAppendEntries to %d, args=%v", rf.me, index, args)
				rf.mu.Unlock()
				ok := rf.sendAppendEntries(index, args, reply)
				rf.mu.Lock()
				if !ok { // rpc超时
					rf.lab2CLogger.Printf("[Term%d]Leader %d send AppendEntries to server %d, got no reply", rf.currentTerm,
						rf.me, index)
				} else if reply.Term < rf.currentTerm {
					rf.lab2CLogger.Printf("[Term%d]Leader %d sendAppendEntries recved old rpc with term %d",
						rf.currentTerm, rf.me, reply.Term)
				} else if reply.Term > rf.currentTerm { // leader的Term 落后于其他server的Term
					rf.lab2CLogger.Printf("[Term%d]Leader %d received AppendEntries resp with high term %d from server %d, "+
						"turn to follower", rf.currentTerm, rf.me, reply.Term, index)
					rf.currentTerm = reply.Term
					rf.currentState = Follower
					rf.persist()
					rf.mu.Unlock()
					return
				} else {
					if reply.Success {
						rf.leaderDealWithSuccessAppendEntries(index, args)
						rf.persist()
					} else {
						rf.leaderDealWithFailAppendEntries(index, reply)
					}
				}
				rf.mu.Unlock()
				time.Sleep(150 * time.Millisecond)
			}
		}(index)
	}
	for {
		rf.mu.Lock()
		if rf.killed() || rf.currentState != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}

}

func runAsFollower(rf *Raft) {
	rf.logger.Printf("[Term%d]Server %d run as Follower", rf.currentTerm, rf.me)
	rf.mu.Lock()
	rf.votedFor = nil
	rf.persist()
	rf.mu.Unlock()
	for {
		if rf.killed() {
			return
		}
		timeout := genElectionTimeout()
		rf.logger.Printf("Server %d genTimeout=%v", rf.me, timeout)
		time.Sleep(timeout)
		rf.mu.Lock()
		if rf.receivedRpcDuringTimeout {
			rf.receivedRpcDuringTimeout = false
		} else {
			rf.currentState = Candidate
			rf.logger.Printf("Server %d switched to candidate", rf.me)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}

func runAsCandidate(rf *Raft) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.logger.Printf("[Term%d]Server %d run as Candidate", rf.currentTerm, rf.me)
	rf.votedFor = &rf.me
	rf.persist()
	electionTimeout := genElectionTimeout()
	voteNum := 1
	//unreachedServerNum := 0
	var okChs []chan bool
	var replyChs []chan *RequestVoteReply
	for i := 0; i < len(rf.peers); i++ {
		okChs = append(okChs, make(chan bool, 1))
		replyChs = append(replyChs, make(chan *RequestVoteReply, 1))
	}
	currentTerm := rf.currentTerm
	me := rf.me
	lastLogIndex := 0
	if len(rf.log) > 0 {
		lastLogIndex = rf.log[len(rf.log)-1].Index
	}
	lastLogTerm := 0
	if len(rf.log) > 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	rf.mu.Unlock()
	respChan := make(chan *RequestVoteReply, len(rf.peers))
	for serverIndex, _ := range rf.peers {
		if serverIndex == rf.me {
			continue
		}
		go func(server, currentTerm int) {
			for {
				rf.mu.Lock()
				if rf.currentState != Candidate || rf.currentTerm != currentTerm || rf.killed() {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				args := &RequestVoteArgs{
					Term:         currentTerm,
					CandidateID:  me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(server, args, reply)
				if ok {
					respChan <- reply
					return
				}
			}

		}(serverIndex, currentTerm)
	}
	timer := time.NewTicker(electionTimeout)
	for {
		select {
		case r := <-respChan:
			rf.mu.Lock()
			if rf.currentState != Candidate || rf.killed() {
				rf.mu.Unlock()
				return
			}
			if r.VoteGranted {
				rf.logger.Printf("Server %d got vote from server %d", rf.me, r.ServerID)
				voteNum++
				rf.logger.Printf("Server%d received vote from %d", rf.me, r.ServerID)
				validServerNum := len(rf.peers)
				if voteNum >= (validServerNum/2 + 1) {
					rf.currentState = Leader
					rf.isIndexArraysReady = false
					rf.persist()
					rf.mu.Unlock()
					return
				}
			} else if r.Term < rf.currentTerm {
				rf.lab2CLogger.Printf("[Term%d]Leader %d requestVote recved old rpc with term %d",
					rf.currentTerm, rf.me, r.Term)
			} else if r.Term > rf.currentTerm {
				rf.logger.Printf("Server %d received RequestVote resp with higher term %d from server %d, "+
					"turn to Follower", rf.me, r.Term, r.ServerID)
				rf.currentTerm = r.Term
				rf.currentState = Follower
				rf.persist()
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		case <-timer.C:
			rf.mu.Lock()
			if rf.currentState != Candidate || rf.killed() {
				rf.mu.Unlock()
				return
			}
			//rf.currentTerm++
			rf.persist()
			rf.mu.Unlock()
			return
		}
	}
}
