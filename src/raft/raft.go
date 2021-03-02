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
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

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
	receivedRpcDuringTimeout bool
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
	Term    int
	Success bool
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
	rf.receivedRpcDuringTimeout = true
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
		rf.receivedRpcDuringTimeout = true
		reply.VoteGranted = true
		rf.votedFor = &args.CandidateID
	} else if rf.votedFor != nil {
		rf.logger.Printf("Server %d in term %d, already voted for server %d", rf.me, rf.currentTerm, *rf.votedFor)
	} else if args.Term >= rf.currentTerm && args.LastLogIndex >= len(rf.log) {
		rf.logger.Printf("Server %d votes for %d", rf.me, args.CandidateID)
		reply.VoteGranted = true
		reply.Result = true
		rf.votedFor = &args.CandidateID
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logger.Printf("args.LeaderID=%d, Server%d locked in AppendEntries", args.LeaderID, rf.me)
	rf.receivedRpcDuringTimeout = true

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.logger.Printf("[Term%d]Server %d received AppendEntries with higher term %d from server %d, "+
			"return to Follower", rf.currentTerm, rf.me, args.Term, args.LeaderID)
		rf.currentState = Follower
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		return
	}

	if rf.currentState == Leader {
		rf.logger.Printf("Server%d is leader", rf.me)
	} else if rf.currentState == Candidate {
		rf.logger.Printf("Server%d is Candidate", rf.me)
		rf.currentState = Follower
		rf.receivedRpcDuringTimeout = true
		rf.logger.Printf("Server %d received AppendEntries from server %d, Candidate->Follower", rf.me, args.LeaderID)
	} else if rf.currentState == Follower {
		rf.logger.Printf("Server%d is Follower", rf.me)
		rf.receivedRpcDuringTimeout = true
		rf.logger.Printf("Server %d received AppendEntries from server %d, still Follower", rf.me, args.LeaderID)
		rf.currentTerm = args.Term
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	rf.logger.Printf("[Term%d] Server %d replyed AppendEntries to server %d", rf.currentTerm, rf.me, args.LeaderID)
	rf.logger.Printf("Server%d unlocked in AppendEntries", rf.me)
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

const rpcTimeout = time.Millisecond * 200

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
		rf.logger.Printf("sendRequestVote timeout")
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
		rf.logger.Printf("sendAppendEntries timeout")
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
	rf.logger = log.New(logFile, "", log.Ltime|log.Lmicroseconds)
	electionTimeout := genElectionTimeout()
	rf.timeAlarm = time.After(electionTimeout)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go runServer(rf)
	return rf
}

func genElectionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	timeout := 800 + rand.Intn(200)
	return time.Duration(timeout) * time.Millisecond
}

func runServer(rf *Raft) {
	rf.logger.Printf("Server %d start running", rf.me)
	for {
		if rf.killed() {
			fmt.Printf("server killed\n")
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

func runAsLeader(rf *Raft) {
	rf.logger.Printf("[Term%d]Server %d run as Leader", rf.currentTerm, rf.me)
	for {
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		if rf.currentState != Leader {
			rf.mu.Unlock()
			return
		}
		for index, _ := range rf.peers {
			if index == rf.me {
				continue
			}
			args := &AppendEntriesArgs{
				LeaderID: rf.me,
				Term:     rf.currentTerm,
			}
			reply := &AppendEntriesReply{}
			rf.logger.Printf("[Term%d]Server %d send AppendEntries to server %d", rf.currentTerm, rf.me, index)
			rf.mu.Unlock()
			ok := rf.sendAppendEntries(index, args, reply)
			rf.mu.Lock()
			if !ok {
				rf.logger.Printf("[Term%d]Leader %d send AppendEntries to server %d, got no reply", rf.currentTerm,
					rf.me, index)
				} else if reply.Term > rf.currentTerm {
				rf.logger.Printf("[Term%d]Leader %d received AppendEntries resp with high term %d from server %d, "+
					"turn to follower", rf.currentTerm, rf.me, reply.Term, index)
				rf.currentTerm = reply.Term
				rf.currentState = Follower
				rf.mu.Unlock()
				return
			}

		}
		rf.mu.Unlock()
		time.Sleep(500 * time.Millisecond)
	}
}

func runAsFollower(rf *Raft) {
	rf.logger.Printf("[Term%d]Server %d run as Follower", rf.currentTerm, rf.me)
	rf.mu.Lock()
	rf.votedFor = nil
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
	electionTimeout := genElectionTimeout()
	voteNum := 1
	unreachedServerNum := 0
	var okChs []chan bool
	var replyChs []chan *RequestVoteReply
	for i := 0; i < len(rf.peers); i++ {
		okChs = append(okChs, make(chan bool, 1))
		replyChs = append(replyChs, make(chan *RequestVoteReply, 1))
	}
	currentTerm := rf.currentTerm
	me := rf.me
	logLength := len(rf.log)
	lastLogTerm := 0
	if len(rf.log) > 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	rf.mu.Unlock()
	for serverIndex, _ := range rf.peers {
		if serverIndex == rf.me {
			continue
		}
		go func(server int) {
			args := &RequestVoteArgs{
				Term:         currentTerm,
				CandidateID:  me,
				LastLogIndex: logLength,
				LastLogTerm:  lastLogTerm,
			}
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, reply)
			okChs[server] <- ok
			replyChs[server] <- reply
		}(serverIndex)
	}
	rf.mu.Lock()
	for i, _ := range okChs {
		if i == rf.me {
			continue
		}
		ok := <-okChs[i]
		reply := <-replyChs[i]
		if ok {
			if reply.VoteGranted {
				rf.logger.Printf("Server %d got vote from server %d", rf.me, reply.ServerID)
				voteNum++
			} else if reply.Term > rf.currentTerm {
				rf.logger.Printf("[Term%d]Server %d received RequestVote resp with higher term %d from server %d, "+
					"turn to Follower", rf.currentTerm, rf.me, reply.Term, reply.ServerID)
				rf.currentTerm = reply.Term
				rf.currentState = Follower
				rf.mu.Unlock()
				return
			}
		} else {
			rf.logger.Printf("[Term%d]Server %d got no RequestVote reply from server %d", rf.currentTerm, rf.me, reply.ServerID)
			unreachedServerNum += 1
		}
	}
	if rf.currentState != Candidate {
		rf.mu.Unlock()
		return
	}
	rf.logger.Printf("[Term%d] Server%d:unreachedServerNum =%d", rf.currentTerm, rf.me, unreachedServerNum)
	validServerNum := len(rf.peers) - unreachedServerNum
	if validServerNum == 1 {
		rf.currentState = Follower
		rf.mu.Unlock()
		return
	} else if voteNum >= (validServerNum/2 + 1) {
		rf.currentState = Leader
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	time.Sleep(electionTimeout)
	rf.mu.Lock()
	// sleep期间可能收到来自leader的rpc，改变了状态
	if rf.currentState != Candidate {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
}
