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
// rf.Getstate() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"
const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

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
	state          int
	currentTerm    int
	votedFor       int // -1 means no vote
	votesGranted   int
	heartbeatTimer *time.Timer
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == Leader
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
	// rf.persister.SaveRaftstate(data)
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
	/// Your data here (2A, 2B).
	Term        int
	CandidateID int
}

// RequestVoteReply struct
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// AppendEntriesRequest struct
type AppendEntriesRequest struct {
	Term     int
	LeaderID int
}

// AppendEntriesReply struct
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) resetTimer() {
	rf.heartbeatTimer.Stop()
	rf.heartbeatTimer.Reset(time.Duration(150+rand.Intn(150)) * time.Millisecond)
}

// All convertTo* methods assume that they are run SYNCHRONOUSLY and that the caller owns the lock to rf
func (rf *Raft) convertToFollower(term int) {
	rf.state = Follower
	rf.votedFor = -1
	rf.currentTerm = term

	rf.resetTimer()
}

func (rf *Raft) convertToCandidate() {
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.votesGranted = 1
	rf.resetTimer()

	go rf.runElection()
}

func (rf *Raft) convertToLeader() {
	rf.state = Leader
	rf.heartbeatTimer.Stop()

	go rf.sendHeartbeat()
}

///////////////////////////////////////////////////////////////

// Send heartbeats periodically as the leader. This runs indefinitely as long as the leader is alive
func (rf *Raft) sendHeartbeat() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		var wg sync.WaitGroup

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			wg.Add(1)
			go func(i int) {
				request := &AppendEntriesRequest{rf.currentTerm, rf.me}
				reply := &AppendEntriesReply{}
				wg.Done()

				if rf.sendAppendEntries(i, request, reply) {
					rf.handleAppendEntries(request, reply)
				}
			}(i)
		}
		wg.Wait()
		rf.mu.Unlock()
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

// Send AppendEntry to a particular host
func (rf *Raft) sendAppendEntry(i int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	appendRequest := AppendEntriesRequest{rf.currentTerm, rf.me}
	appendResponse := AppendEntriesReply{}
	if rf.state == Leader && rf.sendAppendEntries(i, &appendRequest, &appendResponse) {
		if !appendResponse.Success {
			rf.convertToFollower(appendResponse.Term)
		}
	}
}

// When a candidate times it out will trigger the election
func (rf *Raft) runElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Candidate {
		return
	}
	var wg sync.WaitGroup

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(i int) {
			request := &RequestVoteArgs{rf.currentTerm, rf.me}
			reply := &RequestVoteReply{}
			wg.Done()

			if rf.sendRequestVote(i, request, reply) {
				rf.handleRequestVote(request, reply)
			}
		}(i)
	}
	wg.Wait()
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesRequest, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
	} else if args.Term > rf.currentTerm && rf.state != Follower {
		rf.convertToFollower(args.Term)
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID || args.Term > rf.currentTerm {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.currentTerm = args.Term
		reply.Term = args.Term
		rf.resetTimer()
	}
}

//
//  RequestVote RPC response handler.
//
func (rf *Raft) handleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Candidate || rf.currentTerm != args.Term {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
	} else if reply.VoteGranted {
		rf.votesGranted++
		if rf.votesGranted*2 > len(rf.peers) {
			rf.convertToLeader()
		}
	}
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		rf.convertToFollower(args.Term)
		reply.Term = args.Term
		reply.Success = true
	}
}

//
//  AppendEntries RPC response handler.
//
func (rf *Raft) handleAppendEntries(args *AppendEntriesRequest, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader || rf.currentTerm < args.Term {
		return
	} else if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
	}
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
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.votesGranted = 0
	rf.state = Follower
	rf.heartbeatTimer = time.AfterFunc(time.Duration(150+rand.Intn(150))*time.Millisecond, func() { rf.mu.Lock(); defer rf.mu.Unlock(); rf.convertToCandidate() })
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
