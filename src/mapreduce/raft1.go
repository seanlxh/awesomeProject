package mapreduce

import (
	"math/rand"
	"raft"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)
import "labrpc"

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

const NULL int = -1

type Log struct {
	Term    int
	Command interface{}
}

type AppendEntriesArgs struct { //发送日志
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct { //发送日志回复
	Term    int
	Success bool
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *raft.Persister     // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	state State

	//server
	currentTerm int
	votedFor    int
	log         []Log

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	applyChannel         chan ApplyMsg
	votedChannel         chan bool
	AppendEntriesChannel chan bool
	killCh               chan bool //for Kill()
}

func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

func (rf *Raft) persist() {

}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if args.Term > rf.currentTerm {
		rf.beFollower(args.Term)
		send(rf.votedChannel)
	}
	success := false
	if args.Term < rf.currentTerm {

	} else if rf.votedFor != NULL && rf.votedFor != args.CandidateId {

	} else if args.LastLogTerm < rf.getLastLogTerm() {

	} else if args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex < rf.getLastLogIdx() {

	} else {
		rf.votedFor = args.CandidateId
		success = true
		rf.state = Follower
		send(rf.votedChannel)
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = success
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
	//index := -1
	//term := -1
	//isLeader := true
	//
	//// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := (rf.state == Leader)

	if isLeader {
		index = rf.getLastLogIdx() + 1
		newLog := Log{
			Term:    rf.currentTerm,
			Command: command,
		}
		rf.log = append(rf.log, newLog)
	}
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
	persister *raft.Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = NULL
	rf.log = make([]Log, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	// initialize from state persisted before a crash

	rf.applyChannel = applyCh
	rf.AppendEntriesChannel = make(chan bool, 1)
	rf.votedChannel = make(chan bool, 1)
	rf.killCh = make(chan bool, 1)
	rf.readPersist(persister.ReadRaftState())

	heartbeatTime := time.Duration(100) * time.Millisecond

	go func() {
		for {
			select {
			case <-rf.killCh:
				return
			default:
			}
			electionTime := time.Duration(rand.Intn(100)+300) * time.Millisecond

			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()

			switch state {
			case Follower, Candidate:
				select {
				case <-rf.votedChannel:
				case <-rf.AppendEntriesChannel:
				case <-time.After(electionTime):
					rf.mu.Lock()
					rf.beCandidate() //becandidate, Reset election timer, then start election
					rf.mu.Unlock()
				}
			case Leader:
				rf.startAppendLog()
				time.Sleep(heartbeatTime)
			}

		}

	}()
	return rf
}

func (rf *Raft) beCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	go rf.startElection()
}

func (rf *Raft) startElection() {
	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		rf.getLastLogIdx(),
		rf.getLastLogTerm(),
	}
	var votes int32 = 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(idx int) {
			reply := &RequestVoteReply{}
			ret := rf.sendRequestVote(idx, &args, reply)
			if ret {
				if reply.Term > rf.currentTerm {
					rf.beFollower(reply.Term)
					//send(rf.votedChannel)
					return
				}
				if rf.state != Candidate {
					return
				}
				if reply.VoteGranted {
					atomic.AddInt32(&votes, 1)
				}
				if atomic.LoadInt32(&votes) > int32(len(rf.peers)/2) {
					rf.beLeader()
					send(rf.votedChannel)
				}
			}

		}(i)

	}
}

func send(ch chan bool) {
	select {
	case <-ch:
	default:
	}
	ch <- true
}

func (rf *Raft) getLastLogIdx() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	idx := rf.getLastLogIdx()
	if idx < 0 {
		return -1
	}
	return rf.log[idx].Term
}

func (rf *Raft) beFollower(term int) {
	rf.state = Follower
	rf.votedFor = NULL
	rf.currentTerm = term
}

func (rf *Raft) beLeader() {
	if rf.state != Candidate {
		return
	}
	rf.state = Leader
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.getLastLogIdx() + 1
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer send(rf.AppendEntriesChannel)
	if args.Term > rf.currentTerm {
		rf.beFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}
	prevIndexTerm := -1
	if args.PrevLogIndex >= 0 && args.PrevLogIndex < len(rf.log) {
		prevIndexTerm = rf.log[args.PrevLogIndex].Term
	}
	if prevIndexTerm != args.PrevLogTerm {
		return
	}

	index := args.PrevLogIndex
	for i := 0; i < len(args.Entries); i++ {
		index++
		if index >= len(rf.log) {
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
		if rf.log[index].Term != args.Entries[i].Term {
			rf.log = rf.log[:index]
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.getLastLogIdx())
		rf.updateLastApplied()
	}
	reply.Success = true
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func (rf *Raft) startAppendLog() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(idx int) {
			if rf.state == Leader {
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.getPrevLogIdx(idx),
					PrevLogTerm:  rf.getPrevLogTerm(idx),
					Entries:      append([]Log{}, rf.log[rf.nextIndex[idx]:]...),
					LeaderCommit: rf.commitIndex,
				}
				reply := &AppendEntriesReply{}
				res := rf.sendAppendEntries(idx, &args, reply)
				rf.mu.Lock()
				if !res || rf.state != Leader {
					rf.mu.Unlock()
					return
				}
				if reply.Term > rf.currentTerm {
					rf.beFollower(reply.Term)
					rf.mu.Unlock()
					return
				}
				if reply.Success {
					rf.matchIndex[idx] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[idx] = rf.matchIndex[idx] + 1
					rf.updateCommitIndex()
					rf.mu.Unlock()
					return
				} else {
					rf.nextIndex[idx]--
					rf.mu.Unlock()
				}

			}
		}(i)
	}
}

func (rf *Raft) updateCommitIndex() {
	rf.matchIndex[rf.me] = len(rf.log) - 1
	copyMatchIndex := make([]int, len(rf.matchIndex))
	copy(copyMatchIndex, rf.matchIndex)
	sort.Ints(copyMatchIndex)
	N := copyMatchIndex[len(copyMatchIndex)/2]
	if N > rf.commitIndex && rf.log[N].Term == rf.currentTerm {
		rf.commitIndex = N
		rf.updateLastApplied()
	}
}

func (rf *Raft) updateLastApplied() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		curLog := rf.log[rf.lastApplied]
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      curLog.Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyChannel <- applyMsg
	}
}

func (rf *Raft) getPrevLogIdx(i int) int {
	raft.DPrintf("fdfddsffsdfsffs%d", len(rf.nextIndex))
	return rf.nextIndex[i] - 1
}

func (rf *Raft) getPrevLogTerm(i int) int {
	prevLogIdx := rf.getPrevLogIdx(i)
	if prevLogIdx < 0 {
		return -1
	}
	return rf.log[prevLogIdx].Term
}
