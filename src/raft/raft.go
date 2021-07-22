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
	//	"bytes"
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

type RaftLog struct {
	Command interface{}
	Term    int
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
	applyCh       chan ApplyMsg
	electionTimer *time.Timer

	state       RaftState
	log         []RaftLog
	currentTerm int
	votedFor    int
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	snapshotIndex int
	snapshotTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.lock()
	defer rf.unlock()
	// Your code here (2A).
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
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var l []RaftLog
	d.Decode(&term)
	d.Decode(&votedFor)
	d.Decode(&l)
	rf.currentTerm = term
	rf.votedFor = votedFor
	rf.log = l
	rf.lastApplied = len(l) - 1
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []RaftLog
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
	log.Printf("%v <- %v: RequestVote received: %v", args.CandidateId, rf.me, *args)
	rf.lock()
	defer rf.unlock()
	candidate := rf.votedFor
	if args.Term > rf.currentTerm {
		candidate = args.CandidateId
		rf.currentTerm = args.Term
		rf.state = Follower
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		log.Printf("%v <- %v: reject RequestVote: currentTerm = %v", args.CandidateId, rf.me, rf.currentTerm)
		return
	}
	if !rf.atLeastUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = false
		log.Printf("%v <- %v: reject RequestVote: lastApplied = %v", args.CandidateId, rf.me, rf.lastApplied)
		return
	}
	if candidate == args.CandidateId {
		reply.VoteGranted = true
		rf.votedFor = candidate
		go rf.nextElection()
		log.Printf("%v <- %v: ack RequestVote", args.CandidateId, rf.me)
		return
	}
	log.Printf("%v <- %v: reject RequestVote: votedFor = %v", args.CandidateId, rf.me, rf.votedFor)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	log.Printf("%v <- %v: AppendEntries received: %v", args.LeaderId, rf.me, *args)
	rf.lock()
	defer rf.unlock()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
	}
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		log.Printf("%v <- %v: reject AppendEntries: currentTerm = %v", args.LeaderId, rf.me, rf.currentTerm)
		return
	}
	ok, term := rf.matchPrevIndexAndTerm(args.PrevLogIndex, args.PrevLogTerm)
	if !ok {
		reply.Term = term
		log.Printf("%v <- %v: reject AppendEntries: log = %v", args.LeaderId, rf.me, rf.log)
		log.Printf("%v <- %v: reject AppendEntries: prevLogTerm = %v", args.LeaderId, rf.me, term)
		return
	}
	reply.Success = true
	if len(args.Entries) > 0 {
		rf.updateLog(args.PrevLogIndex, args.Entries)
		rf.persist()
	}
	rf.commitTo(args.LeaderCommit)
	go rf.nextElection()
	log.Printf("%v <- %v: ack AppendEntries", args.LeaderId, rf.me)
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
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	rf.lock()
	defer rf.unlock()

	// Your code here (2B).
	if rf.state != Leader {
		return rf.lastApplied, rf.currentTerm, false
	}
	rf.log = append(rf.log, RaftLog{
		Command: command,
		Term:    rf.currentTerm,
	})
	rf.persist()
	rf.lastApplied++
	return rf.lastApplied + 1, rf.currentTerm, true
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

func (rf *Raft) lock() {
	rf.mu.Lock()
}

func (rf *Raft) unlock() {
	rf.mu.Unlock()
}

func (rf *Raft) getLogEntry(index int) *RaftLog {
	if index < 0 || index > rf.lastApplied {
		log.Printf("%v log[%v] does not exist", rf.me, index)
		return nil
	}
	if index <= rf.snapshotIndex {
		log.Printf("%v log[%v] is already discarded", rf.me, index)
		return nil
	}
	return &rf.log[index-rf.snapshotIndex-1]
}

func (rf *Raft) updateLog(prevLogIndex int, entries []RaftLog) {
	d := 0
	if prevLogIndex < rf.snapshotIndex {
		d = rf.snapshotIndex - prevLogIndex
		prevLogIndex = rf.snapshotIndex
		entries = entries[d:]
	}
	rf.log = append(rf.log[:prevLogIndex-rf.snapshotIndex], entries...)
	rf.lastApplied = len(rf.log) + rf.snapshotIndex
	log.Printf("%v updated log: %v", rf.me, rf.log)
}

func (rf *Raft) atLeastUpToDate(index int, term int) bool {
	if rf.lastApplied < 0 {
		return true
	}
	currentTerm := rf.getLogEntry(rf.lastApplied).Term
	if currentTerm == term {
		return rf.lastApplied <= index
	}
	return currentTerm <= term
}

func (rf *Raft) matchPrevIndexAndTerm(index int, term int) (bool, int) {
	if index <= rf.snapshotIndex {
		return true, 0
	}
	if index <= rf.lastApplied {
		currentTerm := rf.getLogEntry(index).Term
		minTerm := term
		if minTerm > currentTerm {
			minTerm = currentTerm
		}
		return currentTerm == term, minTerm
	}
	if rf.lastApplied < 0 {
		return false, -1
	}
	return false, rf.getLogEntry(rf.lastApplied).Term
}

func (rf *Raft) commitTo(index int) {
	for rf.commitIndex < index {
		rf.commitIndex++
		log.Printf("%v commit %v: %v", rf.me, rf.commitIndex, rf.getLogEntry(rf.commitIndex))
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.getLogEntry(rf.commitIndex).Command,
			CommandIndex: rf.commitIndex + 1,
		}
	}
}

func (rf *Raft) tryToCommit() {
	i := rf.lastApplied
	nRequired := len(rf.peers)/2 + 1
	for i > rf.commitIndex {
		if rf.getLogEntry(i).Term != rf.currentTerm {
			return
		}
		nMatched := 1
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
			if rf.matchIndex[peer] >= i {
				nMatched++
			}
		}
		if nMatched >= nRequired {
			break
		}
		i--
	}
	rf.commitTo(i)
}

func (rf *Raft) tillNextHeartbeat(term int, peer int) {
	rf.lock()
	if term != rf.currentTerm {
		rf.unlock()
		return
	}
	rd := rand.Int()
	d := time.Duration(rd%100) + 100
	d = d * time.Millisecond
	rf.unlock()
	log.Printf("%v -> %v: next heartbeat in %v", rf.me, peer, d)
	time.Sleep(d)
	log.Printf("%v -> %v: start heartbeat", rf.me, peer)
}

func (rf *Raft) stopElectionTimer() {
	if rf.electionTimer != nil {
		rf.electionTimer.Stop()
		rf.electionTimer = nil
	}
}

func (rf *Raft) startElectionTimer(f func()) {
	rf.stopElectionTimer()
	rd := rand.Int()
	d := time.Duration(rd%300) + 400
	d = d * time.Millisecond
	log.Printf("%v: next election in %v", rf.me, d)
	rf.electionTimer = time.AfterFunc(d, f)
}

func (rf *Raft) nextElection() {
	rf.lock()
	defer rf.unlock()
	rf.startElectionTimer(func() {
		if rf.killed() {
			return
		}
		rf.lock()
		defer rf.unlock()
		rf.electionTimer = nil
		log.Printf("%v: start election", rf.me)
		rf.currentTerm++
		rf.state = Candidate
		rf.votedFor = rf.me
		go rf.collectVotes(rf.currentTerm)
		go rf.nextElection()
	})
}

func (rf *Raft) collectHeartbeatFromPeer(term int, peer int) {
	rf.lock()
	defer rf.unlock()
	for {
		if rf.killed() || rf.currentTerm != term {
			return
		}
		prevLogIndex := rf.nextIndex[peer] - 1
		if prevLogIndex < rf.snapshotIndex {
			log.Printf("%v -> %v: peer too far behind, install snapshot here", rf.me, peer)
			return
		}
		prevLogTerm := rf.snapshotTerm
		if prevLogIndex > rf.snapshotIndex {
			prevLogTerm = rf.getLogEntry(prevLogIndex).Term
		}
		var entries []RaftLog
		for i := rf.nextIndex[peer]; i <= rf.lastApplied; i++ {
			entries = append(entries, *rf.getLogEntry(i))
		}
		args := &AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: rf.commitIndex,
			Entries:      entries,
		}
		var reply AppendEntriesReply
		rf.unlock()
		log.Printf("%v -> %v: AppendEntries", rf.me, peer)
		ok := rf.sendAppendEntries(peer, args, &reply)
		rf.lock()
		if !ok {
			log.Printf("%v <- %v: AppendEntries nok", rf.me, peer)
			continue
		}
		if term != rf.currentTerm {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			go rf.nextElection()
			return
		}
		if !reply.Success {
			i := rf.nextIndex[peer] - 1
			for i > rf.snapshotIndex+1 && rf.getLogEntry(i).Term >= reply.Term {
				i--
			}
			rf.nextIndex[peer] = i
			continue
		}
		matchIndex := args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = matchIndex + 1
		rf.matchIndex[peer] = matchIndex
		rf.tryToCommit()
		rf.unlock()
		rf.tillNextHeartbeat(term, peer)
		rf.lock()
	}
}

func (rf *Raft) collectHeartbeats(term int) {
	rf.lock()
	defer rf.unlock()
	if term != rf.currentTerm {
		return
	}
	for peer := range rf.peers {
		rf.nextIndex[peer] = rf.lastApplied + 1
		rf.matchIndex[peer] = -1
		if peer == rf.me {
			continue
		}
		go rf.collectHeartbeatFromPeer(term, peer)
	}
}

func (rf *Raft) collectVoteFromPeer(peer int, args *RequestVoteArgs, r chan *RequestVoteReply) {
	var reply RequestVoteReply
	log.Printf("%v -> %v: RequestVote", rf.me, peer)
	ok := rf.sendRequestVote(peer, args, &reply)
	if !ok {
		log.Printf("%v <- %v: RequestVote nok", rf.me, peer)
		r <- nil
		return
	}
	r <- &reply
}

func (rf *Raft) collectVoteResults(term int, r chan *RequestVoteReply) {
	nPeers := len(rf.peers)
	nRequired := nPeers/2 + 1
	nReceived := 1
	nGranted := 1
	for v := range r {
		nReceived++
		rf.lock()
		if rf.currentTerm != term {
			rf.unlock()
			return
		}
		if v != nil {
			if v.VoteGranted {
				nGranted++
				if nGranted == nRequired {
					rf.state = Leader
					log.Printf("%v: elected as Leader", rf.me)
					rf.stopElectionTimer()
					go rf.collectHeartbeats(rf.currentTerm)
					rf.unlock()
					return
				}
			}
			if v.Term > rf.currentTerm {
				rf.currentTerm = v.Term
				rf.state = Follower
				rf.unlock()
				return
			}
		}
		rf.unlock()
		if nReceived == nPeers {
			return
		}
	}
}

func (rf *Raft) collectVotes(term int) {
	rf.lock()
	defer rf.unlock()
	if rf.killed() || term != rf.currentTerm {
		return
	}
	ch := make(chan *RequestVoteReply, len(rf.peers)-1)
	lastLogTerm := -1
	if rf.lastApplied >= 0 {
		lastLogTerm = rf.getLogEntry(rf.lastApplied).Term
	}
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastApplied,
		LastLogTerm:  lastLogTerm,
	}
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.collectVoteFromPeer(peer, args, ch)
	}
	go rf.collectVoteResults(term, ch)
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
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	rand.Seed(time.Now().UnixNano())

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.state = Follower
	rf.snapshotIndex = -1
	rf.snapshotTerm = -1
	rf.nextIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 0
	}
	rf.matchIndex = make([]int, len(peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = -1
	}
	rf.readPersist(persister.ReadRaftState())
	rf.nextElection()

	return rf
}
