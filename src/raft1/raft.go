package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const (
	RaftState_Leader    = "leader"
	RaftState_Follower  = "follower"
	RaftState_Candidate = "candidate"
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// election params

	currentTerm     int
	votedFor        int
	log             []LogEntry
	state           string
	lastHeard       time.Time
	electionTimeout time.Duration

	// log params
	applyCh     chan raftapi.ApplyMsg
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	applyCond   *sync.Cond

	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte
	pendingSnapshot   bool
	replicateCh       chan struct{}
}

func (rf *Raft) lastIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) termAt(index int) int {
	return rf.log[index-rf.lastIncludedIndex].Term
}

func (rf *Raft) entryAt(index int) LogEntry {
	return rf.log[index-rf.lastIncludedIndex]
}

func (rf *Raft) entriesFrom(index int) []LogEntry {
	src := rf.log[index-rf.lastIncludedIndex:]
	out := make([]LogEntry, len(src))
	copy(out, src)
	return out
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == RaftState_Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	rf.persister.Save(w.Bytes(), rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, lastIncludedIndex, lastIncludedTerm int
	var logEntries []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logEntries) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		panic("decode error")
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = logEntries
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex || index > rf.lastIndex() {
		return
	}

	term := rf.termAt(index)
	newLog := []LogEntry{{Term: term}}
	newLog = append(newLog, rf.entriesFrom(index+1)...)

	rf.log = newLog
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = term
	rf.snapshot = snapshot
	rf.persist()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = RaftState_Follower
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	lastIndex := rf.lastIndex()
	lastTerm := rf.termAt(lastIndex)
	upToDate := args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastHeard = time.Now()
		rf.electionTimeout = rf.newElectionTimeout()
	}
	rf.persist()
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	rf.state = RaftState_Follower
	rf.lastHeard = time.Now()
	rf.electionTimeout = rf.newElectionTimeout()
	reply.Term = rf.currentTerm
	reply.Success = false

	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.XTerm = -1
		reply.XLen = rf.lastIncludedIndex + 1
		rf.persist()
		return
	}

	if args.PrevLogIndex > rf.lastIndex() {
		reply.XTerm = -1
		reply.XLen = rf.lastIndex() + 1
		rf.persist()
		return
	}

	if rf.termAt(args.PrevLogIndex) != args.PrevLogTerm {
		reply.XTerm = rf.termAt(args.PrevLogIndex)
		i := args.PrevLogIndex
		for i > rf.lastIncludedIndex+1 && rf.termAt(i-1) == reply.XTerm {
			i--
		}
		reply.XIndex = i
		rf.persist()
		return
	}

	for k, entry := range args.Entries {
		pos := args.PrevLogIndex + 1 + k

		if pos > rf.lastIndex() {
			rf.log = append(rf.log, args.Entries[k:]...)
			break
		}

		if rf.termAt(pos) != entry.Term {
			rf.log = append(rf.log[:pos-rf.lastIncludedIndex], args.Entries[k:]...)
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		lastNew := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNew {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNew
		}
		rf.applyCond.Broadcast()
	}

	reply.Success = true
	rf.persist()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	rf.state = RaftState_Follower
	rf.lastHeard = time.Now()
	rf.electionTimeout = rf.newElectionTimeout()
	reply.Term = rf.currentTerm

	if args.LastIncludedIndex <= rf.commitIndex {
		rf.persist()
		return
	}

	newLog := []LogEntry{{Term: args.LastIncludedTerm}}
	if args.LastIncludedIndex < rf.lastIndex() && rf.termAt(args.LastIncludedIndex) == args.LastIncludedTerm {
		newLog = append(newLog, rf.entriesFrom(args.LastIncludedIndex+1)...)
	}

	rf.log = newLog
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.snapshot = args.Data
	rf.commitIndex = args.LastIncludedIndex
	rf.pendingSnapshot = true
	rf.persist()
	rf.applyCond.Broadcast()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

func (rf *Raft) applier() {
	for {
		rf.mu.Lock()
		for !rf.pendingSnapshot && rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		if rf.pendingSnapshot {
			msg := raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.lastIncludedTerm,
				SnapshotIndex: rf.lastIncludedIndex,
			}
			rf.pendingSnapshot = false
			if rf.lastApplied < rf.lastIncludedIndex {
				rf.lastApplied = rf.lastIncludedIndex
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			continue
		}

		msgs := make([]raftapi.ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msgs = append(msgs, raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.entryAt(i).Command,
				CommandIndex: i,
			})
		}
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()

		for _, msg := range msgs {
			rf.applyCh <- msg
		}
	}
}

func (rf *Raft) stepDown(term int) {
	rf.currentTerm = term
	rf.state = RaftState_Follower
	rf.votedFor = -1
	rf.lastHeard = time.Now()
	rf.electionTimeout = rf.newElectionTimeout()
	rf.persist()
}

func (rf *Raft) StartHeartBeat() {
	for {
		rf.mu.Lock()
		if rf.state != RaftState_Leader {
			rf.mu.Unlock()
			return
		}

		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			if rf.nextIndex[i] <= rf.lastIncludedIndex {
				args := &InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderID:          rf.me,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.lastIncludedTerm,
					Data:              rf.snapshot,
				}
				go rf.replicateSnapshot(i, args)
				continue
			}

			prevIndex := rf.nextIndex[i] - 1
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  rf.termAt(prevIndex),
				Entries:      rf.entriesFrom(rf.nextIndex[i]),
				LeaderCommit: rf.commitIndex,
			}
			go rf.replicateEntries(i, args)
		}
		rf.mu.Unlock()

		select {
		case <-time.After(100 * time.Millisecond):
		case <-rf.replicateCh:
		}
	}
}

func (rf *Raft) replicateEntries(i int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	if ok := rf.sendAppendEntries(i, args, reply); !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.stepDown(reply.Term)
		return
	}

	if rf.state != RaftState_Leader || rf.currentTerm != args.Term {
		return
	}

	if reply.Success {
		match := args.PrevLogIndex + len(args.Entries)
		if match > rf.matchIndex[i] {
			rf.matchIndex[i] = match
		}
		rf.nextIndex[i] = rf.matchIndex[i] + 1
		rf.advanceCommitIndex()
		return
	}

	if reply.XTerm == -1 {
		rf.nextIndex[i] = reply.XLen
	} else {
		rf.nextIndex[i] = reply.XIndex
		for k := rf.lastIndex(); k > rf.lastIncludedIndex; k-- {
			if rf.termAt(k) == reply.XTerm {
				rf.nextIndex[i] = k + 1
				break
			}
		}
	}

	if rf.nextIndex[i] < 1 {
		rf.nextIndex[i] = 1
	}
	if rf.nextIndex[i] > rf.lastIndex()+1 {
		rf.nextIndex[i] = rf.lastIndex() + 1
	}
}

func (rf *Raft) replicateSnapshot(i int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	if ok := rf.sendInstallSnapshot(i, args, reply); !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.stepDown(reply.Term)
		return
	}

	if rf.state != RaftState_Leader || rf.currentTerm != args.Term {
		return
	}

	if args.LastIncludedIndex > rf.matchIndex[i] {
		rf.matchIndex[i] = args.LastIncludedIndex
	}
	if rf.nextIndex[i] < rf.matchIndex[i]+1 {
		rf.nextIndex[i] = rf.matchIndex[i] + 1
	}
	rf.advanceCommitIndex()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != RaftState_Leader {
		return -1, rf.currentTerm, false
	}

	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	index := rf.lastIndex()
	rf.matchIndex[rf.me] = index
	rf.persist()

	select {
	case rf.replicateCh <- struct{}{}:
	default:
	}

	return index, rf.currentTerm, true
}

// call when lock is held
func (rf *Raft) advanceCommitIndex() {
	initialCommitIndex := rf.commitIndex
	for n := rf.commitIndex + 1; n <= rf.lastIndex(); n++ {
		if rf.termAt(n) != rf.currentTerm {
			continue
		}
		matchCount := 0
		for _, m := range rf.matchIndex {
			if m >= n {
				matchCount++
			}
		}
		if matchCount > len(rf.peers)/2 {
			rf.commitIndex = n
		}
	}

	if initialCommitIndex != rf.commitIndex {
		rf.applyCond.Broadcast()
	}
}

func (rf *Raft) newElectionTimeout() time.Duration {
	return time.Duration(300+(rand.Int63()%300)) * time.Millisecond
}

func (rf *Raft) startElections() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == RaftState_Leader || time.Since(rf.lastHeard) <= rf.electionTimeout {
		return
	}
	rf.currentTerm++
	rf.state = RaftState_Candidate
	voteCount := 1
	rf.votedFor = rf.me
	rf.lastHeard = time.Now()
	rf.electionTimeout = rf.newElectionTimeout()
	rf.persist()

	lastLogIndex := rf.lastIndex()
	req := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  rf.termAt(lastLogIndex),
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(i int) {
			resp := &RequestVoteReply{}
			if ok := rf.sendRequestVote(i, req, resp); !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if resp.Term > rf.currentTerm {
				rf.stepDown(resp.Term)
				return
			}

			if rf.state != RaftState_Candidate || rf.currentTerm != req.Term {
				return
			}
			if resp.VoteGranted {
				voteCount++
			}

			if voteCount > len(rf.peers)/2 {
				rf.state = RaftState_Leader
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for p := range rf.peers {
					rf.nextIndex[p] = rf.lastIndex() + 1
					rf.matchIndex[p] = 0
				}
				rf.matchIndex[rf.me] = rf.lastIndex()
				go rf.StartHeartBeat()
			}
		}(i)
	}
}

func (rf *Raft) ticker() {
	for {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state != RaftState_Leader && time.Since(rf.lastHeard) > rf.electionTimeout {
			go rf.startElections()
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.log = []LogEntry{{Term: 0}}
	rf.votedFor = -1
	rf.electionTimeout = rf.newElectionTimeout()
	rf.lastHeard = time.Now()
	rf.state = RaftState_Follower
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.replicateCh = make(chan struct{}, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
