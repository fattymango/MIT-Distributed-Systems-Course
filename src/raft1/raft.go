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
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == RaftState_Leader
	return term, isleader
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
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
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
	var currentTerm, votedFor int
	var logEntries []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logEntries) != nil {
		panic("decode error")

	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logEntries
	}
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
		rf.persist()
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = RaftState_Follower
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	lastIndex := len(rf.log) - 1
	lastTerm := rf.log[lastIndex].Term
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.persist()
		return
	}
	for k, entry := range args.Entries {
		pos := args.PrevLogIndex + 1 + k

		if pos >= len(rf.log) {
			rf.log = append(rf.log, args.Entries[k:]...)
			break
		}

		if rf.log[pos].Term != entry.Term {
			rf.log = append(rf.log[:pos], args.Entries[k:]...)
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
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) applier() {
	for {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		slice := make([]raftapi.ApplyMsg, 0)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			slice = append(slice, raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i,
			})
		}
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()
		for _, msg := range slice {
			rf.applyCh <- msg
		}

	}
}

func (rf *Raft) StartHeartBeat() {
	for {
		rf.mu.Lock()
		if rf.state != RaftState_Leader {
			rf.mu.Unlock()
			return
		}

		allArgs := make([]*AppendEntriesArgs, len(rf.peers))
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			prevIndex := rf.nextIndex[i] - 1

			entries := make([]LogEntry, len(rf.log)-rf.nextIndex[i])
			copy(entries, rf.log[rf.nextIndex[i]:])

			allArgs[i] = &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  rf.log[prevIndex].Term,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
		}
		rf.mu.Unlock()

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(i int, args *AppendEntriesArgs) {
				reply := &AppendEntriesReply{}

				if ok := rf.sendAppendEntries(i, args, reply); !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = RaftState_Follower
					rf.votedFor = -1
					rf.lastHeard = time.Now()
					rf.electionTimeout = rf.newElectionTimeout()
					rf.persist()
					return
				}

				if rf.state != RaftState_Leader || rf.currentTerm != args.Term {
					return
				}

				if reply.Success {
					rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[i] = rf.matchIndex[i] + 1
					rf.advanceCommitIndex()
				} else {
					rf.nextIndex[i]--
					if rf.nextIndex[i] < 1 {
						rf.nextIndex[i] = 1
					}
				}
			}(i, allArgs[i])
		}

		time.Sleep(100 * time.Millisecond)
	}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != RaftState_Leader {
		return -1, rf.currentTerm, false
	}
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	index = len(rf.log) - 1
	rf.matchIndex[rf.me] = index
	term = rf.currentTerm
	isLeader = true
	rf.persist()
	return index, term, isLeader
}

// call when lock is held
func (rf *Raft) advanceCommitIndex() {
	initialCommitIndex := rf.commitIndex
	for n := rf.commitIndex + 1; n < len(rf.log); n++ {
		if rf.log[n].Term != rf.currentTerm {
			continue
		}
		matchCount := 0
		for _, i := range rf.matchIndex {
			if i >= n {
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
	lastLogIndex := len(rf.log) - 1
	req := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  rf.log[lastLogIndex].Term,
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(i int) {
			resp := &RequestVoteReply{}
			ok := rf.sendRequestVote(i, req, resp)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if resp.Term > rf.currentTerm {
				rf.currentTerm = resp.Term
				rf.state = RaftState_Follower
				rf.votedFor = -1
				rf.persist()
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
				for p := range rf.nextIndex {
					rf.nextIndex[p] = len(rf.log)
					rf.matchIndex[p] = 0
					if p == rf.me {
						rf.matchIndex[p] = len(rf.log) - 1
					}
				}
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
	rf.log = append(make([]LogEntry, 0), LogEntry{0, nil})
	rf.votedFor = -1
	rf.electionTimeout = rf.newElectionTimeout()
	rf.lastHeard = time.Now()
	rf.state = RaftState_Follower
	rf.applyCh = applyCh
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCond = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
