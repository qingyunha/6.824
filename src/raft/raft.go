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
	"labrpc"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

type state string

const (
	Follower  state = "follower"
	Candidate state = "candidate"
	Leader    state = "leaders"
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

type Entry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int

	state           state
	voteNum         int
	becomeLeader    chan bool
	electionTimeout *time.Timer

	applyCh chan ApplyMsg

	log         []Entry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

func (rf *Raft) follower2candidate() {
	// times out, starts election
	fmt.Printf("[%d] follower --> candidate\n", rf.me)
	if rf.votedFor != -1 {
		rf.currentTerm += 1
	}
	rf.state = Candidate
	// voteself
	rf.voteNum = 1
	rf.votedFor = rf.me
	fmt.Printf("[%d] start election state:%s term:%d\n", rf.me, rf.state, rf.currentTerm)
}

func (rf *Raft) candidate2leader() {
	// receives votes from majority of servers
	fmt.Printf("[%d] candidate --> Leader\n", rf.me)
	rf.becomeLeader <- true
	rf.state = Leader
	lastIndex := len(rf.log) - 1
	for i := range rf.peers {
		rf.nextIndex[i] = lastIndex + 1
		rf.matchIndex[i] = 0
	}
	fmt.Printf("[%d] now I'm the leader term:%d log:%v\n", rf.me, rf.currentTerm, rf.log[1:])
}

func (rf *Raft) leader2follower(term int) {
	// discovers servers with high term
	fmt.Printf("[%d] leader --> follower\n", rf.me)
	rf.currentTerm = term
	rf.voteNum = 0
	rf.votedFor = -1
	rf.state = Follower
}

func (rf *Raft) candidate2follower(term int) {
	// discovers current leader or new term
	fmt.Printf("[%d] candidate --> follower\n", rf.me)
	if term > rf.currentTerm {
		rf.voteNum = 0
		rf.votedFor = -1
	}
	rf.currentTerm = term
	rf.state = Follower
}

func (rf *Raft) candidate2candidate() {
	fmt.Printf("[%d] candidate --> candidate\n", rf.me)
	// times out, new election
	rf.currentTerm += 1
	// voteself
	rf.voteNum = 1
	rf.votedFor = rf.me
	fmt.Printf("[%d] start election state:%s term:%d\n", rf.me, rf.state, rf.currentTerm)
}

func (rf *Raft) toFollowerWithHighTerm(term int) {
	// discovers current leader or new term
	fmt.Printf("[%d] %s --> follower *\n", rf.me, rf.state)
	rf.voteNum = 0
	rf.votedFor = -1
	rf.currentTerm = term
	rf.state = Follower
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var msg string = "ok"
	if args.Term > rf.currentTerm {
		fmt.Printf("[%d]!!! get voterequest with HIGH TERM %d\n", rf.me, args.Term)
		rf.toFollowerWithHighTerm(args.Term)
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || rf.votedFor != -1 {
		if rf.votedFor != -1 {
			msg = fmt.Sprintf("already vote for %d", rf.votedFor)
		} else {
			msg = "low term"
		}
		reply.VoteGranted = false
	} else {
		lastIndex := len(rf.log) - 1
		lastTerm := rf.log[lastIndex].Term
		if args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			if rf.electionTimeout != nil {
				rf.electionTimeout.Reset(time.Duration(rand.Int31n(1000)+500) * time.Millisecond)
			}
		} else {
			if args.LastLogTerm < lastTerm {
				msg = "low last Term"
			}
			if args.LastLogIndex < lastIndex {
				msg = "low last index"
			}
		}
	}
	fmt.Printf("[%d]--- votefor %d. %v %s\n", rf.me, args.CandidateId, reply.VoteGranted, msg)
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

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int

	Entries []Entry

	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.toFollowerWithHighTerm(args.Term)
		fmt.Printf("[%d] get AppendEntries with HIGH TERM %d\n", rf.me, args.Term)
	} else if args.Term == rf.currentTerm {
		if rf.electionTimeout != nil {
			rf.electionTimeout.Reset(time.Duration(rand.Int31n(1000)+500) * time.Millisecond)
		}
		if rf.state == Candidate {
			rf.candidate2follower(rf.currentTerm)
		}
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	lastIndex := len(rf.log) - 1
	if lastIndex < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}
	reply.Success = true
	if len(args.Entries) > 0 {
		//fmt.Printf("[%d] > commitIndex:%d, log:%v\n    prevlogindex:%d, new:%v\n", rf.me, rf.commitIndex, rf.log[1:], args.PrevLogIndex, args.Entries)
	}
	leaderLastIndex := args.PrevLogIndex + len(args.Entries)
	if lastIndex < leaderLastIndex {
		var i int
		for i = args.PrevLogIndex + 1; i <= lastIndex; i++ {
			if rf.log[i].Term != args.Entries[i-args.PrevLogIndex-1].Term {
				rf.log = rf.log[:i]
				break
			}
		}
		for ; i <= leaderLastIndex; i++ {
			rf.log = append(rf.log, args.Entries[i-args.PrevLogIndex-1])
		}
	} else {
		var i int
		for i = args.PrevLogIndex + 1; i <= leaderLastIndex; i++ {
			rf.log[i] = args.Entries[i-args.PrevLogIndex-1]
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		lastCommitIndex := rf.commitIndex
		if lastIndex < args.LeaderCommit {
			rf.commitIndex = lastIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		//if lastCommitIndex < rf.commitIndex {
		//fmt.Printf("[%d] apply from reqest:%+v lastCommitIndex:%d commitIndex:%d log:%v\n", rf.me, args, lastCommitIndex, rf.commitIndex, rf.log[1:])
		//}
		go func(start, end int) {
			for i, e := range rf.log[start+1 : end+1] {
				msg := ApplyMsg{true, e.Command, lastCommitIndex + 1 + i}
				//fmt.Printf("[%d] send ApplyMsg %+v\n", rf.me, msg)
				rf.applyCh <- msg
			}
		}(lastCommitIndex, rf.commitIndex)
	}
	if len(args.Entries) > 0 {
		//fmt.Printf("[%d] < commitIndex:%d, log:%v\n", rf.me, rf.commitIndex, rf.log[1:])
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	term, isLeader = rf.GetState()
	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}
	entry := Entry{term, command}
	rf.log = append(rf.log, entry)
	index = len(rf.log) - 1
	//fmt.Printf("[%d] Start command:%v index:%d, term:%d, log:%v, nextIndex:%v\n", rf.me, command, index, term, rf.log[1:], rf.nextIndex)
	rf.mu.Unlock()
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

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.votedFor = -1
	rf.currentTerm = 1

	rf.log = []Entry{Entry{}}
	rf.applyCh = applyCh
	rf.becomeLeader = make(chan bool)

	for _ = range peers {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	go func() {
		//election periodically
		for {
			timeout := time.Duration(rand.Int31n(1000)+500) * time.Millisecond
			rf.electionTimeout = time.NewTimer(timeout)
			<-rf.electionTimeout.C
			if !rf.electionTimeout.Stop() {
				rf.mu.Lock()
				if rf.state == Follower {
					rf.follower2candidate()
				} else if rf.state == Candidate {
					rf.candidate2candidate()
				} else {
					// leader pass electionTimeout
					rf.mu.Unlock()
					continue
				}
				args := &RequestVoteArgs{}
				args.Term = rf.currentTerm
				args.CandidateId = rf.me
				args.LastLogIndex = len(rf.log) - 1
				args.LastLogTerm = rf.log[args.LastLogIndex].Term
				rf.mu.Unlock()

				wg := sync.WaitGroup{}
				for i := range rf.peers {
					if i == me {
						continue
					}
					wg.Add(1)
					go func(i int, args *RequestVoteArgs) {
						//fmt.Printf("[%d]*** send vote to %d\n", me, i)
						reply := &RequestVoteReply{}
						result := make(chan bool)
						timeout := make(chan bool)
						go func() {
							result <- rf.sendRequestVote(i, args, reply)
						}()
						go func() {
							time.Sleep(500 * time.Millisecond)
							timeout <- true
						}()
						select {
						case <-result:
							rf.mu.Lock()
							if args.Term != rf.currentTerm {
								rf.mu.Unlock()
								fmt.Printf("[%d]*** discovers new term:%d oldterm:%d. current state: %s\n", me, rf.currentTerm, args.Term, rf.state)
								break
							}
							if rf.state != Candidate {
								rf.mu.Unlock()
								fmt.Printf("[%d]*** not in Candidate state. term:%d state:%s \n", me, rf.currentTerm, rf.state)
								break
							}

							if reply.Term > rf.currentTerm {
								rf.candidate2follower(reply.Term)
							} else if reply.Term == rf.currentTerm {
								if reply.VoteGranted {
									rf.voteNum += 1
									if rf.voteNum > len(rf.peers)/2 {
										rf.candidate2leader()
									}
								}
							}
							rf.mu.Unlock()
						case <-timeout:
							//fmt.Printf("[%d]*** wait %d vote reply timeout\n", me, i)
						}
						wg.Done()
					}(i, args)
				}
				wg.Wait()
				rf.mu.Lock()
				if rf.state == Candidate {
					rf.votedFor = -1
				}
				rf.mu.Unlock()
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			if _, isLeader := rf.GetState(); isLeader {
				for i := range rf.peers {
					if i == rf.me {
						continue
					}
					go func(i int) {
						rf.mu.Lock()
						index := len(rf.log) - 1
						args := &AppendEntriesArgs{}
						args.Term = rf.currentTerm
						args.LeaderId = rf.me
						args.LeaderCommit = rf.commitIndex
						args.PrevLogIndex = rf.nextIndex[i] - 1
						args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
						args.Entries = rf.log[rf.nextIndex[i]:]
						rf.mu.Unlock()
						reply := &AppendEntriesReply{}
						if len(args.Entries) > 0 {
							//fmt.Printf("[%d] (index:%d) replicate log to %d %+v\n", rf.me, index, i, args)
						}
						if rf.sendAppendEntries(i, args, reply) {
							rf.mu.Lock()
							if reply.Term > rf.currentTerm {
								fmt.Printf("[%d] (index:%d) send log. got HIGH TERM %d\n", rf.me, index, reply.Term)
								rf.toFollowerWithHighTerm(reply.Term)
							}
							if rf.state != Leader {
								rf.mu.Unlock()
								return
							}
							if reply.Success {
								if index >= rf.nextIndex[i] {
									rf.nextIndex[i] = index + 1
								}
								if index >= rf.matchIndex[i] {
									rf.matchIndex[i] = index
								}
								//fmt.Printf("[%d] replicate log to %d success. index:%d, nextIndex:%d, matchIndex:%d\n", rf.me, i, index, rf.nextIndex[i], rf.matchIndex[i])
							} else {
								if rf.nextIndex[i] > 1 && args.PrevLogIndex == rf.nextIndex[i]-1 {
									rf.nextIndex[i] = args.PrevLogIndex / 2
								}
								fmt.Printf("[%d] (index:%d) send to %d failed. retry nextIndex:%d\n", rf.me, index, i, rf.nextIndex[i])
							}
							rf.mu.Unlock()
						}
					}(i)
				}

				lastCommitIndex := rf.commitIndex
				rf.matchIndex[rf.me] = len(rf.log) - 1
				sorted := make([]int, len(rf.matchIndex))
				copy(sorted, rf.matchIndex)
				sort.Sort(sort.Reverse(sort.IntSlice(sorted)))
				for i, m := range sorted {
					if i+1 > len(rf.peers)/2 {
						if m > rf.commitIndex {
							rf.commitIndex = m
						}
						break
					}
					if m <= rf.commitIndex {
						break
					}
				}
				if rf.commitIndex > lastCommitIndex {
					go func(before, end int) {
						for i, e := range rf.log[before+1 : end+1] {
							msg := ApplyMsg{true, e.Command, lastCommitIndex + 1 + i}
							//fmt.Printf("[%d] send ApplyMsg %+v\n", rf.me, msg)
							rf.applyCh <- msg
						}
					}(lastCommitIndex, rf.commitIndex)
				}
				time.Sleep(300 * time.Millisecond)
			} else {
				<-rf.becomeLeader
			}
		}
	}()
	return rf
}
