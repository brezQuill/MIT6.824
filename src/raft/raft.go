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

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

// log中的记录
type Entry struct {
	Term    int
	Command interface{}
}

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[] -- 可以用做candidateId吗？
	dead      int32               // set by Kill()
	Stop      int32               //set by me

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role uint // 扮演的角色

	currentTerm int
	voteFor     int
	log         []Entry // 还没定义entry

	// server state
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// leader state
	nextIndex  []int // ##Reinitialized after election## for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	electionTimer  *time.Timer
	heartBeatTimer *time.Timer

	applyCh chan ApplyMsg
	cv      *sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.role == LEADER
	// Your code here (2A).
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

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.ElectionTimerReset()
	rf.ChangeToCandidate()
	DPrintf("%d : 启动选举, Term = %d", rf.me, rf.currentTerm)
	myInfo := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: len(rf.log) - 1, LastLogTerm: rf.log[len(rf.log)-1].Term}
	reply := RequestVoteReply{}

	numVotes := 1
	for server, _ := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(x int, myInfo RequestVoteArgs, reply RequestVoteReply) { // 要传局部变量，否则race
			success := rf.peers[x].Call("Raft.RequestVote", &myInfo, &reply)
			if !success {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.ChangeToFollower(reply.Term)
				rf.ElectionTimerReset()
				return
			}

			numVotes += reply.VoteGranted
			if numVotes > len(rf.peers)/2 && rf.role == CANDIDATE {
				rf.ChangeToLeader() // 内部停止选举计时器
				DPrintf("%d : 当选！, Term = %d", rf.me, rf.currentTerm)
				go rf.heartbeats(rf.me, rf.currentTerm)
				go rf.sendAppendEntries(rf.currentTerm) // 初始化nextIndex
			}
		}(server, myInfo, reply)
	}
}

// 随心跳传递当前commitIndex
func (rf *Raft) heartbeats(id int, term int) {
	args := AppendEntriesArgs{Term: term, LeaderId: id}

	for !rf.killed() && !rf.stop() {
		rf.mu.RLock()
		// DPrintf("%d : 发送心跳!!!", rf.me)
		if rf.role != LEADER {
			rf.mu.RUnlock()
			break
		}
		// 随心跳传递当前commitIndex
		args.LeaderCommit = rf.commitIndex
		args.PrevLogIndex = rf.commitIndex
		args.PrevLogTerm = rf.log[rf.commitIndex].Term
		//
		rf.mu.RUnlock()
		for server, _ := range rf.peers {
			if server == rf.me {
				continue
			}
			go func(x int, myInfo AppendEntriesArgs) {
				reply := AppendEntriesReply{}
				rf.peers[x].Call("Raft.AppendEntries", &myInfo, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					rf.ChangeToFollower(reply.Term)
					rf.ElectionTimerReset()
				}
			}(server, args)
		}
		<-rf.heartBeatTimer.C
		rf.HeartBeatTimerReset()
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.VoteGranted, reply.Term = 0, rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.ChangeToFollower(args.Term)
	}
	if rf.voteFor != -1 && rf.voteFor != args.CandidateId { // 在同一轮，但是票已经投了（可能是投给自己）
		reply.VoteGranted = 0
	} else if rf.log[len(rf.log)-1].Term > args.LastLogTerm ||
		(rf.log[len(rf.log)-1].Term == args.LastLogTerm && len(rf.log)-1 > args.LastLogIndex) {

		reply.VoteGranted = 0

	} else {
		// 投票，并更新voteFor
		reply.VoteGranted = 1
		rf.voteFor = args.CandidateId
		rf.ElectionTimerReset()
	}
	reply.Term = rf.currentTerm
}

/*
1. 来自以往任期的信息不做处理     ：直接回复currentTerm
2. 收到来自更新或者当前任期的信息 ：转变为FOLLOWER、停止心跳、重启选举倒计时
3. 判断preLogIndex是否存在
	a. 存在但preLogTerm不匹配 ：返回最初的ConflictIndex 及其 conflictTerm
	b. 存在并且preLogTerm匹配 ：
		1）若len(rf.log) <= args.PrevLogIndex + len(args.Entries)，直接用args.Entries覆盖本地对应log
		2）否则 ：逐个判断是否存在与args.Entries冲突的本地entry，存在冲突则往后entries全部截断后拼接上args.Entries，不存在冲突则舍弃args.Entries
		3）更新commitIndex ：rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))，因为Leader可能commit了更多
		4）当lastApplied < 更新后的commit ：唤醒ApplyToUser协程，执行提交
	c. 不存在preLogIndex ： reply.ConflictIndex = len(rf.log)，reply.ConflictTerm = 0
4. 重设选举计时器超时时间
5. 对心跳的处理和上面一样
*/

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.ChangeToFollower(args.Term)
		rf.ElectionTimerReset()
	}
	reply.Term = rf.currentTerm
	rf.role = FOLLOWER

	if len(rf.log) > args.PrevLogIndex {
		// args.PrevLogIndex处存在entry
		if rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
			// Term匹配成功
			// 截断？不能直接截断，网络会导致RPC乱序，可以更长的Entries（User command发送的晚）反而会先到达， 其返回后会更新nextLogIndex为一个大值，
			// 此时短的Entries（User command发送的早），会造成command被覆盖

			if len(rf.log) <= args.PrevLogIndex+len(args.Entries) {
				// 如果，本地log的尾巴没有新entries那么长，直接截断
				rf.log = rf.log[0 : args.PrevLogIndex+1]
				rf.log = append(rf.log, args.Entries...)
			} else {
				i := 0
				for i < len(args.Entries) && rf.log[args.PrevLogIndex+1+i].Term == args.Entries[i].Term {
					i++
				}
				if i != len(args.Entries) { // 原先写的是i != len(args.Entries)-1
					// 发现了冲突，截断！
					rf.log = rf.log[0 : args.PrevLogIndex+1+i]
					rf.log = append(rf.log, args.Entries[i:]...)
				}
			}

			reply.Success = true
			rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))

			if rf.lastApplied < rf.commitIndex {
				rf.cv.Signal()
				// DPrintf("%d : commitIndex = %d", rf.me, rf.commitIndex)
			}
		} else {
			// Term匹配失败
			reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
			i := args.PrevLogIndex
			for i > 0 && rf.log[i].Term == reply.ConflictTerm {
				i--
			}
			reply.ConflictIndex = i + 1
			reply.Success = false
		}
	} else {
		// args.PrevLogIndex处不存在entry
		reply.ConflictIndex = len(rf.log)
		// DPrintf("%d reply to %d, command = %d :args.PrevLogIndex = %d 处不存在entry, reply.ConflictIndex = %d, reply.Term = %d", rf.me, args.LeaderId, args.Entries[len(args.Entries)-1].Command.(int), args.PrevLogIndex, reply.ConflictIndex, reply.Term)
		reply.Success = false
	}
	rf.ElectionTimerReset()

}

func min(a int, b int) int {
	if a > b {
		return b
	}
	return a
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
/*
1. 传入currentTerm，防止任期中途改变，将新的任期封装到entries中
2. 当网络失败，一直重发，直到自己是不是LEADER或者任期发生了改变
3. 收到回复后检查：a. reply.Term > currentTerm --> changeToFollower
				 b. 检查自己是不是LEADER或者任期发生了改变
				 c. 如果preLogIndex和preLogTerm成功匹配 -- 判断是否有必要更新state
				 	1）检查nextIndex和matchIndex是否已经是“更新”的值，是则return，否则更新并判断是否需要满足commit条件
				 d. 如果preLogIndex和preLogTerm匹配失败 -- 重发
				 	1）preLogTerm冲突 ： 找到合适的nextIndex
					2) preLogIndex不存在 ：置nextIndex = conflictIndex
*/
// 重构一下：负责将传入的Entries广播出去？
func (rf *Raft) sendAppendEntries(Term int) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	// 调用这个函数可以顺便发送heartbeats，对heartbeats做一下处理，重新计时
	numMatched := 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(x int) {
			rf.mu.RLock()
			prevLogIndex := rf.nextIndex[x] - 1
			args := AppendEntriesArgs{Term, rf.me, prevLogIndex, rf.log[prevLogIndex].Term, rf.log[rf.nextIndex[x]:], rf.commitIndex}
			// DPrintf("%d   send    to %d, Term = %d: len(args.Entries) = %d, nextIndex = %v", rf.me, x, Term, len(args.Entries), rf.nextIndex)
			rf.mu.RUnlock()

			for !rf.killed() {
				reply := AppendEntriesReply{}
				// rf.mu.RLock()
				// DPrintf("%d   send    to %d, Term = %d: len(args.Entries) = %d, nextIndex = %v", rf.me, x, Term, len(args.Entries), rf.nextIndex)
				// rf.mu.RUnlock()
				success := rf.peers[x].Call("Raft.AppendEntries", &args, &reply)
				// rf.mu.RLock()
				// DPrintf("%d receive from %d, Term = %d: len(args.Entries) = %d, nextIndex = %v", rf.me, x, Term, len(args.Entries), rf.nextIndex)
				// rf.mu.RUnlock()
				// DPrintf("%d sent to %d, command = %d :args.PrevLogIndex = %d, Term = %d", rf.me, x, args.Entries[len(args.Entries)-1].Command, args.PrevLogIndex, args.Term)
				if !success {
					// 网络失败
					rf.mu.RLock()
					// DPrintf("%d   send    to %d, Term = %d: len(args.Entries) = %d, nextIndex = %v, 网络失败!!", rf.me, x, Term, len(args.Entries), rf.nextIndex)
					if rf.role != LEADER || rf.currentTerm != args.Term {
						rf.mu.RUnlock()
						return
					}
					// DPrintf("%d   send    to %d, Term = %d: len(args.Entries) = %d, nextIndex = %v, 网络失败,决定重发!!", rf.me, x, Term, len(args.Entries), rf.nextIndex)
					rf.mu.RUnlock()
					time.Sleep(10 * time.Millisecond)
					// return
				} else {
					// 网络成功
					rf.mu.Lock()

					if reply.Term > rf.currentTerm {
						// DPrintf("%d receive from %d, Term = %d: 回复中任期更新，丢弃并转换角色", rf.me, x, Term)
						rf.ChangeToFollower(reply.Term)
						rf.ElectionTimerReset()
						rf.mu.Unlock()
						return
					}
					// 当并发传送RPC时，其中某些reply可能已经改变了leader的nextIndex  ...
					// 需要检查收到回复时，自己还是否是leader; 或者任期发生了改变
					if rf.role != LEADER || rf.currentTerm != args.Term {
						// DPrintf("%d receive from %d, Term = %d: 任期或角色变化，丢弃", rf.me, x, Term)
						rf.mu.Unlock()
						return
					}

					if reply.Success {
						if rf.nextIndex[x] > args.PrevLogIndex+1+len(args.Entries) || rf.matchIndex[x] > args.PrevLogIndex+len(args.Entries) {
							// DPrintf("%d receive from %d, Term = %d: len(args.Entries) = %d, nextIndex = %v, 但是nextIndex和matchIndex都更新过了", rf.me, x, Term, len(args.Entries), rf.nextIndex)
							// 这里没有解锁....这个bug花了5个小时
							rf.mu.Unlock()
							return
						}
						rf.nextIndex[x] = args.PrevLogIndex + 1 + len(args.Entries) // 这里需要+1
						rf.matchIndex[x] = args.PrevLogIndex + len(args.Entries)
						// DPrintf("%d receive from %d, Term = %d: len(args.Entries) = %d, nextIndex = %v", rf.me, x, Term, len(args.Entries), rf.nextIndex)

						numMatched++

						// preCommit := rf.commitIndex // for debug
						if numMatched > len(rf.peers)/2 && rf.commitIndex < rf.matchIndex[x] {
							rf.commitIndex = rf.matchIndex[x]
							// DPrintf("%d : 唤醒", rf.me)
							rf.cv.Signal() // 唤醒，提交commit
							// DPrintf("%d : 结束唤醒", rf.me)
							// DPrintf("%d : commitIndex changed. pre: %d, now: %d, lastApply: %d", rf.me, preCommit, rf.commitIndex, rf.lastApplied)
						}

						rf.mu.Unlock()
						return
					} else if reply.ConflictTerm != 0 {
						// prevLogIndex处存在entry, 但是Term不对
						for prevLogIndex > 0 && (rf.log[prevLogIndex].Term != reply.ConflictTerm && prevLogIndex >= reply.ConflictIndex) {
							prevLogIndex--
						}
						// DPrintf("%d receive from %d, Term = %d: prevLogIndex = %d 处存在entry, 但是Term不对", rf.me, x, Term, args.PrevLogIndex)
					} else {
						// DPrintf("%d receive from %d, Term = %d: prevLogIndex = %d 处不存在entry", rf.me, x, Term, args.PrevLogIndex)
						// prevLogIndex处不存在entry
						// 当远端遇见一个往期RPC，会直接回复我们，此时reply.ConflictTerm == 0，reply.ConflictIndex = 0
						// 假如我们不丢弃往期RPC回复会执行以下代码，造成prevLogIndex = -1的超限错误。这个BUG花了老子4个小时
						prevLogIndex = reply.ConflictIndex - 1
						// DPrintf("%d sent to %d :args.PrevLogIndex = %d 处不存在entry, reply.ConflictIndex = %d", rf.me, x, args.PrevLogIndex, reply.ConflictIndex)
						// DPrintf("%d receive from %d, command = %d : reply.Term = %d, reply.ConflictIndex = %d", rf.me, x, args.Entries[len(args.Entries)-1].Command.(int), reply.Term, reply.ConflictIndex)
						// DPrintf("%d receive from %d, command = %v :args.PrevLogIndex = %d, args.Term = %d -- reply.Term = %d, reply.ConflictIndex = %d, reply.ConflictTerm = %d", rf.me, x, args.Entries[len(args.Entries)-1].Command, args.PrevLogIndex, args.Term, reply.Term, reply.ConflictIndex, reply.ConflictTerm)
					}
					rf.nextIndex[x] = prevLogIndex + 1
					args.PrevLogIndex = prevLogIndex
					args.Entries = rf.log[rf.nextIndex[x]:]
					args.PrevLogTerm = rf.log[prevLogIndex].Term
					args.LeaderCommit = rf.commitIndex

					rf.mu.Unlock()
				}

			} // 网络失败是否需要重发？
		}(i)
	}
	rf.HeartBeatTimerReset() // !!!!!!!!!!!
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
	rf.mu.RLock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.role == LEADER
	DPrintf("%d : currentTerm = %d, command = %v, nextIndex = %v", rf.me, term, command, rf.nextIndex)
	rf.mu.RUnlock()

	if isLeader {
		// DPrintf("%d : Start a command, Term = %d", rf.me, term)
		rf.mu.Lock()
		entry := Entry{Term: term, Command: command}
		rf.log = append(rf.log, entry)
		index = len(rf.log) - 1
		go rf.sendAppendEntries(term)
		rf.mu.Unlock()
		// 因为是解锁后传送RPC，需要确保传送的RPC携带的Term没有被改变
	}

	return index, term, isLeader
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() && !rf.stop() {
		// 不是leader尝试发起选举
		<-rf.electionTimer.C
		if !rf.electionTimer.Stop() {
			select {
			case <-rf.electionTimer.C:
			default:
			}
		}
		rf.electionTimer.Reset(randomizedElectionTimeout())
		rf.startElection()
	}

}

func (rf *Raft) ChangeToFollower(Term int) {
	rf.currentTerm = Term
	rf.role = FOLLOWER
	rf.voteFor = -1
	if !rf.heartBeatTimer.Stop() {
		select {
		case <-rf.heartBeatTimer.C:
		default:
		}
	}
}

func (rf *Raft) ChangeToCandidate() {
	rf.role = CANDIDATE
	rf.currentTerm++
	rf.voteFor = rf.me
}

func (rf *Raft) ChangeToLeader() {
	rf.role = LEADER
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
	rf.HeartBeatTimerReset()
	for server := range rf.peers {
		rf.nextIndex[server] = len(rf.log)
		rf.matchIndex[server] = 0
	}
}

func (rf *Raft) ElectionTimerReset() {
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
	rf.electionTimer.Reset(randomizedElectionTimeout())
}

func (rf *Raft) HeartBeatTimerReset() {
	if !rf.heartBeatTimer.Stop() {
		select {
		case <-rf.heartBeatTimer.C:
		default:
		}
	}
	rf.heartBeatTimer.Reset(heartBeatTimeout())
}

func (rf *Raft) ApplyToUser() {
	for !rf.killed() && !rf.stop() {
		// DPrintf("%d : before apply Lock", rf.me)
		rf.mu.Lock()
		// DPrintf("%d : after apply Lock", rf.me)
		for !(rf.lastApplied < rf.commitIndex) {
			rf.cv.Wait()
		}
		// DPrintf("%d : lastApplied = %d, commitIndex = %d", rf.me, rf.lastApplied, rf.commitIndex)
		for i := rf.lastApplied; i <= rf.commitIndex; i++ {
			rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i}
			rf.lastApplied = i
		}
		// DPrintf("%d : after apply", rf.me)
		// DPrintf("%d : lastApplied = %d", rf.me, rf.lastApplied)
		rf.mu.Unlock()
	}
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
	rf.applyCh = applyCh
	rf.cv = sync.NewCond(&rf.mu)
	rf.role = FOLLOWER
	rf.electionTimer = time.NewTimer(randomizedElectionTimeout())
	rf.heartBeatTimer = time.NewTimer(heartBeatTimeout())

	rf.dead = 0
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.log = make([]Entry, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.nextIndex[rf.me] = len(rf.log)
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.ApplyToUser()
	return rf
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

func (rf *Raft) set_stop() {
	time.Sleep(time.Second * 50)
	atomic.StoreInt32(&rf.Stop, 1)
}

func (rf *Raft) stop() bool {
	z := atomic.LoadInt32(&rf.Stop)
	return z == 1
}

func randomizedElectionTimeout() time.Duration {
	return time.Duration(time.Duration(200+rand.Intn(200)) * time.Millisecond)
}

func heartBeatTimeout() time.Duration {
	return time.Duration(120 * time.Millisecond)
}
