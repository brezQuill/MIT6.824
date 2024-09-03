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
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
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
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role uint // 扮演的角色

	currentTerm int
	voteFor     int
	log         []Entry

	// server state
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// leader state
	nextIndex  []int // ##Reinitialized after election## for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	electionTimer  *time.Timer
	heartBeatTimer *time.Timer
	applyCh        chan ApplyMsg
	cv             *sync.Cond
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
	start := time.Now()
	rf.mu.RLock()
	Term := rf.currentTerm
	voteFor := rf.voteFor
	Log := DeepCopyEntries(rf.log)
	rf.mu.RUnlock()
	TPrintf("%d## : persist花费时间=%v", rf.me, time.Since(start).Milliseconds())

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(Term)
	e.Encode(voteFor)
	e.Encode(Log)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var Log []Entry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&Log) != nil {
		log.Fatalf("Failed to decode persistent state")
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.log = Log
	}
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
	OutDate     bool
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

// 随心跳传递当前commitIndex
func (rf *Raft) heartbeats() {
	for !rf.killed() {

		rf.mu.RLock()
		if rf.role == LEADER {
			args := AppendEntriesArgs{Term: rf.currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
				PrevLogIndex: rf.commitIndex,
				PrevLogTerm:  rf.log[rf.commitIndex].Term}

			rf.mu.RUnlock()
			for server, _ := range rf.peers {
				if server == rf.me {
					continue
				}
				go func(x int, myInfo AppendEntriesArgs) {
					reply := AppendEntriesReply{}
					rf.peers[x].Call("Raft.AppendEntries", &myInfo, &reply)
					if !reply.Success && reply.ConflictTerm == 0 {
						DPrintf("%d## : 副作用心跳", rf.me)
					} else {
						DPrintf("%d## : 正常心跳", rf.me)
					}
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.currentTerm { //
						if rf.role == LEADER {
							rf.StopHeartBeat()
							rf.ElectionTimerReset() //从leader转为follower？？？？
						}
						rf.ChangeToFollower(reply.Term)
						go rf.persist()
					}
				}(server, args)
			}
		} else {
			rf.mu.RUnlock()
		}
		<-rf.heartBeatTimer.C
		rf.HeartBeatTimerReset()
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("%d## reply to %d : RequestVote开始", rf.me, args.CandidateId)
	defer DPrintf("%d## reply to %d : RequestVote结束", rf.me, args.CandidateId)
	// Your code here (2A, 2B).
	start := time.Now()
	rf.mu.Lock()
	TPrintf("%d## : RequestVote 抢锁花费时间=%v", rf.me, time.Since(start).Milliseconds())
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.VoteGranted, reply.Term = 0, rf.currentTerm
		TPrintf("%d## : old vote, 花费时间=%v", rf.me, time.Since(start).Milliseconds())
		return
	}

	if args.Term > rf.currentTerm {
		rf.ChangeToFollower(args.Term)
		rf.StopHeartBeat()
		// rf.ElectionTimerReset() // ?可能来自一个自嗨的孤儿server
	}

	isUpToDate := rf.isUpToDate(args)
	if isUpToDate {
		reply.OutDate = true // 告诉对方你过时了
	} else if (rf.voteFor == rf.me && rf.isOutDate(args) && rf.role != LEADER) ||
		((rf.voteFor == -1 || rf.voteFor == args.CandidateId) && !isUpToDate) {
		// 同期候选选手, 但是人家更新 || 未投票且log不比人家新
		rf.role = FOLLOWER
		rf.voteFor = args.CandidateId
		reply.VoteGranted = 1
		rf.ElectionTimerReset()
	}

	reply.Term = rf.currentTerm
	go rf.persist()
	TPrintf("%d## : RequestVote 花费时间=%v", rf.me, time.Since(start).Milliseconds())
}

func (rf *Raft) isUpToDate(args *RequestVoteArgs) bool {
	return rf.log[len(rf.log)-1].Term > args.LastLogTerm ||
		(rf.log[len(rf.log)-1].Term == args.LastLogTerm && len(rf.log)-1 > args.LastLogIndex)
}

func (rf *Raft) isOutDate(args *RequestVoteArgs) bool {
	return rf.log[len(rf.log)-1].Term < args.LastLogTerm ||
		(rf.log[len(rf.log)-1].Term == args.LastLogTerm && len(rf.log)-1 < args.LastLogIndex)
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
	CPrintf("%d## reply to %d : AppendEntries开始", rf.me, args.LeaderId)
	defer CPrintf("%d## reply to %d : AppendEntries结束", rf.me, args.LeaderId)
	start := time.Now()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		TPrintf("%d## : old entries, 花费时间=%v", rf.me, time.Since(start).Milliseconds())
		return
	} // 这个区域用读锁说不定有问题

	if args.Term > rf.currentTerm {
		KPrintf("%d## : 退位让贤", rf.me)
		rf.ChangeToFollower(args.Term)
		rf.StopHeartBeat()
		go rf.persist()
	}
	// 收到同期AppendPRC的时候，自己不可能是Leader，但可能是Candidate，不过没关系，因为同期server的票都投出去了
	rf.ElectionTimerReset()
	if len(rf.log) > args.PrevLogIndex {
		// args.PrevLogIndex处存在entry
		if rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
			// Term匹配成功
			reply.Success = true
			rf.updateLocalLog(args)
			temp := min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
			rf.commitIndex = max(temp, rf.commitIndex)
			if rf.lastApplied < rf.commitIndex {
				rf.cv.Signal()
			}
		} else {
			// Term匹配失败
			reply.Success = false
			reply.ConflictTerm = rf.log[args.PrevLogIndex].Term

			i := args.PrevLogIndex
			for i > 0 && rf.log[i].Term == reply.ConflictTerm {
				i--
			}
			reply.ConflictIndex = i + 1
		}
	} else {
		// args.PrevLogIndex处不存在entry
		reply.Success = false
		reply.ConflictIndex = len(rf.log)
	}
	reply.Term = rf.currentTerm
	TPrintf("%d## : AppendEntries 花费时间=%v", rf.me, time.Since(start).Milliseconds())
}

/*
截断？不能直接截断本地Log，网络会导致RPC乱序，可以更长的Entries（User command发送的晚）反而会先到达， 其返回后会更新nextLogIndex为一个大值，
此时短的Entries（User command发送的早），会造成command被覆盖
*/
func (rf *Raft) updateLocalLog(args *AppendEntriesArgs) {
	if len(rf.log) <= args.PrevLogIndex+1+len(args.Entries) {
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
	go rf.persist()
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
func (rf *Raft) sendAppendEntries(Term int, LogLen int) {
	DPrintf("%d##--Term=%d : sendAppendEntries开始", rf.me, Term)
	defer DPrintf("%d##--Term=%d : sendAppendEntries结束", rf.me, Term)
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	numMatched := 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(x int) {
			DPrintf("%d## send to %d--Term=%d : sendAppend开始", rf.me, x, Term)
			defer DPrintf("%d## send to %d--Term=%d : sendAppend结束", rf.me, x, Term)
			rf.mu.RLock()
			prevLogIndex := rf.nextIndex[x] - 1
			entries := DeepCopyEntries(rf.log[rf.nextIndex[x]:])
			// entries := rf.log[rf.nextIndex[x]:]
			args := AppendEntriesArgs{Term, rf.me, prevLogIndex, rf.log[prevLogIndex].Term, entries, rf.commitIndex}
			rf.mu.RUnlock()

			for !rf.killed() {
				reply := AppendEntriesReply{} // 每次重发都要置空，否则gob解码会报错，因为接收远程数据的容器（这里是reply）不为空

				success := rf.peers[x].Call("Raft.AppendEntries", &args, &reply)

				if !success {
					// 网络失败
					rf.mu.RLock()
					if rf.role != LEADER || rf.currentTerm != Term || LogLen < len(rf.log) {
						rf.mu.RUnlock()
						return
					}
					rf.mu.RUnlock()
					time.Sleep(10 * time.Millisecond) // 休眠一会儿再重发
				} else {
					// 网络成功
					rf.mu.Lock()

					if reply.Term > rf.currentTerm {
						if rf.role == LEADER {
							rf.StopHeartBeat()
							rf.ElectionTimerReset() // 从Leader状态退位，需要重启选举计时
						}
						rf.ChangeToFollower(reply.Term)
						go rf.persist()
						rf.mu.Unlock()
						return
					}
					// 需要检查收到回复时，自己还是否是leader; 或者任期发生了改变
					if rf.role != LEADER || rf.currentTerm != args.Term {
						rf.mu.Unlock()
						return
					}

					if reply.Success {
						numMatched++
						nextIndex := args.PrevLogIndex + 1 + len(args.Entries)
						matchIndex := args.PrevLogIndex + len(args.Entries)
						if rf.nextIndex[x] > nextIndex || rf.matchIndex[x] > matchIndex {
							// 当并发传送RPC时，其中某些reply超车，并且已经更新了leader的nextIndex，直接返回
							rf.mu.Unlock() // 这里没有解锁....这个bug花了5个小时
							return
						}
						rf.nextIndex[x] = nextIndex
						rf.matchIndex[x] = matchIndex
						CPrintf("%d## : nextIndex=%v, matchIndex=%v, commitIndex=%d", rf.me, rf.nextIndex, rf.matchIndex, rf.commitIndex)

						if numMatched > len(rf.peers)/2 &&
							rf.commitIndex < rf.matchIndex[x] &&
							rf.currentTerm == rf.log[LogLen-1].Term {
							// 大部分服务器同步成功，并且还存在未apply的条目
							rf.commitIndex = rf.matchIndex[x]
							rf.cv.Signal() // 唤醒，提交commit
						}

						rf.mu.Unlock()
						return
					} else if reply.ConflictTerm != 0 {
						// prevLogIndex处存在entry, 但是Term不对
						for prevLogIndex > 0 && (rf.log[prevLogIndex].Term != reply.ConflictTerm && prevLogIndex >= reply.ConflictIndex) {
							prevLogIndex--
						}
					} else {
						// prevLogIndex处不存在entry
						// 当远端遇见一个往期RPC，会直接回复我们，此时reply.ConflictTerm == 0，reply.ConflictIndex = 0
						// 假如我们不丢弃往期RPC回复会执行以下代码，造成prevLogIndex = -1的超限错误。这个BUG花了老子4个小时
						prevLogIndex = reply.ConflictIndex - 1
					}
					rf.nextIndex[x] = prevLogIndex + 1
					args.PrevLogIndex = prevLogIndex
					args.Entries = rf.log[rf.nextIndex[x]:]
					args.PrevLogTerm = rf.log[prevLogIndex].Term
					args.LeaderCommit = rf.commitIndex

					rf.mu.Unlock()
				}

			} // for !rf.killed() {
		}(i)
	}
	rf.HeartBeatTimerReset()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.ElectionTimerReset()
	rf.ChangeToCandidate()

	KPrintf("%d## : 选举开始, Term = %d", rf.me, rf.currentTerm)
	defer KPrintf("%d## : 选举结束, Term = %d", rf.me, rf.currentTerm)

	myInfo := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: len(rf.log) - 1, LastLogTerm: rf.log[len(rf.log)-1].Term}

	recvFrom := make([]int, 0)
	numVotes := 1
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(x int, myInfo RequestVoteArgs) { // 要传局部变量，否则race
			DPrintf("%d## send to %d: 拉票开始", rf.me, x)
			defer DPrintf("%d## send to %d : 拉票结束", rf.me, x)
			for !rf.killed() {
				reply := RequestVoteReply{}
				success := rf.peers[x].Call("Raft.RequestVote", &myInfo, &reply)
				if !success {
					rf.mu.RLock()
					if rf.currentTerm != myInfo.Term || rf.role != CANDIDATE || numVotes > len(rf.peers)/2 {
						// 任期改变 || 不再是候选者 || 票数已满足
						rf.mu.RUnlock()
						return
					}
					rf.mu.RUnlock()
					time.Sleep(10 * time.Millisecond)
				} else {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if reply.OutDate {
						rf.ElectionTimerReset(100) // 过时重设
					}

					if reply.Term > rf.currentTerm {
						rf.ChangeToFollower(reply.Term)
						rf.StopHeartBeat()
						go rf.persist()
						// rf.ElectionTimerReset() // ? 不要重置，回复可能来自一个被隔离的自嗨server
						return
					}

					if reply.VoteGranted == 1 {
						recvFrom = append(recvFrom, x)
					}

					numVotes += reply.VoteGranted
					if numVotes > len(rf.peers)/2 && rf.role == CANDIDATE && rf.currentTerm == myInfo.Term {
						rf.ChangeToLeader() // 内部停止选举计时器
						KPrintf("%d## : 当选！, Term = %d, 票来自：%v", rf.me, rf.currentTerm, recvFrom)
						go rf.sendAppendEntries(rf.currentTerm, len(rf.log)) // 初始化nextIndex
					}
					return
				}
			}

		}(server, myInfo)
	}
}

// 利用反射进行深拷贝
func DeepCopyEntry(entry Entry) Entry {
	newEntry := Entry{Term: entry.Term}

	// 直接复制 Value 字段
	if entry.Command != nil {
		// 使用反射获取实际值
		value := reflect.ValueOf(entry.Command)

		switch value.Kind() {
		case reflect.Int:
			// 对于 int 类型，直接赋值即可
			newEntry.Command = value.Interface()
		case reflect.Ptr:
			// 对于指针类型，进行深拷贝
			if !value.IsNil() {
				newValue := reflect.New(value.Elem().Type()) // 创建新的实例
				newValue.Elem().Set(value.Elem())            // 复制值
				newEntry.Command = newValue.Interface()      // 赋值
			}
		case reflect.Slice:
			// 对于切片类型，进行深拷贝
			newSlice := reflect.MakeSlice(value.Type(), value.Len(), value.Cap())
			reflect.Copy(newSlice, value)
			newEntry.Command = newSlice.Interface()
		case reflect.String:
			// 对于字符串，直接赋值即可
			newEntry.Command = value.Interface()
		default:
			// 处理其他类型
			newEntry.Command = value.Interface()
		}
	}

	return newEntry
}

// DeepCopyEntries deep copies a slice of Entry structs.
func DeepCopyEntries(entries []Entry) []Entry {
	newEntries := make([]Entry, len(entries))
	for i, entry := range entries {
		newEntries[i] = DeepCopyEntry(entry)
	}
	return newEntries
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
	// DPrintf("%d## : Start开始", rf.me)
	// defer DPrintf("%d## : Start结束", rf.me)
	// DPrintf("%d## : currentTerm = %d, command = %v, nextIndex = %v", rf.me, term, command, rf.nextIndex)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.role == LEADER
	if isLeader {
		rf.log = append(rf.log, Entry{Term: term, Command: command})
		go rf.persist()
		index = len(rf.log) - 1
		go rf.sendAppendEntries(term, index+1)
	}
	return index, term, isLeader
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// 不是leader尝试发起选举
		<-rf.electionTimer.C
		go rf.startElection()
	}

}

func (rf *Raft) ApplyToUser() {
	for !rf.killed() {
		rf.mu.Lock()
		for !(rf.lastApplied < rf.commitIndex) {
			rf.cv.Wait()
		}
		CPrintf("%d## : rf.lastApplied=%d, rf.commitIndex=%d, len(log)=%d", rf.me, rf.lastApplied, rf.commitIndex, len(rf.log))
		rf.mu.Unlock()
		for {
			rf.mu.Lock()
			if rf.lastApplied >= rf.commitIndex {
				rf.mu.Unlock()
				break
			}

			rf.lastApplied++
			// CPrintf("%d## : rf.lastApplied=%d, rf.commitIndex=%d, len(log)=%d", rf.me, rf.lastApplied, rf.commitIndex, len(rf.log))
			msg := ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied].Command, CommandIndex: rf.lastApplied}
			rf.mu.Unlock()
			rf.applyCh <- msg
		}
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
	rf.electionTimer = time.NewTimer(rf.randomizedElectionTimeout())
	rf.heartBeatTimer = time.NewTimer(heartBeatTimeout()) // 创建时就启动倒计时

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
	go rf.heartbeats()
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

func (rf *Raft) randomizedElectionTimeout(postpone ...int) time.Duration {
	if len(postpone) == 1 {
		return time.Duration(time.Duration(postpone[0]+130+rf.me*20+rand.Intn(20)) * time.Millisecond)
	}
	return time.Duration(time.Duration(130+rf.me*20+rand.Intn(20)) * time.Millisecond)
}

func heartBeatTimeout() time.Duration {
	return time.Duration(110 * time.Millisecond)
}

func (rf *Raft) ChangeToFollower(Term int) {
	rf.currentTerm = Term
	rf.role = FOLLOWER
	rf.voteFor = -1
}

func (rf *Raft) StopHeartBeat() {
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
	go rf.persist()
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

func (rf *Raft) ElectionTimerReset(postpone ...int) {
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
	if len(postpone) == 1 {
		rf.electionTimer.Reset(rf.randomizedElectionTimeout(postpone[0]))
	}
	rf.electionTimer.Reset(rf.randomizedElectionTimeout())
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

func min(a int, b int) int {
	if a > b {
		return b
	}
	return a
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
