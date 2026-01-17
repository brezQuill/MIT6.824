package raft

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
	Index   int
}

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

type InstallSnapshotArg struct {
	Term              int
	LeaderId          int
	Snapshot          []byte
	LastIncludedIndex int
	LastIncludedTerm  int
}

type InstallSnapshotReply struct {
	Term int
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

type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[] -- 可以用做candidateId吗？
	dead      int32               // set by Kill()

	role uint // 扮演的角色

	currentTerm int
	voteFor     int
	log         []Entry

	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	nextIndex  []int // ##Reinitialized after election## for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	electionTime  atomic.Value
	heartBeatTime atomic.Value
	applyCh       chan ApplyMsg
	applyCv       *sync.Cond

	replicatorCV []*sync.Cond
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	term := rf.currentTerm
	isleader := rf.role == LEADER
	// Your code here (2A).
	return term, isleader
}

func (rf *Raft) persist() {
	start := time.Now()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	TPrintf("%d## : persist花费时间=%v", rf.me, time.Since(start).Milliseconds())
}

func (rf *Raft) raftState() []byte {
	start := time.Now()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)
	data := w.Bytes()
	TPrintf("%d## : raftState花费时间=%v", rf.me, time.Since(start).Milliseconds())
	return data
}

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
	rf.lastApplied, rf.commitIndex = rf.firstEntry().Index, rf.firstEntry().Index
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	DPrintf("%d## : Snapshot开始", rf.me)
	defer DPrintf("%d## : Snapshot结束", rf.me)
	rf.mu.Lock()
	DPrintf("%d## : Snapshot抢到锁", rf.me)
	defer rf.mu.Unlock()
	snapshotIndex := rf.firstEntry().Index
	if index <= snapshotIndex {
		return
	}
	rf.log = shrinkEntriesArray(rf.log[index-snapshotIndex:])
	rf.log[0].Command = nil
	rf.persister.SaveStateAndSnapshot(rf.raftState(), snapshot)
}

func shrinkEntriesArray(entries []Entry) []Entry {
	newEntries := make([]Entry, len(entries))
	copy(newEntries, entries)
	return newEntries
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	DPrintf("%d## : CondInstallSnapshot开始", rf.me)
	defer DPrintf("%d## : CondInstallSnapshot结束", rf.me)

	rf.mu.Lock()
	DPrintf("%d## : CondInstallSnapshot抢到锁", rf.me)
	defer rf.mu.Unlock()

	// outdated snapshot
	if lastIncludedIndex <= rf.commitIndex {
		return false
	}

	if lastIncludedIndex > rf.lastEntry().Index {
		rf.log = make([]Entry, 1)
	} else {
		rf.log = shrinkEntriesArray(rf.log[lastIncludedIndex-rf.firstEntry().Index:])
		rf.log[0].Command = nil
	}
	// update dummy entry with lastIncludedTerm and lastIncludedIndex
	rf.log[0].Term, rf.log[0].Index = lastIncludedTerm, lastIncludedIndex
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex

	rf.persister.SaveStateAndSnapshot(rf.raftState(), snapshot)
	return true
}

func (rf *Raft) InstallSnapshot(request *InstallSnapshotArg, response *InstallSnapshotReply) {
	DPrintf("%d## : InstallSnapshot开始", rf.me)
	defer DPrintf("%d## : InstallSnapshot结束", rf.me)
	rf.mu.Lock()
	DPrintf("%d## : InstallSnapshot抢到锁", rf.me)
	defer rf.mu.Unlock()

	response.Term = rf.currentTerm

	if request.Term < rf.currentTerm {
		return
	}

	if request.Term > rf.currentTerm {
		rf.ChangeToFollower(request.Term)
		rf.persist()
	}

	rf.ElectionTimerReset()

	// outdated snapshot
	if request.LastIncludedIndex <= rf.commitIndex {
		return
	}

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      request.Snapshot,
			SnapshotTerm:  request.LastIncludedTerm,
			SnapshotIndex: request.LastIncludedIndex,
		}
	}()
}

//
// example RequestVote RPC handler.
//

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
	DPrintf("%d## reply to %d : AppendEntries开始", rf.me, args.LeaderId)
	defer DPrintf("%d## reply to %d : AppendEntries结束", rf.me, args.LeaderId)
	start := time.Now()
	rf.mu.Lock()
	TPrintf("%d## : 抢到锁，花费时间=%v", rf.me, time.Since(start).Milliseconds())
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	if args.Term > rf.currentTerm {
		rf.ChangeToFollower(args.Term)
		rf.persist()
	}
	rf.ElectionTimerReset()

	if args.PrevLogIndex < rf.firstEntry().Index {
		reply.Term, reply.Success = 0, false
		CPrintf("%d## : 异常情况--args.PrevLogIndex(%d) < rf.firstEntry().Index(%d)", rf.me, args.PrevLogIndex, rf.firstEntry().Index)
		return
	}

	firstIndex := rf.firstEntry().Index
	if rf.lastEntry().Index >= args.PrevLogIndex {
		// args.PrevLogIndex处存在entry
		if rf.log[args.PrevLogIndex-firstIndex].Term == args.PrevLogTerm {
			// Term匹配成功
			reply.Success = true
			rf.updateLocalLog(args)
			temp := min(args.LeaderCommit, rf.log[args.PrevLogIndex-firstIndex].Index+len(args.Entries))
			rf.commitIndex = max(temp, rf.commitIndex)
			if rf.lastApplied < rf.commitIndex {
				rf.applyCv.Signal()
			}
		} else {
			// Term匹配失败
			reply.Success = false
			reply.ConflictTerm = rf.log[args.PrevLogIndex-firstIndex].Term

			index := args.PrevLogIndex - firstIndex
			for index > 0 && rf.log[index].Term == reply.ConflictTerm {
				index--
			}
			reply.ConflictIndex = index + 1
		}
	} else {
		// args.PrevLogIndex处不存在entry
		reply.Success = false
		reply.ConflictIndex = rf.lastEntry().Index + 1
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) AppendEntriesReplyHandler(peer int, arg *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Term > rf.currentTerm {
		rf.ElectionTimerReset()
		rf.ChangeToFollower(reply.Term)
		rf.persist()
		return
	}
	// 需要检查收到回复时，自己还是否是leader; 或者任期发生了改变
	if rf.role != LEADER || rf.currentTerm != arg.Term {
		return
	}

	if reply.Success {
		nextIndex, matchIndex := arg.PrevLogIndex+1+len(arg.Entries), arg.PrevLogIndex+len(arg.Entries)

		rf.nextIndex[peer], rf.matchIndex[peer] = max(nextIndex, rf.nextIndex[peer]), max(matchIndex, rf.matchIndex[peer])
		if rf.majorityAgree(matchIndex) && len(arg.Entries) > 0 && rf.currentTerm == arg.Entries[len(arg.Entries)-1].Term {
			rf.commitIndex = matchIndex
			if rf.lastApplied < rf.commitIndex {
				// 大部分服务器同步成功，并且还存在未apply的条目
				rf.applyCv.Signal() // 唤醒，提交commit
			}
		}
	} else {
		rf.nextIndex[peer] = reply.ConflictIndex
	}
}

func (rf *Raft) InstallSnapshotReplyHandler(peer int, arg *InstallSnapshotArg, reply *InstallSnapshotReply) {
	if reply.Term > rf.currentTerm {
		rf.ElectionTimerReset()
		rf.ChangeToFollower(reply.Term)
		rf.persist()
		return
	}
	rf.matchIndex[peer] = arg.LastIncludedIndex
	rf.nextIndex[peer] = rf.matchIndex[peer] + 1
}

func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.RLock()
	if rf.role != LEADER {
		rf.mu.RUnlock()
		return
	}
	preLogIndex := rf.nextIndex[peer] - 1
	if preLogIndex < rf.firstEntry().Index {
		// send snapshot
		arg, reply := rf.genSnapshotArg(), new(InstallSnapshotReply)
		rf.mu.RUnlock()
		if rf.peers[peer].Call("Raft.InstallSnapshot", arg, reply) {
			rf.mu.Lock()
			rf.InstallSnapshotReplyHandler(peer, arg, reply)
			rf.mu.Unlock()
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	} else {
		// send entries
		arg, reply := rf.genAppendArg(preLogIndex), new(AppendEntriesReply)
		rf.mu.RUnlock()
		if rf.peers[peer].Call("Raft.AppendEntries", arg, reply) {
			rf.mu.Lock()
			rf.AppendEntriesReplyHandler(peer, arg, reply)
			rf.mu.Unlock()
		} else {
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCV[peer].L.Lock()
	defer rf.replicatorCV[peer].L.Unlock()

	for !rf.killed() {
		for !rf.needSendEntries(peer) {
			rf.replicatorCV[peer].Wait()
		}
		rf.replicateOneRound(peer)
	}
}

func (rf *Raft) BroadCastHeartBeat(isHeartBeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartBeat {
			go rf.replicateOneRound(peer)
		} else {
			rf.replicatorCV[peer].Signal()
		}

	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("%d## reply to %d : RequestVote开始", rf.me, args.CandidateId)
	defer DPrintf("%d## reply to %d : RequestVote结束", rf.me, args.CandidateId)

	start := time.Now()
	rf.mu.Lock()
	TPrintf("%d## : RequestVote 抢到锁花费时间=%v", rf.me, time.Since(start).Milliseconds())
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.VoteGranted, reply.Term = 0, rf.currentTerm
		SPrintf("%d##reply to %d : 不给你票, because myTerm=%d, argTerm=%d", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		return
	}

	if args.Term > rf.currentTerm {
		rf.ChangeToFollower(args.Term)
	}

	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && !rf.isUpToDate(args) {
		// 未投票且log不比人家新
		rf.role = FOLLOWER
		rf.voteFor = args.CandidateId
		reply.VoteGranted = 1
		rf.ElectionTimerReset()
		SPrintf("%d##reply to %d :term=%d, 就给你票, myVote=%d, mylastEntry is {term=%d, index=%d}, {argTerm=%d, argIndex=%d}", rf.me, args.CandidateId, args.Term, rf.voteFor, rf.lastEntry().Term, rf.lastEntry().Index, args.LastLogTerm, args.LastLogIndex)
	} else {
		SPrintf("%d##reply to %d :term=%d, 不给你票, myVote=%d, mylastEntry is {term=%d, index=%d}, {argTerm=%d, argIndex=%d}", rf.me, args.CandidateId, args.Term, rf.voteFor, rf.lastEntry().Term, rf.lastEntry().Index, args.LastLogTerm, args.LastLogIndex)
	}
	reply.Term = rf.currentTerm
	rf.persist()
}

func (rf *Raft) startElection() {
	rf.ChangeToCandidate()
	rf.ElectionTimerReset()
	SPrintf("%d## : 选举开始, currentTerm = %d", rf.me, rf.currentTerm)
	// defer SPrintf("%d## : 选举结束, currentTerm = %d", rf.me, rf.currentTerm)

	myInfo := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: rf.lastEntry().Index, LastLogTerm: rf.lastEntry().Term}

	recvFrom := make([]int, 0)
	numVotes := 1
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(x int, myInfo RequestVoteArgs) { // 要传局部变量，否则race
			SPrintf("%d## send to %d: 拉票开始", rf.me, x)
			defer SPrintf("%d## send to %d : 拉票结束", rf.me, x)
			reply := RequestVoteReply{}

			if rf.peers[x].Call("Raft.RequestVote", &myInfo, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					SPrintf("%d## recv from %d: failed because myTerm=%d, replyTerm=%d", rf.me, x, rf.currentTerm, reply.Term)
					rf.ChangeToFollower(reply.Term)
					rf.persist()

					return
				}

				if reply.VoteGranted == 1 {
					recvFrom = append(recvFrom, x)
				}

				SPrintf("%d## recv from %d: success got vote=%d, myTerm=%d replyTerm=%d", rf.me, x, reply.VoteGranted, rf.currentTerm, reply.Term)
				numVotes += reply.VoteGranted
				if numVotes > len(rf.peers)/2 && rf.role == CANDIDATE && rf.currentTerm == myInfo.Term {
					SPrintf("%d## : 当选, votes from %v, Term=%d", rf.me, recvFrom, rf.currentTerm)
					rf.ChangeToLeader()          // 内部停止选举计时器
					rf.BroadCastHeartBeat(false) // 初始化nextIndex
				} else if numVotes > len(rf.peers)/2 {
					SPrintf("%d## : 票过期了? numVotes=%d, currentTerm=%d, myInfoTerm=%d", rf.me, numVotes, rf.currentTerm, myInfo.Term)
				}
			}
		}(server, myInfo)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		for !time.Now().After(rf.getElectionTime()) {
			time.Sleep(time.Until(rf.getElectionTime()))
		}

		rf.mu.Lock()
		if rf.role != LEADER {
			rf.startElection()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) heartbeats() {
	for !rf.killed() {
		for !time.Now().After(rf.getHeartBeatTime()) {
			time.Sleep(time.Until(rf.getHeartBeatTime()))
		}
		rf.HeartBeatTimerReset()
		rf.mu.RLock()
		if rf.role == LEADER {
			rf.mu.RUnlock()
			rf.BroadCastHeartBeat(true)
		} else {
			rf.mu.RUnlock()
		}
	}
}

func (rf *Raft) ApplyToUser() {
	for !rf.killed() {
		rf.mu.Lock()
		for !(rf.lastApplied < rf.commitIndex) {
			rf.applyCv.Wait()
		}
		CPrintf("%d## : rf.lastApplied=%d, rf.commitIndex=%d", rf.me, rf.lastApplied, rf.commitIndex)
		firstIndex, commitIndex, lastApplied := rf.firstEntry().Index, rf.commitIndex, rf.lastApplied
		entries := make([]Entry, commitIndex-lastApplied)
		copy(entries, rf.log[lastApplied-firstIndex+1:commitIndex-firstIndex+1])
		rf.mu.Unlock()
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()
		// max: apply期间可能InstallSnapshot; commitIndex rather than rf.commitIndex in case of rf.commitIndex changed during apply
		rf.lastApplied = max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
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
	DPrintf("%d## : Start开始", rf.me)
	defer DPrintf("%d## : Start结束", rf.me)
	rf.mu.Lock()
	TPrintf("%d## : Start抢到锁", rf.me)
	defer rf.mu.Unlock()

	index, term, isLeader := -1, rf.currentTerm, rf.role == LEADER
	if isLeader {
		index = rf.lastEntry().Index + 1
		rf.log = append(rf.log, Entry{Term: term, Command: command, Index: index})
		rf.persist()
		rf.BroadCastHeartBeat(false)
	}
	return index, term, isLeader
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
	rf.role = FOLLOWER
	rf.applyCh = applyCh
	rf.applyCv = sync.NewCond(&rf.mu)

	rf.ElectionTimerReset()
	rf.HeartBeatTimerReset()

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

	rf.nextIndex[rf.me] = 1
	// start ticker goroutine to start elections
	rf.initReplicator()
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

func (rf *Raft) initReplicator() {
	rf.replicatorCV = make([]*sync.Cond, len(rf.peers))
	for peer := range rf.peers {
		if peer != rf.me {
			rf.replicatorCV[peer] = sync.NewCond(&sync.Mutex{})
			go rf.replicator(peer)
		}
	}
}

func (rf *Raft) randomizedElectionTimeout() time.Duration {
	return time.Duration(120+rand.Intn(150)) * time.Millisecond
}

func (rf *Raft) ChangeToFollower(Term int) {
	rf.currentTerm = Term
	rf.role = FOLLOWER
	rf.voteFor = -1
	if rf.role == LEADER {
		rf.ElectionTimerReset()
	}
}

func (rf *Raft) ChangeToCandidate() {
	rf.role = CANDIDATE
	rf.currentTerm++
	rf.voteFor = rf.me
	rf.persist()
}

func (rf *Raft) ChangeToLeader() {
	rf.role = LEADER
	for server := range rf.peers {
		rf.nextIndex[server] = rf.lastEntry().Index + 1
		rf.matchIndex[server] = 0
	}
}

func (rf *Raft) HeartBeatTimerReset() {
	rf.heartBeatTime.Store(time.Now().Add(50 * time.Millisecond))
}

func (rf *Raft) getHeartBeatTime() time.Time {
	return rf.heartBeatTime.Load().(time.Time)
}

func (rf *Raft) ElectionTimerReset() {
	rf.electionTime.Store(time.Now().Add(rf.randomizedElectionTimeout()))
}

func (rf *Raft) getElectionTime() time.Time {
	return rf.electionTime.Load().(time.Time)
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

func (rf *Raft) firstEntry() Entry {
	return rf.log[0]
}

func (rf *Raft) lastEntry() Entry {
	return rf.log[len(rf.log)-1]
}

func (rf *Raft) isUpToDate(args *RequestVoteArgs) bool {
	return rf.lastEntry().Term > args.LastLogTerm ||
		(rf.lastEntry().Term == args.LastLogTerm && rf.lastEntry().Index > args.LastLogIndex)
}

func copyLog(arg []Entry) []Entry {
	Log := make([]Entry, len(arg))
	copy(Log, arg)
	return Log
}

/*
截断？不能直接截断本地Log，网络会导致RPC乱序，可以更长的Entries（User command发送的晚）反而会先到达， 其返回后会更新nextLogIndex为一个大值，
此时短的Entries（User command发送的早），会造成command被覆盖
*/
func (rf *Raft) updateLocalLog(args *AppendEntriesArgs) {
	lastIndex := rf.lastEntry().Index
	firstIndex := rf.firstEntry().Index
	if lastIndex > args.PrevLogIndex+len(args.Entries) {
		i := 0
		for i < len(args.Entries) && rf.log[args.PrevLogIndex-firstIndex+1+i].Term == args.Entries[i].Term {
			i++
		}
		if i == len(args.Entries) {
			return
		}
	}
	rf.log = copyLog(rf.log[0 : args.PrevLogIndex-firstIndex+1])
	rf.log = append(rf.log, args.Entries...)
	rf.persist()
}

func (rf *Raft) needSendEntries(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.role == LEADER && rf.matchIndex[peer] < rf.lastEntry().Index
}

func (rf *Raft) genSnapshotArg() *InstallSnapshotArg {
	return &InstallSnapshotArg{Term: rf.currentTerm,
		Snapshot:          rf.persister.snapshot,
		LastIncludedIndex: rf.firstEntry().Index,
		LastIncludedTerm:  rf.firstEntry().Term}
}

func (rf *Raft) genAppendArg(preLogIndex int) *AppendEntriesArgs {
	entries := DeepCopyEntries(rf.log[preLogIndex-rf.firstEntry().Index+1:])
	return &AppendEntriesArgs{rf.currentTerm, rf.me, preLogIndex,
		rf.log[preLogIndex-rf.firstEntry().Index].Term, entries, rf.commitIndex}
}

// 利用反射进行深拷贝
func DeepCopyEntry(entry Entry) Entry {
	newEntry := Entry{Term: entry.Term, Index: entry.Index}

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

func (rf *Raft) majorityAgree(matchIndex int) bool {
	num := 1
	for peer := range rf.peers {
		if peer != rf.me && rf.matchIndex[peer] == matchIndex {
			num++
		}
	}
	return num > len(rf.peers)/2
}
