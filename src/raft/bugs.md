# Test(2A&B)
## 锁相关
1. 没有使用defer的情况下，在return前要解锁。一开始能记得，一个函数很长，并且是回头补充代码的时候及其容易忘记。（价值5小时+海量脑细胞的BUG）


## 逻辑细节
1. 代码逻辑耦合性太高，有时候添加一个新逻辑需要照顾到之前的逻辑，及时添加有效注释应该能有帮助。（价值4小时+若干头发）
2. 代码边界条件一定要仔细揣摩、反复验证，不要节省时间，否则很容易写出对头发不友好的代码（价值3小时）
3. 当远端遇见一个往期RPC，会直接回复我们，此时reply.ConflictTerm == 0，reply.ConflictIndex = 0.假如我们不丢弃往期RPC回复会执行以下代码，造成prevLogIndex = -1的超限错误。这个BUG花了老子4个小时

## HeartBeat和Timer
1. HeartBeat goroutine未正常关闭
   1. 每次成为Leader都会启动一个HeartBeat goroutine，但是退位后却仅关闭了计时器，有可能并未退出
   2. 当在新的任期再次成为Leader重新启动一个HeartBeat goroutine，此时旧的goroutine会参与抢夺定时器
   3. 当就的HeartBeat goroutine抢到定时器，此时发送的heartBeat会被follower拒收
   4. 成为Leader的同时会重启heartBeatTimer、创建HeartBeat goroutine并发送一个sendAppendEntries RPC,所以第一轮heartBeat不会出现问题，但是当heartBeatTimer过期问题就来了，旧HeartBeat goroutine可能抢到timer然后发送无用的心跳，很可能有follower就会因未收到心跳而启动选举
   5. 在现有代码结构下，没想到有效方法解决这个问题，干脆改变了代码结构：每个server出生自带一个永不停止的heartbeat goroutine，但是只有当自己是Leader的时候才能让别人听到心跳
## 总结
1. 其实锁相关的BUG挺好修复，因为造成死锁的原因就那么几个，关键在于你得确定：是锁引起的BUG。在复杂的多routine的情况下，有时候某些线程死锁阻塞住了，有些却还在工作，很容易被表面现象迷惑。需要有测试代码确定哪些线程在工作而哪些不工作，在本lab中难有这种条件，因为测试代码太庞大了。
2. 逻辑细节BUG需要从源头掐死
3. 尽量优化代码结构

# Test(2C) 
## 锁相关
1. 除非万分肯定，不然别把channel放进临界区，会造成不幸。（5个小时的BUG）（我一开始就知道这点，还是这么做了，因为相信MIT，太天真了...）

## 逻辑细节
1. 当重启之后阅读，持久化日志，commitIndex会初始为0，Apply函数中的rf.lastApplied == rf.commitIndex条件约束太弱了...（能找到就谢天谢地了...）
2. 一定要先提高Term，再发起选举(脑子不清楚的时候建议睡觉而不是写代码，这个BUG差点让我怀疑人生)
3. heartbeats收到回复：reply.Success == false，不能立即返回，因为可能是由于Term过期而被拒收，此时reply中携带着最新的Term，需要借此更新自身Term。
4. Start函数总需要全程写锁，防止转变为follower后还向自己的log添加条目（未体现出BUG）
5. 可能需要确保在回复AppendEntries PRC之前就执行完persist().假如不这样做,考虑这样的情况:
   1. Leader发送了一个条目,恰好(2/peers+1)个server成功match并且回复,同时启动一个goroutine用于执行persist()
   2. 在Leader确认大多数server都match,发起了commit
   3. 此时问题出现了: 当某个(或几个)server在执行persisit()之前就崩溃了,然后又立马恢复,此处它们会读取旧的持久化数据,而旧的数据中没有包含刚刚Leader认为的大多数server都match的那个条目!假如此时Leader还能正常工作,它后续可以补上这个漏洞.
   4. 但是! 假如Leader这时候被分区隔离了或者崩溃了,那么剩余的server会推选出一个新的Leader继续工作,这个新Leader不包含旧Leader最后提交的那个条目!到此,这个不一致漏洞永远无法堵上.
   5. (这个BUG平均跑20趟Test(2C)才暴露一次,在秋招这个时间点我差点就要放弃这个BUG了,可就是忍不住想找出来...算下来这个BUG应该花了大于10个小时,找出来是真的爽.这个bug告诉我们:bug可以出现在任何意想不到的地方...)
   6. (这个BUG和"旧snapshot"类似,都是server回退到旧状态的BUG)
## raft规则
1. 在StartElection中收到更高Term的回复，不要重置选举倒计时。回复可能来自于一个被隔离的自嗨Server，具有极高的Term，但是log少的可怜，不能因为它重置选举倒计时。只有3中情况需要重置：1.收到有效Append RPC；2.投出同意票；3.自己启动了选举 
2. 当选Leader的时候会sendAppendEntries用于更新nextIndex。但要注意一点，不能commit往期的entries.(这个bug还没体现出来)
3. RequestVote需要全程上写锁，不然可能出现高term给低term投票的情况
4. 不能每次收到匹配的AppendEntries都草率地执行rf.commitIndex=min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries)),因为收到的可能是老的RPC，这样做会造成commitIndex回退

## 性能缺陷
1. 每次Start被调用都会创建一个sendAppendEntries goroutine，而在sendAppendEntries中又会无限重发网络失败的PRC，最后一个Test的极端情况下，存在接近4000个无限循环的sendAppendEntries goroutine，因此造成了巨大的资源浪费。其实同一时间只要存在一个sendAppendEntries goroutine就行了，因为每次发送的都是entries = log[nextIndex[i] : ]。
2. 以往逻辑：遇见同期VoteRPC就退位 --> 添加逻辑：如果两者是同期的候选者，但是对方更新，那就退位给对方
3. persist最多可能花费30毫秒的时间，如果将其合并PRC操作中会大大影响性能，同时persist只需要读锁。因此将其从主goroutine中脱离出去，并且只给它分配读锁（这一点可能会导致BUG，见**逻辑细节 5**）
4. 先DeepCopy再persist，把锁从persist转移到DeepCopy，能减小锁的占用时间（略有改善）
5. Test (2C): Figure 8 (unreliable)体现出了一些问题，PRC发送数量巨大（是MIT5倍）。原因大概率是：各个server疯狂地举行选举，很难达成一致。这会造成两类RPC数量剧增：一是RequestVote；二是sendAppendEntries。后者是由于Test程序会在servers没有达到一致commit的情况下，一直发送Start(cmd)。但是目前找不到解决办法，只能尝试提高electionTimer延迟试试。
   1. ----仔细阅读原论文，发现了算法中可能的问题：选举过程中遇到发送失败的RPC直接退出而没有重发。我在sendAppendEntries中实现了重发逻辑，只是觉得没必要在选举中重发。。。(做了修改后没有发现改善)
   2. 每个server的electionTimerOut由rf.me决定(显著改善，但是还不满意)

# Test(2D)
## Tips
1. 按log大小决定是否进行snapshot
2. Snapshot : 状态机将自己的状态保存下来,交付给raft
3. CondInstallSnapshot :  should refuse to install a snapshot if it is an old snapshot (i.e., if Raft has processed entries after the snapshot's lastIncludedTerm/lastIncludedIndex).  
4. InstallSnapshot : 当follower落后太多,就发送这个RPC让它安装携带的snapshot以跟上步伐
5. InstallSnapshot流程: follower接受到InstallSnapshot RPC --> 通过applyCh将snapshot传给状态机 --> 状态机收到后唤醒CondInstallSnapshot,告诉raft自己更新到了哪个snapshot
## ElectionTimer 
1. 重置timer的时机考虑不够全面：
   1. 假设server1，2，3号是leader被隔离了，4号，5号在同一分区会反复发起选举导致Term很大。而隔离区的leader则会反复接受Command导致Log很新。
   2. 当1，2，3号重新连接，他们所有RPC都会被拒绝，因为Term很小。4号和5号会发起选举，但是却不会成功当选leader，因为1,2,3号的log都很新，不会把票投给他们。而问题就在于1，2，3号拒绝了4,5号的请求之后直接回复他们了，而没有**重置自身的选举计时**。要知道1，2，3号是旧leader，而leader的选举计时器是关闭的，没有重启计时器就永远无法发起选举。所以最后的结果就是：4，5号反复发起选举 **-->** 1，2，3号反复拒绝，但自己却不发起选举。