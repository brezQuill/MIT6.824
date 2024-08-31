package mr

import (
	"container/list"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

// Task类型--阶段
const (
	MAP int = iota
	REDUCE
	WAIT // 等待工作
	EXIT // 退出工作
)

type WorkMachine struct {
	Tasks     *list.List
	HeartBeat chan struct{}
}

type Coordinator struct {
	Phase int

	IdleTasks *list.List

	Workers map[int]WorkMachine // map WorkerID : WorkMachine={TaskList, chan}
	NWorker int                 // 记录数量，充当workerID

	NMapTask int // reduce任务需要这个值，用来找到所有intermediate文件 （阅读时添加的字段）
	NReduce  int // 聚合函数参数，传入参数

	NTaskDone int // 记录完成的任务数量， 用于判断是否该进入下一阶段 （阅读时添加的字段）

	TLock sync.RWMutex
}

func (c *Coordinator) MachineRegister(args *EmptyArgs, machine *Machine) error {
	c.TLock.Lock()
	c.NWorker++                         // machine的编号从1开始
	c.Workers[c.NWorker] = newMachine() // 将前来注册的machine注册下来
	machine.Id = c.NWorker              // 给前来注册的machine分配id
	c.TLock.Unlock()
	return nil
}

func (c *Coordinator) AssignTask(machine *Machine, task *Task) error {
	// Reduce阶段，不记录machine的过往任务
	c.TLock.Lock()

	if c.IdleTasks.Front() != nil {
		elem := c.IdleTasks.Front()
		c.IdleTasks.Remove(elem)
		*task = elem.Value.(Task)
		// Reduce阶段，分配任务前，清空其执行的以往任务， 因为它死亡时会重新调配记录中的任务
		if c.Phase == REDUCE {
			workMachine := c.Workers[machine.Id]
			workMachine.Tasks = list.New()
			c.Workers[machine.Id] = workMachine
		}
		c.Workers[machine.Id].Tasks.PushBack(*task) // 切记list里面只存Task类型
	} else {
		*task = Task{TaskType: WAIT}
	}
	c.TLock.Unlock()
	// 任务分配结束，等待回复
	go c.WaitReply(*machine, *task)

	return nil
}

// 分配任务后立即新建协程调用，用于监听该任务的动向
func (c *Coordinator) WaitReply(machine Machine, task Task) {
	c.TLock.RLock()
	HeartBeat := c.Workers[machine.Id].HeartBeat
	c.TLock.RUnlock()

	select {
	case <-HeartBeat:
		if task.TaskType == WAIT {
			return
		}
		// 传来心跳，代表任务完成
		// fmt.Println("taskType: ", task.TaskType, "  taskId: ", task.TaskId, " is done")

		// 检测是否需要进入下一个阶段：MAP -> REDUCE -> EXIT
		c.TLock.Lock()
		// 只有Map任务能触发，就算最后两个任务同时回信，也只有一个任务会触发阶段转换
		if task.TaskType == MAP && c.Phase == MAP {
			c.NTaskDone++
			if c.IdleTasks.Front() == nil && c.NTaskDone == c.NMapTask {
				c.NTaskDone = 0
				c.Phase = REDUCE
				c.createReduceTasks() // 已经在锁中了，不能在函数中重复上锁
			}
		} else if task.TaskType == REDUCE && c.Phase == REDUCE { // 只有Reduce任务能触发
			c.NTaskDone++
			if c.IdleTasks.Front() == nil && c.NTaskDone == c.NReduce {
				c.Phase = EXIT
			}
		}
		c.TLock.Unlock()
	case <-time.After(time.Second * 10):
		// 10秒没听到心跳，默认任务失败，需要重置该machine的所有任务 || Reduce阶段，不记录machine的过往任务
		c.TLock.Lock()
		c.NTaskDone -= c.Workers[machine.Id].Tasks.Len() - 1 // 减去最后一个失败的任务
		c.IdleTasks.PushBackList(c.Workers[machine.Id].Tasks)
		// map用哈希+链表实现，由于可能自动动态扩容，而无法对元素取址
		workerMachine := c.Workers[machine.Id]
		workerMachine.Tasks = list.New()
		c.Workers[machine.Id] = workerMachine
		c.TLock.Unlock()
	}
}

func (c *Coordinator) TaskCompleted(machine *Machine, reple *EmptyReply) error {
	c.TLock.RLock()
	HeartBeat := c.Workers[machine.Id].HeartBeat
	c.TLock.RUnlock()

	select {
	case HeartBeat <- struct{}{}:
		// (Id)machine 触发它的心跳，通知老板任务做完了
	case <-time.After(time.Second):
		// 如果是10秒后才通知老板，那老板不会接受，为了防止一直阻塞，一秒后退出
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.TLock.RLock()
	ret := c.Phase == EXIT
	c.TLock.RUnlock()

	return ret
}

// machine注册时初始化
func newMachine() WorkMachine {
	machine := WorkMachine{
		Tasks:     list.New(),
		HeartBeat: make(chan struct{}),
	}
	return machine
}

// 创建Reduce任务，FileName字段存储c.NWorker, 用于推导任务文件名称
// 调用于WaitReply, 已经位于锁中，不能再上锁
func (c *Coordinator) createReduceTasks() {
	for i := 0; i < c.NReduce; i++ {
		c.IdleTasks.PushBack(Task{TaskType: REDUCE,
			FileName: strconv.Itoa(c.NMapTask), // 这里记录的应该是Map_task数量
			TaskId:   i,
			NReducer: c.NReduce})
	}
}

// 创建Map任务
func (c *Coordinator) createMapTasks(files []string) {
	for ind, val := range files {
		c.IdleTasks.PushBack(Task{TaskType: MAP,
			FileName: val,
			TaskId:   ind,
			NReducer: c.NReduce})
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Phase:     MAP,
		IdleTasks: list.New(),
		Workers:   make(map[int]WorkMachine),
		NWorker:   0,
		NMapTask:  len(files),
		NReduce:   nReduce,
	}
	c.createMapTasks(files)
	c.server()
	return &c
}
