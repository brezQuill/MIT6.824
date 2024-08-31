package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"hash/fnv"
	"os"
	"strconv"
)

// Add your RPC definitions here.
type EmptyArgs struct {
	X int
}

type EmptyReply struct {
	Y int
}

// MAP阶段：id-NRducer用于MAP阶段生成中间文件
// REDUCE阶段：id用于生成最终输出
type Task struct {
	TaskType int
	FileName string // map阶段任务文件
	TaskId   int    // reduce阶段任务文件由id推断
	NReducer int    // map任务中间文件切分为NReducer块
}

type Machine struct {
	Id int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

/////// 通用结构体
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
