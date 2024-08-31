package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	var machine Machine
	success := Register(&machine)
	if !success {
		return
	}

	for {
		task, success := AskTask(&machine)
		if !success {
			break // 老板不回应，当他死了，我也跑路
		}

		if task.TaskType == MAP {
			DoMap(mapf, task)
		} else if task.TaskType == REDUCE {
			DoReduce(reducef, task)
		} else if task.TaskType == WAIT {
			DoWait()
		} else {
			log.Fatal("Unknow task type")
		}
		TaskCompleted(machine)
	}
}

func DoMap(mapf func(string, string) []KeyValue, task Task) bool {
	for {
		file, err := os.Open(task.FileName)
		if err != nil {
			log.Fatalf("cannot open %v", task.FileName)
			break
		}

		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", task.FileName)
			break
		}
		file.Close()

		kva := mapf(task.FileName, string(content))

		// 切分成NRduce块
		var mkva = make(map[int][]KeyValue)
		for _, kv := range kva {
			key := ihash(kv.Key) % task.NReducer
			mkva[key] = append(mkva[key], kv)
		}

		// 分别写入中间文件
		for i := 0; i < task.NReducer; i++ {
			success := WriteLocalDisk(mkva[i], task.TaskId, i) // 中间文件命名规则：mr-taskID-ReduceID
			if !success {
				log.Fatal("Map WriteLocalDisk Failed")
				break
			}
		}

		return true
	}

	return false
}

func DoWait() {
	time.Sleep(time.Second)
}

// FileFmt： "NWork-ReduceId"
func DoReduce(reducef func(string, []string) string, task Task) {
	NWork, err := strconv.Atoi(task.FileName)
	if err != nil {
		log.Fatal("reduce task.FileName wrong, usage: NWork")
	}
	var intermediate []KeyValue
	for i := 0; i < NWork; i++ {
		filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(task.TaskId) + ".json"
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal("DoReduce open intermediate file failed")
		}
		defer file.Close()

		dec := *json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				log.Fatal("DoReduce Decode failed")
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(task.TaskId)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatal("DoReduce Creat file failed")
	}
	defer ofile.Close()
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

}

// 创建并将内容写入中间文件
func WriteLocalDisk(kva []KeyValue, taskID int, reduceID int) bool {
	// 创建临时文件
	tmpFiles, err := ioutil.TempFile(".", "mr-"+strconv.Itoa(taskID)+"*")
	if err != nil {
		log.Fatal("ioutil.TempFile Failed, err=", err)
	}

	// josn解码器
	enc := json.NewEncoder(tmpFiles)

	for _, kv := range kva {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatal("Failed json Encode")
			break
		}
	}

	// 将临时文件改名，成为正式文件
	err = os.Rename(tmpFiles.Name(), "./mr-"+strconv.Itoa(taskID)+"-"+strconv.Itoa(reduceID)+".json")
	return true
}

func Register(machine *Machine) bool {
	args := EmptyArgs{}
	success := call("Coordinator.MachineRegister", &args, machine)
	return success
	// fmt.Println("Register success, Id = ", machine.Id)
}

func TaskCompleted(machine Machine) {
	reply := EmptyReply{}
	call("Coordinator.TaskCompleted", &machine, &reply)
}

func AskTask(machine *Machine) (Task, bool) {
	var task Task
	success := call("Coordinator.AssignTask", machine, &task)
	return task, success
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// log.Fatal("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
