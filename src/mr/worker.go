package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func ReduceKeyValue(kv *KeyValue) {
	ok := call("Coordinator.HandleReduceKeyValue", kv, &EmptyReply{})
	if !ok {
		log.Fatal("ReduceKeyValue was not OK!")
	}
}

func CompleteMapTask(mapTask *Task, kvs []KeyValue) {
	if mapTask == nil {
		log.Fatal("mapTask was nil!")
	}

	log.Printf("task %s completed\n", mapTask.TaskID)

	completionResponse := CompletedWorkResponse{}
	completionResponse.Kvs = kvs
	completionResponse.CompletedTaskID = mapTask.TaskID

	ok := call("Coordinator.HandleMapTaskCompletion", &completionResponse, &EmptyReply{})
	if !ok {
		log.Fatal("FinishWorkAssignment was not OK!")
	}
}

func RequestWorkAssignment() *Task {
	response := Task{}

	ok := call("Coordinator.HandleWorkAssignmentRequest", &EmptyArgs{}, &response)
	if !ok {
		return nil
	}

	return &response
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		task := RequestWorkAssignment()
		if task == nil {
			log.Fatalln("null mapTask!")
		}
	
		switch task.TaskType {
		case TaskTypeMap:
			log.Printf("mapping on %s\n", task.TaskID)
			HandleMapTask(task, mapf)
			break
		case TaskTypeReduce:
			log.Printf("reducing on %s\n", task.TaskID)
			HandleReduceTask(task, reducef)
			break
		}
	}
}

func HandleMapTask(task *Task, mapf func(string, string) []KeyValue) {
	task.State = TaskStateInProgress

	kv := mapf(task.TaskID, string(task.Data))

	CompleteMapTask(task, kv)
}

func HandleReduceTask(task *Task, reducef func(string, []string) string) {
	
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	fmt.Println(sockname)
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
