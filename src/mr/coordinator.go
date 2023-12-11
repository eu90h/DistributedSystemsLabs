package mr

import (
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type TaskState int

type TaskType int

type Task struct {
	TaskType TaskType
	State TaskState
	TaskID string
	Data []byte
	Intermediates []KeyValue
}

type Coordinator struct {
	MapTasks []Task
	ReduceTasks []Task
	counter int
	NumMapTasksRemaining int
	NumReduceTasksRemaining int
	mu sync.Mutex
}

const (
	TaskStateIdle TaskState = iota
	TaskStateInProgress
	TaskStateCompleted
)

const (
	TaskTypeMap TaskType = 0
	TaskTypeReduce TaskType = 1
)


func (c *Coordinator) HandleWorkAssignmentRequest(_ *EmptyArgs, task *Task) error {
	if task == nil {
		return rpc.ServerError("mapTask is nil")
	}

	if c.NumMapTasksRemaining > 0 {
		for _, t := range c.MapTasks {
			if t.TaskType == TaskTypeMap && t.State == TaskStateIdle {
				c.mu.Lock()
				t.State = TaskStateInProgress
				c.mu.Unlock()

				*task = t

				return nil
			}
		}
	} else if c.NumReduceTasksRemaining > 0 {
		log.Fatal("Reduce phase not implmented yet")
	}

	return nil
}

func (c *Coordinator) RemoveMapTask(taskID string) {
	if c.NumMapTasksRemaining <= 0 {
		return
	}

	for i := range c.MapTasks {
		if taskID == c.MapTasks[i].TaskID && c.MapTasks[i].State != TaskStateCompleted {
			log.Println("removing", taskID, "from task list")

			c.mu.Lock()
			c.MapTasks[i].State = TaskStateCompleted
			c.NumMapTasksRemaining -= 1
			c.mu.Unlock()

			break
		}
	}
}

func (c *Coordinator) HandleMapTaskCompletion(response CompletedWorkResponse, _ *EmptyReply) error {
	c.RemoveMapTask(response.CompletedTaskID)

	return nil
}

func (c *Coordinator) HandleReduceKeyValue(reduceTask *Task, _ *EmptyReply) error {
	if reduceTask.TaskType != TaskTypeReduce {
		return rpc.ServerError("incorrect mapTask.TaskType")
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
	return c.NumMapTasksRemaining == 0 && c.NumReduceTasksRemaining == 0
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.NumReduceTasksRemaining = nReduce

	for _,f := range files {
		mapTask := Task{}
		mapTask.TaskType = TaskTypeMap
		mapTask.State = TaskStateIdle
		mapTask.TaskID = f

		file, err := os.Open(f)
		if err != nil {
			log.Fatalf("cannot open %v", f)
		}

		content, err := io.ReadAll(file)
		mapTask.Data = content
		if err != nil {
			log.Fatalf("cannot read %v", f)
		}

		c.MapTasks = append(c.MapTasks, mapTask)
		c.NumMapTasksRemaining += 1

		log.Println("created MapTask", mapTask.TaskID)
		file.Close()
	}

	c.server()
	return &c
}
