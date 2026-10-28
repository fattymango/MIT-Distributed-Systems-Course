package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const taskTimeout = 10 * time.Second

type taskState struct {
	started time.Time
	done    bool
}

type Coordinator struct {
	mu          sync.Mutex
	files       []string
	nReduce     int
	mapTasks    []taskState
	reduceTasks []taskState
	nMapDone    int
	nReduceDone int
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		nReduce:     nReduce,
		mapTasks:    make([]taskState, len(files)),
		reduceTasks: make([]taskState, nReduce),
	}
	c.server(sockname)
	return &c
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nReduceDone == len(c.reduceTasks)
}

func (c *Coordinator) GetTask(args *GetTaskRequest, reply *GetTaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.nMapDone < len(c.mapTasks) {
		if i := claim(c.mapTasks); i >= 0 {
			reply.Task = Task{
				ID:       i,
				TaskType: TaskType_Map,
				FileName: c.files[i],
				NReduce:  c.nReduce,
			}
		} else {
			reply.Err = ErrNoAvailabeFiles.Error()
		}
		return nil
	}

	if c.nReduceDone < len(c.reduceTasks) {
		if i := claim(c.reduceTasks); i >= 0 {
			reply.Task = Task{
				ID:                 i,
				TaskType:           TaskType_Reduce,
				ReduceTaskIdBucket: i,
				TotalMapTasks:      len(c.files),
			}
		} else {
			reply.Err = ErrNoAvailabeFiles.Error()
		}
		return nil
	}

	reply.Err = ErrShutdown.Error()
	return nil
}

func (c *Coordinator) TaskUpdate(args *TaskUpdateRequest, reply *TaskUpdateResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	tasks, nDone := c.mapTasks, &c.nMapDone
	if args.Task.TaskType == TaskType_Reduce {
		tasks, nDone = c.reduceTasks, &c.nReduceDone
	}
	id := args.Task.ID
	if args.Done && id >= 0 && id < len(tasks) && !tasks[id].done {
		tasks[id].done = true
		*nDone++
	}
	reply.Ok = true
	return nil
}

func claim(tasks []taskState) int {
	now := time.Now()
	for i := range tasks {
		if !tasks[i].done && now.Sub(tasks[i].started) > taskTimeout {
			tasks[i].started = now
			return i
		}
	}
	return -1
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}
