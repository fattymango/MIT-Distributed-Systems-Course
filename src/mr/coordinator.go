package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	fileManager        *FileManager
	taskIDCounter      int
	taskIDCounterMutex sync.Mutex
	files              []string
	nReduce            int

	ReduceTaskCounter      int
	ReduceTaskCounterMutex sync.Mutex
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		fileManager: NewFileManager(),
		files:       files,
		nReduce:     nReduce,
	}

	c.fileManager.LoadFiles(files)
	log.Printf("files loaded, len = %d", c.fileManager.FileAvailableLen())
	// Your code here.

	c.server(sockname)
	return &c
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// ret := false
	log.Printf("FileAvailableLen()=%d, FileDoneLen()=%d, files=%d", c.fileManager.FileAvailableLen(), c.fileManager.FileDoneLen(), len(c.files))
	// ret := c.fileManager.FileAvailableLen() <= 0 && c.fileManager.FileDoneLen() == len(c.files)
	ret := c.fileManager.IsAllFilesDone() && c.currentReduceTaskCounterID() == len(c.files)

	// Your code here.

	return ret
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) GetTask(args *GetTaskRequest, reply *GetTaskResponse) error {

	// check if Map jobs are done
	if c.fileManager.IsAllFilesDone() {

		//check if Reduce jobs are done, then shutdown
		if c.currentReduceTaskCounterID() >= len(c.files) {
			reply.Err = ErrShutdown.Error()
			return nil
		}

		return c.handleGetReduceTask(args, reply)

	}
	return c.handleGetMapTask(args, reply)

}
func (c *Coordinator) handleGetReduceTask(args *GetTaskRequest, reply *GetTaskResponse) error {
	id := c.getNewTaskCounterID()
	rid := c.getNewReduceTaskCounterID()
	log.Printf("reduce task id: %d", rid)
	reply.Task = Task{
		ID:                 id,
		TaskType:           TaskType_Reduce,
		ReduceTaskIdBucket: rid,
		TotalMapTasks:      len(c.files),
	}
	return nil
}
func (c *Coordinator) handleGetMapTask(args *GetTaskRequest, reply *GetTaskResponse) error {
	file, err := c.fileManager.Poll()
	if err != nil {
		log.Printf("failed to poll file, %s", err)
		reply.Err = err.Error()
		return nil
	}

	id := c.getNewTaskCounterID()
	log.Printf("file polled, file name: %s, id: %d", file, id)

	reply.Task = Task{
		ID:       id,
		FileName: file,
		NReduce:  c.nReduce,
		TaskType: TaskType_Map,
	}
	return nil
}

func (c *Coordinator) TaskUpdate(args *TaskUpdateRequest, reply *TaskUpdateResponse) error {
	log.Printf("task update, task id: %d", args.Task.ID)

	c.fileManager.FileDone(args.Task.FileName)
	return nil
}

func (c *Coordinator) getNewTaskCounterID() int {
	c.taskIDCounterMutex.Lock()
	defer c.taskIDCounterMutex.Unlock()
	c.taskIDCounter++
	return c.taskIDCounter
}

func (c *Coordinator) getNewReduceTaskCounterID() int {
	c.ReduceTaskCounterMutex.Lock()
	defer c.ReduceTaskCounterMutex.Unlock()
	c.ReduceTaskCounter++
	return c.ReduceTaskCounter
}
func (c *Coordinator) currentReduceTaskCounterID() int {
	c.ReduceTaskCounterMutex.Lock()
	defer c.ReduceTaskCounterMutex.Unlock()
	return c.ReduceTaskCounter
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
