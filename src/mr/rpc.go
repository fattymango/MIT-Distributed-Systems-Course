package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

// example to show how to declare the arguments
// and reply for an RPC.

type TaskType string

const (
	TaskType_Map    = "map"
	TaskType_Reduce = "reduce"
)

type Task struct {
	ID       int
	TaskType TaskType
	FileName string
	NReduce  int

	ReduceTaskIdBucket int
	TotalMapTasks      int
}

type GetTaskRequest struct {
	WorkerId int
}

type GetTaskResponse struct {
	Task Task
	Err  string
}
type TaskUpdateRequest struct {
	WorkerId int
	Task     Task
	Done     bool
}

type TaskUpdateResponse struct {
	Ok bool
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
