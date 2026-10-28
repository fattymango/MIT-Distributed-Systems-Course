package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	for {
		task, err := CallGetTask()
		if err != nil {
			switch err.Error() {
			case ErrShutdown.Error():
				log.Printf("received shutdown signal\n")
				os.Exit(0)
			default:
				time.Sleep(200 * time.Millisecond)
				continue
			}
		}

		switch task.TaskType {
		case TaskType_Map:
			if err := processFile(task.FileName, task.ID, task.NReduce, mapf); err != nil {
				log.Printf("map task %d failed: %s\n", task.ID, err)
				continue
			}
			CallTaskUpdate(task)
		case TaskType_Reduce:
			if err := reduce(task.ReduceTaskIdBucket, task.TotalMapTasks, reducef); err != nil {
				log.Printf("reduce task %d failed: %s\n", task.ID, err)
				continue
			}
			CallTaskUpdate(task)
		default:
			log.Printf("received wrong task type, %s", task.TaskType)
		}
	}

}
func reduce(reduceTaskIdBucket int, totalMapTasks int, reducef func(string, []string) string) error {

	// 1. Read every pair for this bucket, from every map task's file.
	kva, err := readReduceInput(reduceTaskIdBucket, totalMapTasks)
	if err != nil {
		return err
	}

	// 2. Sort by key, so identical keys sit next to each other.
	sort.Slice(kva, func(i, j int) bool { return kva[i].Key < kva[j].Key })

	// 3. Output goes to a temp file first, same as the map side.
	tmp, err := os.CreateTemp(".", "mr-tmp-*")
	if err != nil {
		return err
	}

	// 4. Walk the sorted list one key-run at a time.
	//    i is the start of a run, j moves past the end of it.
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		// Collect all values for this key, e.g. "apple" -> ["1","1","1"].
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}

		// Let the app decide what to do with them (word count: count them).
		output := reducef(kva[i].Key, values)

		// Exact format the tests expect: "key value\n".
		fmt.Fprintf(tmp, "%v %v\n", kva[i].Key, output)

		i = j // jump to the next key
	}

	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}

	// 5. Atomic rename to the final output name.
	return os.Rename(tmp.Name(), fmt.Sprintf("mr-out-%d", reduceTaskIdBucket))
}

// The reducer works out its own input files from two numbers:
// its bucket (reduceID) and how many map tasks there were (nMap).
func readReduceInput(reduceID int, nMap int) ([]KeyValue, error) {
	var kva []KeyValue

	for m := 0; m < nMap; m++ {
		// Map task m always wrote its bucket reduceID to mr-m-reduceID.
		name := fmt.Sprintf("mr-%d-%d", m, reduceID)

		f, err := os.Open(name)
		if err != nil {
			return nil, err
		}

		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break // end of this file
				}
				f.Close()
				return nil, err
			}
			kva = append(kva, kv)
		}
		f.Close()
	}

	return kva, nil
}
func processFile(fileName string, taskID int, nReduce int, mapf func(string, string) []KeyValue) error {

	content, err := os.ReadFile(fileName)
	if err != nil {
		return err
	}

	res := mapf(fileName, string(content))

	buckets := make([][]KeyValue, nReduce)
	for _, kv := range res {
		r := ihash(kv.Key) % nReduce
		buckets[r] = append(buckets[r], kv)
	}

	for r, kvs := range buckets {
		if err := writeIntermediate(taskID, r, kvs); err != nil {
			return err
		}
	}

	log.Printf("map task %d: %d pairs into %d files", taskID, len(res), nReduce)
	return nil
}

func writeIntermediate(taskID int, r int, kvs []KeyValue) error {
	// 3. Write to a temp file first, in the current directory ("."),
	//    not /tmp, because rename can fail across filesystems.
	tmp, err := os.CreateTemp(".", "mr-tmp-*")
	if err != nil {
		return err
	}

	// 4. One JSON object per pair, one per line.
	//    The reducer reads them back with a json.Decoder until EOF.
	enc := json.NewEncoder(tmp)
	for _, kv := range kvs {
		if err := enc.Encode(&kv); err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
			return err
		}
	}

	// 5. Close before renaming, so all data is flushed to the file.
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}

	// 6. Atomic rename: readers see either the complete file or nothing.
	//    If two workers ran the same task, the second rename just replaces
	//    the first with identical content, which is harmless.
	final := fmt.Sprintf("mr-%d-%d", taskID, r)
	return os.Rename(tmp.Name(), final)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallGetTask() (*Task, error) {

	// declare an argument structure.
	args := GetTaskRequest{}

	// declare a reply structure.
	reply := GetTaskResponse{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
		return nil, ErrCallFailed
	}

	if reply.Err != "" {
		return nil, errors.New(reply.Err)
	}
	return &reply.Task, nil
}
func CallTaskUpdate(task *Task) error {
	args := TaskUpdateRequest{
		Task: *task,
		Done: true,
	}
	reply := TaskUpdateResponse{}
	ok := call("Coordinator.TaskUpdate", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
		return ErrCallFailed
	}
	return nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
