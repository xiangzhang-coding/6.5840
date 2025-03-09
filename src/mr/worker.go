package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var fileMutex sync.Mutex

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type LocalState struct {
	id           int
	startTime    time.Time
	state        string // offline online busy, online means idle
	lastPingTime time.Time
	mu           sync.Mutex
}

func (ls *LocalState) Local2Worker() WorkerState {
	ws := WorkerState{}
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ws.id = ls.id
	ws.startTime = ls.startTime
	ws.state = ls.state
	ws.lastPingTime = ls.lastPingTime
	return ws
}

func (ls *LocalState) Worker2Local(ws WorkerState) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.id = ws.id
	ls.startTime = ws.startTime
	ls.state = ws.state
	ls.lastPingTime = ws.lastPingTime
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// this can change to read from a config file or command line.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	file, err := os.OpenFile("Client_app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("unable to open log file:", err)
	}
	defer file.Close()
	log.SetOutput(file)

	localState := LocalState{
		id: -1,
	}
	log.Println("local worker starting...")

	if localState.id == -1 {
		// register with coordinator
		ws := localState.Local2Worker()
		ws = createWorker(&ws)
		localState.Worker2Local(ws)
	}

	if localState.id == -1 {
		log.Fatal("failed to register with coordinator")
	} else {
		log.Printf("worker id\t %d registered with coordinator\n", localState.id)
	}

	go localState.pingCoordinator()

	go localState.working(mapf, reducef)

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func getReduce(res *int) {

	args := ExampleArgs{}
	reply := ExampleReply{}

	ok := call("Coordinator.GetReduce", &args, &reply)
	if ok != true {
		log.Println("coordinator not responding...")
	} else {
		*res = reply.Y
		log.Printf("I got that there are %d reduce tasks", reply.Y)
	}
}

func createWorker(args *WorkerState) WorkerState {
	reply := WorkerState{}
	ok := call("Coordinator.CreateWorker", &args, &reply)
	if ok {
		return reply
	} else {
		return WorkerState{}
	}
}

func (ls *LocalState) pingCoordinator() {
	args := WorkerState{}
	reply := WorkerState{}
	for {
		ok := call("Coordinator.Ping", &args, &reply)
		if ok == true {
			ls.Worker2Local(reply)
		} else {
			log.Printf("Workder-%v pinged,coordinator not responding...\n", ls.id)
		}
		time.Sleep(2 * time.Second)
	}
}

func (ls *LocalState) getTask(task *Task) bool {
	args := WorkerState{}
	ok := call("Coordinator.GetTask", &args, task)
	if ok == true && task.filename != "" {
		ls.state = "busy"
		return true
	} else {
		return false
	}
}

func getReduceID(task *Task) int {
	var result int
	ok := call("Coordinator.GetReduceID", &task, &result)
	if ok {
		return result
	} else {
		log.Println("coordinator not responding... Can't get reduce id")
		return result
	}
}

func (ls *LocalState) doJob(localtask *Task,
	mapf func(string, string) []KeyValue, reducef func(string, []string) string) bool {
	if localtask.filename == "shutdown" {
		log.Println("All jobs have been done, cooordinator will shutdown soon...")
		log.Println("worker will exit...")
		os.Exit(0)
	}
	if localtask.work_type == "map" {
		intermediate := []KeyValue{}
		filename := localtask.filename
		file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
		if err != nil {
			log.Printf("worker %d failed to open file %s\n", ls.id, filename)
			return false
		}
		defer file.Close()
		// for unix/macos or linux not windows
		err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX)
		if err != nil {
			log.Printf("worker %d failed to lock file %s: %v\n", ls.id, filename, err)
			return false
		}
		defer syscall.Flock(int(file.Fd()), syscall.LOCK_UN)

		content, err := io.ReadAll(file)
		if err != nil {
			log.Printf("cannot read %v\n", filename)
			return false
		}

		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)

		sort.Sort(ByKey(intermediate))
		var nReduce int = -1
		getReduce(&nReduce)
		if nReduce == -1 {
			log.Printf("worker %d failed to get reduce tasks\n", ls.id)
			return false
		}

		reduce_tasks := make([][]KeyValue, nReduce)
		for i := 0; i < len(intermediate); i++ {
			j := ihash(intermediate[i].Key) % nReduce
			reduce_tasks[j] = append(reduce_tasks[j], intermediate[i])
		}

		for i := 0; i < nReduce; i++ {
			oname := fmt.Sprintf("mr-out-%d", i)

			fileMutex.Lock()
			ofile, err := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				log.Printf("Failed to open file %s: %v\n", oname, err)
				fileMutex.Unlock() // 解锁
				return false
			}

			for _, kv := range reduce_tasks[i] {
				fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
			}
			ofile.Close()
			fileMutex.Unlock()
		}
		return true

	}
	// work_type == "reduce"
	var reduceID int = -1
	if localtask.work_type == "reduce" {
		reduceID = getReduceID(localtask)
		if reduceID == -1 {
			log.Fatal("worker %d failed to get reduce id\n", ls.id)
		}
		filename := fmt.Sprintf("mr-out-%d", reduceID)

		file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
		if err != nil {
			log.Printf("worker %d failed to open file %s\n", ls.id, filename)
			return false
		}
		defer file.Close()
		// for unix/macos or linux not windows
		err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX)
		if err != nil {
			log.Printf("worker %d failed to lock file %s: %v\n", ls.id, filename, err)
			return false
		}
		defer syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		content, err := io.ReadAll(file)
		if err != nil {
			log.Printf("cannot read %v\n", filename)
			return false
		}

		// sort the content by key

		intermediate := make([]KeyValue, 0)
		pair := KeyValue{}
		for _, line := range strings.Split(string(content), "\n") {
			if line == "" {
				continue
			}
			fields := strings.Split(line, " ")

			if len(fields) != 2 {
				log.Printf("worker %d failed to parse %s\n", ls.id, filename)

			}
			pair.Key = fields[0]
			pair.Value = fields[1]
			intermediate = append(intermediate, pair)
		}
		sort.Sort(ByKey(intermediate))

		oname := fmt.Sprintf("mr-out-%d", reduceID)
		fileMutex.Lock()
		//overwrite the output file
		ofile, err := os.OpenFile(oname, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			log.Printf("Failed to open file %s: %v\n", oname, err)
			fileMutex.Unlock() // 解锁
			return false
		}

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

		ofile.Close()
		fileMutex.Unlock()
		return true
	}
	return false
}

func (ls *LocalState) working(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	task := Task{}
	for {
		time.Sleep(2 * time.Second)
		ls.mu.Lock()
		if ls.state == "online" {
			ok := ls.getTask(&task)
			if !ok {
				log.Printf("worker %d failed to get task\n", ls.id)
			} else {
				log.Printf("worker %d got task %s\n", ls.id, task.filename)
				ls.state = "busy"
			}
		}
		ls.mu.Unlock()
		if task.filename != "" {
			continue
		}

		restult := ls.doJob(&task, mapf, reducef)
		if !restult {
			log.Printf("worker %d failed to do job %s\n", ls.id, task.filename)
			ls.mu.Lock()
			ls.state = "online"
			ls.mu.Unlock()
		} else {
			log.Printf("worker %d finished job %s\n", ls.id, task.filename)
			ls.mu.Lock()
			ls.state = "online"
			ls.mu.Unlock()
			for {
				doneOk := reportDone(ls)
				if doneOk {
					break
				} else {
					time.Sleep(1 * time.Second)
				}
			}
		}
	}
}

func reportDone(ls *LocalState) bool {
	ws := ls.Local2Worker()
	result := false
	ok := call("Coordinator.TaskDone", &ws, &result)
	if ok {
		return result
	} else {
		return false
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
