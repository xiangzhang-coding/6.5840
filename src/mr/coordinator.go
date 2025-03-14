package mr

//todo

/*
logrus: 功能强大，支持结构化日志。
zap: 高性能日志库。
zerolog: 零分配 JSON 日志库。
*/
import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	workersSign []bool
	workerList  []WorkerState

	mu sync.Mutex

	tasklist     []Task
	fileQuantity int
	mapDone      bool
	nReduce      int
	reduceID     []int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetReduce(args *ExampleArgs, reply *ExampleReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.Y = c.nReduce
	return nil
}

func (c *Coordinator) CreateWorker(args *WorkerState, reply *WorkerState) error {
	flag := false
	temp := WorkerState{}
	temp.startTime = time.Now()
	temp.state = "online"
	temp.lastPingTime = time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	length := len(c.workersSign)
	if length == 0 {
		c.workersSign = make([]bool, 128)
		c.workerList = make([]WorkerState, 128)
	}
	i := 0
	for ; i < len(c.workersSign); i++ {
		if c.workersSign[i] == false {
			c.workersSign[i] = true
			temp.ID = i
			c.workerList[i] = temp
			flag = true
			break
		}
	}
	if flag == false {
		c.workersSign = append(c.workersSign, true)
		temp.ID = i
		c.workerList = append(c.workerList, temp)
		flag = true
	}

	*reply = temp
	return nil
}

func (c *Coordinator) checkActive() {
	var counter int = 0
	for {

		c.mu.Lock()
		for i := 0; i < len(c.workersSign); i++ {
			if c.workersSign[i] == true {
				counter++
			}
		}
		log.Println("Approximate Total Workers: ", counter)
		counter = 0
		for i := 0; i < len(c.workerList); i++ {
			if time.Now().Sub(c.workerList[i].lastPingTime) > time.Minute*10 {
				c.workersSign[i] = false
			}
			if time.Now().Sub(c.workerList[i].lastPingTime) > time.Minute*5 {
				c.workerList[i].state = "offline"
			}
		}
		c.mu.Unlock()
		time.Sleep(time.Minute * 5)
	}
}

func (c *Coordinator) checkTask() {
	var waitingCounter int = 0
	var doingCounter int = 0
	var mapdoneCounter int = 0
	var reducedoneCounter int = 0
	for {

		c.mu.Lock()
		for i := 0; i < len(c.tasklist); i++ {
			if c.tasklist[i].state == "waiting" {
				waitingCounter++
			}
			if c.tasklist[i].state == "doing" {
				if time.Now().Sub(c.tasklist[i].startWorkTime) > time.Second*10 {
					c.tasklist[i].state = "waiting"
					for j := 0; j < len(c.reduceID); j++ {
						if c.reduceID[j] == c.tasklist[i].worker_id {
							c.reduceID[j] = -1
							break
						}
					}
					c.tasklist[i].worker_id = -1
					waitingCounter++
				} else {
					doingCounter++
				}
			}
			if c.tasklist[i].state == "done" {
				if c.tasklist[i].work_type == "map" {
					mapdoneCounter++
				} else {
					reducedoneCounter++
				}
			}
		}
		c.mu.Unlock()
		log.Println("Waiting Tasks: ", waitingCounter)
		log.Println("Doing Tasks: ", doingCounter)
		log.Println("Map Done Tasks: ", mapdoneCounter)
		log.Println("Reduce Done Tasks: ", reducedoneCounter)

		waitingCounter = 0
		doingCounter = 0
		mapdoneCounter = 0
		reducedoneCounter = 0

		time.Sleep(time.Second * 2)
	}

}

// todo  if long time no ping, delete the state in workersSign
func (c *Coordinator) Ping(args *WorkerState, reply *WorkerState) error {
	id := (*args).ID
	//todo check the id is valid.
	c.mu.Lock()
	defer c.mu.Unlock()
	var flag bool = false
	temp := WorkerState{}
	for _, worker := range c.workerList {
		if worker.ID == id {
			worker.lastPingTime = time.Now()
			if worker.state == "offline" {
				worker.state = "online"
			}
			temp.ID = worker.ID
			temp.startTime = worker.startTime
			temp.state = worker.state
			temp.lastPingTime = worker.lastPingTime
			*reply = temp
			flag = true
			break
		}
	}
	if flag == false {
		return c.CreateWorker(args, reply)
	}

	return nil
}

func (c *Coordinator) GetReduceID(args *Task, reply *int) error {
	worker_id := (*args).worker_id
	var flag bool = false
	c.mu.Lock()
	for i := 0; i < len(c.reduceID); i++ {
		if c.reduceID[i] == worker_id {
			flag = true
			*reply = i
			break
		}
	}
	c.mu.Unlock()
	if flag == false {
		*reply = -1
	}
	return nil

}

func (c *Coordinator) GetTask(args *WorkerState, task *Task) error {
	worker_id := (*args).ID
	worker_state := (*args).state
	var flag bool = false
	var alljobdone bool = true
	if worker_state == "online" {
		c.mu.Lock()
		if c.mapDone == true {
			for i := 0; i < len(c.tasklist); i++ {
				if c.tasklist[i].state == "waiting" {
					c.tasklist[i].worker_id = worker_id
					c.tasklist[i].state = "doing"
					c.tasklist[i].startWorkTime = time.Now()
					*task = c.tasklist[i]
					flag = true
					alljobdone = false

					for k := 0; k < len(c.reduceID); k++ {
						if c.reduceID[k] == -1 {
							c.reduceID[k] = worker_id
							break
						}
					}

					for j := 0; j < len(c.workerList); j++ {
						if c.workerList[j].ID == worker_id {
							c.workerList[j].state = "busy"
						}
					}
					break
				}

			}
			if alljobdone == true {
				shutdown := Task{filename: "shutdown",
					addTime: time.Now()}
				*task = shutdown
				flag = true
			}
		} else {
			for i := 0; i < len(c.tasklist); i++ {
				if c.tasklist[i].state == "waiting" && c.tasklist[i].work_type == "map" {
					c.tasklist[i].worker_id = worker_id
					c.tasklist[i].state = "doing"
					c.tasklist[i].startWorkTime = time.Now()
					*task = c.tasklist[i]
					flag = true
					for j := 0; j < len(c.workerList); j++ {
						if c.workerList[j].ID == worker_id {
							c.workerList[j].state = "busy"
						}
					}
					break
				}

			}
		}
		c.mu.Unlock()

	}
	if flag == false {
		*task = Task{}
	}

	return nil
}

func (c *Coordinator) map2reduce() {
	var counter int = 0
	for {
		counter = 0
		c.mu.Lock()
		for i := 0; i < len(c.tasklist); i++ {
			if c.tasklist[i].state == "done" && c.tasklist[i].work_type == "map" {
				c.tasklist[i].state = "waiting"
				c.tasklist[i].work_type = "reduce"
			}
			if c.tasklist[i].state == "waiting" && c.tasklist[i].work_type == "reduce" {
				counter++
			}
		}
		if counter == c.nReduce {
			c.mapDone = true
		}
		c.mu.Unlock()
		time.Sleep(time.Second * 2)
	}
}

func (c *Coordinator) TaskDone(args *WorkerState, reply *bool) error {
	worker_id := (*args).ID
	worker_state := (*args).state
	if worker_state != "busy" {
		*reply = false
		return nil
	}

	var flag bool = false
	c.mu.Lock()
	for i := 0; i < len(c.tasklist); i++ {
		if c.tasklist[i].worker_id == worker_id && c.tasklist[i].state == "doing" {
			c.tasklist[i].state = "done"
			*reply = true
			flag = true
			for j := 0; j < len(c.workerList); j++ {
				if c.workerList[j].ID == worker_id {
					c.workerList[j].state = "online"
					break
				}
			}
			break
		}
	}
	c.mu.Unlock()
	if flag == false {
		*reply = false
	}
	return nil

}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	// Your code here.
	c.mu.Lock() // AI suggestion: use defer c.mu.Unlock() to unlock the mutex automatically.
	if c.mapDone == false {
		c.mu.Unlock()
		return false
	}

	for i := 0; i < len(c.tasklist); i++ {
		if c.tasklist[i].state != "done" {
			c.mu.Unlock()
			return false
		}
	}
	c.mu.Unlock()
	log.Println("All tasks done. Coordinator will exit in 10 seconds")
	time.Sleep(time.Second * 10)

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	// log settings
	file, err := os.OpenFile("Server_app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("unable to open log file:", err)
	}
	defer file.Close()

	log.SetOutput(file)

	log.Println("Coordinator started.")

	c.nReduce = nReduce
	tmptask := Task{}
	for _, name := range files {
		//c.AllFiles = append(c.AllFiles, fileState{filename: name, finished: false})
		tmptask.filename = name
		tmptask.addTime = time.Now()
		tmptask.state = "waiting"
		tmptask.worker_id = -1
		tmptask.work_type = "map"
		c.tasklist = append(c.tasklist, tmptask)
	}
	c.reduceID = make([]int, nReduce)
	for i := range c.reduceID {
		c.reduceID[i] = -1
	}
	c.mapDone = false

	// log active workers per 5 minutes
	go c.checkActive()
	go c.checkTask()
	go c.map2reduce()

	c.server()
	return &c
}
