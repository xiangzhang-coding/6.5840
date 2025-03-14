package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

// my code

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type WorkerState struct {
	ID           int
	startTime    time.Time
	state        string // offline online busy, online means idle
	lastPingTime time.Time
}

type Task struct {
	filename      string
	addTime       time.Time
	startWorkTime time.Time
	state         string // waiting, doing, done
	worker_id     int
	work_type     string // map, reduce
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
