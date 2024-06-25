package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

const MAP = 0
const REDUCE = 1
const WAITING = 2
const DONE = 3

// dispatch

type ApplyTaskArgs struct {
	Pid int
}

type ApplyTaskReply struct {
	Category   int
	MapTask    *MapTask
	ReduceTask *ReduceTask
}

// concrete arguments of Map Reduce Task
type MapTask struct {
	Id            int
	Filename      string
	NumReduceTask int
}

type ReduceTask struct {
	Id                    int
	IntermediateFilenames []string
}

// ----------------------------------------------------------------------------

// ---	Map Tasks
type FinishMapTaskArgs struct {
	Id                int
	IntermediateFiles []string
	Pid               int
}

type FinishMapTaskReply struct {
}

// ---  Reduce Tasks
type FinishReduceTaskArgs struct {
	Id  int
	Pid int
}

type FinishReduceTaskReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
