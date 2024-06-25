package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	m sync.Mutex

	NumMapTask int
	mapTasks   []TaskStatus

	NumReduceTask int
	reduceTasks   []TaskStatus

	workersStatus     map[int]string
	intermediateFiles map[int][]string // map from reduce_id to intermediate files
	// 1. url
	// 2. hdfs
	// ->
}

//  tracking which tasks have finished
type TaskStatus struct {
	// task id is the index of the tasks
	Done      bool
	Filename  string
	StartTime int64
	Status    string
}

func (c *Coordinator) DispatchTask(args *ApplyTaskArgs, reply *ApplyTaskReply) error {
	c.m.Lock()
	defer c.m.Unlock()
	if c.workersStatus[args.Pid] == "" {
		c.workersStatus[args.Pid] = "idle"
	}

	// 1. try to dispatch a map task
	workerMapTask := &MapTask{}
	for i, mapStatus := range c.mapTasks {
		if mapStatus.Status == "pending" {
			reply.Category = MAP
			workerMapTask.Id = i
			workerMapTask.Filename = mapStatus.Filename
			workerMapTask.NumReduceTask = c.NumReduceTask
			reply.MapTask = workerMapTask
			// change the status of the map task
			c.mapTasks[i].Status = "running"
			c.mapTasks[i].StartTime = time.Now().Unix()
			// change the status of worker
			c.workersStatus[args.Pid] = "busy"
			return nil
		}
	}

	// 2. if all mapTasks are running but not finished, waiting
	for _, mapStatus := range c.mapTasks {
		if !mapStatus.Done {
			reply.Category = WAITING
			return nil
		}
	}

	// 3. all mapTasks are finished, then dispatch reduce tasks
	for i, reduceStatus := range c.reduceTasks {
		if reduceStatus.Status == "pending" {
			reply.Category = REDUCE
			workerReduceTask := &ReduceTask{}
			workerReduceTask.Id = i
			workerReduceTask.IntermediateFilenames = c.intermediateFiles[i]
			reply.ReduceTask = workerReduceTask
			// change the status of the reduce task
			c.reduceTasks[i].Status = "running"
			c.reduceTasks[i].StartTime = time.Now().Unix()
			// change the status of worker
			c.workersStatus[args.Pid] = "busy"
			return nil
		}
	}

	// 4. if all reduceTasks are running but not finished, waiting
	if c.Done() {
		reply.Category = DONE
	} else {
		reply.Category = WAITING
	}
	return nil
}

func (c *Coordinator) FinishMapTask(args *FinishMapTaskArgs, reply *FinishMapTaskReply) error {
	log.Printf("Receive finished Map Task: %v\n", args.Id)
	c.m.Lock()
	defer c.m.Unlock()
	c.workersStatus[args.Pid] = "idle"
	c.mapTasks[args.Id].Done = true
	c.mapTasks[args.Id].Status = "Completed"
	c.mapTasks[args.Id].StartTime = -1
	for i := 0; i < c.NumReduceTask; i++ {
		c.intermediateFiles[i] = append(c.intermediateFiles[i], args.IntermediateFiles[i])
	}
	return nil
}

func (c *Coordinator) FinishReduceTask(args *FinishReduceTaskArgs, reply *FinishReduceTaskReply) error {
	log.Printf("Receive finished Reduce Task: %v\n", args.Id)
	c.m.Lock()
	defer c.m.Unlock()
	c.workersStatus[args.Pid] = "idle"
	c.reduceTasks[args.Id].Done = true
	c.reduceTasks[args.Id].Status = "Completed"
	c.reduceTasks[args.Id].StartTime = -1
	return nil
}

func (c *Coordinator) StartTicker() {
	ticker := time.NewTicker(10 * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				if c.Done() {
					return
				}
				c.CheckDeadWorker()
			}
		}
	}()
}

func (c *Coordinator) CheckDeadWorker() {
	c.m.Lock()
	defer c.m.Unlock()
	for i, mapTask := range c.mapTasks {
		if mapTask.Status == "running" {
			now := time.Now().Unix()
			if mapTask.StartTime > 0 && now > (mapTask.StartTime+10) {
				c.mapTasks[i].StartTime = -1
				c.mapTasks[i].Status = "pending"
				continue
			}
		}
	}

	for i, reduceTask := range c.reduceTasks {
		if reduceTask.Status == "running" {
			now := time.Now().Unix()
			if reduceTask.StartTime > 0 && now > (reduceTask.StartTime+10) {
				c.reduceTasks[i].Status = "pending"
				c.reduceTasks[i].StartTime = -1
				continue
			}
		}
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	// register c as an RPC server
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)

	fmt.Printf("sockname: %s\n", sockname)

	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	// run the Server method
	go http.Serve(l, nil)
}

//return
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	for _, task := range c.reduceTasks {
		if !task.Done {
			return false
		}
	}
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.NumReduceTask = nReduce
	c.NumMapTask = len(files)
	c.intermediateFiles = make(map[int][]string)
	c.workersStatus = make(map[int]string)

	log.Printf("%v map tasks; %v reduce tasks", c.NumMapTask, c.NumReduceTask)

	for _, v := range files {
		c.mapTasks = append(c.mapTasks, TaskStatus{Filename: v, Done: false, Status: "pending"})
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, TaskStatus{Filename: fmt.Sprintf("mr-out-{%d}", i+1), Done: false, Status: "pending"})
	}
	c.StartTicker()
	c.server()
	return &c
}
