package mr

import (
	"log"
	"strconv"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	nReduce          int
	mapFiles         []FileItem
	reduceFiles      []ReduceItem
	done             bool
	mapWorkerNums    int
	reduceWorkerNums int
}

type FileItem struct {
	name      string
	lock      sync.Mutex
	isAdapter bool
}

type ReduceItem struct {
	names     []string
	lock      sync.Mutex
	isAdapter bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *TaskArg, reply *TaskReply) error {
	//fmt.Println("AssignTask called")
	//reply.File = "test-file"
	//reply.TaskType = "test-task"
	for i, file := range c.mapFiles {
		file.lock.Lock()
		defer file.lock.Unlock()
		if file.isAdapter {
			continue
		} else {
			reply.File = file.name
			reply.TaskType = "MAP"
			reply.Num = c.mapWorkerNums
			for n := 0; n < c.nReduce; n++ {
				fileName := "mr-" + strconv.Itoa(c.mapWorkerNums) + "-" + strconv.Itoa(n)
				c.reduceFiles[n].names = append(c.reduceFiles[n].names, fileName)
			}
			c.mapWorkerNums += 1
			c.mapFiles[i].isAdapter = true
			return nil
		}
	}

	for index, file := range c.reduceFiles {
		file.lock.Lock()
		defer file.lock.Unlock()
		//fmt.Println("reduceFiles : %v", file.isAdapter)
		if file.isAdapter {
			continue
		} else {
			reply.ReduceFiles = make([]string, 0)
			reply.ReduceFiles = append(reply.ReduceFiles, file.names...)
			//fmt.Println("reduceFiles isNotAdapter: %v", reply.ReduceFiles)
			reply.TaskType = "REDUCE"
			reply.Num = index
			c.reduceFiles[index].isAdapter = true
			return nil
		}
	}
	c.done = true
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	//ret := true

	// Your code here.

	return c.done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:          nReduce,
		mapFiles:         make([]FileItem, 0),
		reduceFiles:      make([]ReduceItem, nReduce),
		done:             false,
		mapWorkerNums:    0,
		reduceWorkerNums: 0,
	}

	for i := 0; i < nReduce; i++ {
		c.reduceFiles[i] = ReduceItem{
			names:     make([]string, 0),
			lock:      sync.Mutex{},
			isAdapter: false,
		}
	}

	// Your code here.
	for _, file := range files {
		mapFile := FileItem{name: file, lock: sync.Mutex{}, isAdapter: false}
		c.mapFiles = append(c.mapFiles, mapFile)
		//fmt.Printf("%v load succes\n", file)
	}

	//for i := 0; i < 8; i++ {
	//	for j := 0; j < 10; j++ {
	//		filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(j)
	//		c.reduceFiles[j].names = append(c.reduceFiles[j].names, filename)
	//		fmt.Printf("%v load succes\n", filename)
	//
	//	}
	//}

	c.server()
	return &c
}
