package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	// 如果文件不存在，err 会包含 "file does not exist" 的信息
	return !os.IsNotExist(err)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := TaskArg{}
		reply := TaskReply{
			ReduceFiles: make([]string, 0),
		}
		ok := call("Coordinator.AssignTask", &args, &reply)
		if ok {
			// reply.Y should be 100.
			//fmt.Printf("reply success, %v\n", reply.File)
			if reply.TaskType == "MAP" {
				filename := reply.File
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				kva := mapf(filename, string(content))
				// 创建一个文件句柄映射
				outputFiles := make(map[int]*os.File)

				// 遍历所有的 key-value
				for _, kv := range kva {
					// 计算文件索引
					index := ihash(kv.Key) % 10
					outputFileName := "mr-" + strconv.Itoa(reply.Num) + "-" + strconv.Itoa(index)

					// 如果文件不存在，打开它；否则，创建新文件
					if _, exists := outputFiles[index]; !exists {
						// 打开或创建文件（以追加模式打开）
						outputFile, err := os.OpenFile(outputFileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
						if err != nil {
							fmt.Println("Error opening or creating file:", err)
							return
						}
						// 将文件保存到 map 中，确保文件只打开一次
						outputFiles[index] = outputFile
					}

					// 获取文件句柄
					outputFile := outputFiles[index]
					// 创建 JSON 编码器
					enc := json.NewEncoder(outputFile)
					if err := enc.Encode(&kv); err != nil {
						fmt.Println("Error writing to file:", err)
						return
					}
				}

				// 关闭所有打开的文件
				for _, file := range outputFiles {
					file.Close()
				}
				//fmt.Println("kva written successfully.")
			} else if reply.TaskType == "REDUCE" {
				var reduceData []KeyValue
				//fmt.Println("reduceFiles isNotAdapter: %v", reply.ReduceFiles)
				reduceFiles := reply.ReduceFiles
				for _, reduceFile := range reduceFiles {
					filename := reduceFile
					file, reduceFileOpenErr := os.Open(filename)
					if reduceFileOpenErr != nil {
						log.Fatalf("cannot open %v", filename)
					}
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						reduceData = append(reduceData, kv)
					}
					file.Close()
				}
				sort.Sort(ByKey(reduceData))
				oname := "mr-out-" + strconv.Itoa(reply.Num)
				ofile, _ := os.Create(oname)

				i := 0
				for i < len(reduceData) {
					j := i + 1
					for j < len(reduceData) && reduceData[j].Key == reduceData[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, reduceData[k].Value)
					}
					output := reducef(reduceData[i].Key, values)
					fmt.Fprintf(ofile, "%v %v\n", reduceData[i].Key, output)
					i = j
				}

				ofile.Close()
			} else {
				//fmt.Printf("task type error, %v\n", reply.TaskType)
			}
		} else {
			fmt.Printf("call failed!\n")
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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
	// the Example() method of struct Coordinato0r.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
