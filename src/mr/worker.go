package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func merge(left, right []string) []string {
	result := make([]string, 0, len(left)+len(right))
	i, j := 0, 0
	for i < len(left) && j < len(right) {
		if left[i] < right[j] {
			result = append(result, left[i])
			i++
		} else {
			result = append(result, right[j])
			j++
		}
	}
	result = append(result, left[i:]...)
	result = append(result, right[j:]...)
	return result
}

func mergeSort(data []string) []string {
	if len(data) <= 1 {
		return data
	}
	mid := len(data) / 2
	left := mergeSort(data[:mid])
	right := mergeSort(data[mid:])
	return merge(left, right)
}

// read Json
func readJSONFile(filename string) ([]KeyValue, error) {
	var data []KeyValue
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	byteValue, err := ioutil.ReadAll(f)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	defer f.Close()
	err = json.Unmarshal(byteValue, &data)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	return data, nil
}

// Write sorted lines to a file in JSON format
func writeJSONFile(filename string, data []KeyValue) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	byteData, err := json.Marshal(data)
	if err != nil {
		log.Println(err)
		return err
	}
	err = ioutil.WriteFile(filename, byteData, 0644)
	return err
}

func checkBufferAndSpill(partitions *map[int][]KeyValue, bufferSize int, mapId int, spillIndex *int) {
	// use a mutex lock here to protect partitions because it is a shared variable
	var mu sync.Mutex
	mu.Lock()
	defer mu.Unlock()
	for partition, content := range *partitions {
		if float64(len((*partitions)[partition])) > 0.8*float64(bufferSize) {
			// spill
			sort.Sort(ByKey(content))
			oname := fmt.Sprintf("mr-%d-%d-spill-%d", mapId, partition, *spillIndex)
			log.Printf("Write spill %s\n", oname)
			*spillIndex += 1
			writeJSONFile(oname, (*partitions)[partition])
			// clear
			(*partitions)[partition] = []KeyValue{}
		}
	}
}

// Perform k-way merge on sorted slices
func mergeSortedSlices(sortedSlices [][]KeyValue) []KeyValue {
	var result []KeyValue
	indices := make([]int, len(sortedSlices))

	for {
		minIndex := -1
		var minValue KeyValue

		for i, slice := range sortedSlices {
			if indices[i] < len(slice) {
				if minIndex == -1 || slice[indices[i]].Key < minValue.Key {
					minIndex = i
					minValue = slice[indices[i]]
				}
			}
		}

		if minIndex == -1 {
			break
		}

		result = append(result, minValue)
		indices[minIndex]++
	}
	return result
}

func mergeSpill(mapId int, numPartitions int) {
	for i := 0; i < numPartitions; i++ {
		pattern := fmt.Sprintf("mr-%d-%d-spill-*", mapId, i)
		fileList, err := filepath.Glob(pattern)
		if err != nil {
			fmt.Printf("failed to find files: %s\n", err.Error())
			continue
		}
		log.Printf("fileList: %v\n", fileList)

		var sortedChunks [][]KeyValue
		for _, file := range fileList {
			data, err := readJSONFile(file)
			if err != nil {
				log.Printf("failed to read file %s: %s\n", file, err.Error())
				continue
			}
			sortedChunks = append(sortedChunks, data)
		}

		// Perform k-way merge on sorted chunks
		finalSortedList := mergeSortedSlices(sortedChunks)

		// Write the final sorted list to a JSON file
		outputFile := fmt.Sprintf("mr-%d-%d", mapId, i)
		err = writeJSONFile(outputFile, finalSortedList)
		if err != nil {
			log.Printf("failed to write output file %s: %s\n", outputFile, err.Error())
		}
	}
}

func doMap(mapTask *MapTask, mapf func(string, string) []KeyValue) {

	log.Println("Executing map function")
	// 1. get the name
	filename := mapTask.Filename

	// 2. Open the file
	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("failed to open file: %s\n", err.Error())
		return
	}
	defer file.Close()

	// 3. Read the content
	contentBytes, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("failed to read file: %s\n", err.Error())
		return
	}
	content := string(contentBytes)
	// 4. call the map function:
	// parse key/value pairs out of the input data and pass them to the user-designed Map function
	map_result := mapf(filename, content)

	partitions := make(map[int][]KeyValue)
	// spillIndex := 0
	for _, kv := range map_result {
		// 2. map shuffle: partition (using kv.key's hash value to mod num_reduce_tasks)
		reduceid := ihash(kv.Key) % mapTask.NumReduceTask
		partitions[reduceid] = append(partitions[reduceid], kv)
		// checkBufferAndSpill(&partitions, 1000, reply.Id, &spillIndex)
	}
	var intermediateFilenames []string
	for i := 0; i < mapTask.NumReduceTask; i++ {
		name := fmt.Sprintf("mr-%v-%v", mapTask.Id, i+1)
		intermediateFilenames = append(intermediateFilenames, name)
		writeJSONFile(name, partitions[i])
	}

	finishArgs := FinishMapTaskArgs{Id: mapTask.Id, IntermediateFiles: intermediateFilenames, Pid: os.Getpid()}
	finishMapRely := FinishMapTaskReply{}
	ok := call("Coordinator.FinishMapTask", &finishArgs, &finishMapRely)
	if !ok {
		log.Fatal("Failed to finish map task")
	}

	// for i := 0; i < reply.NumReduceTask; i++ {
	// 	name := fmt.Sprintf("mr-%d-%d-spill-%d", reply.Id, i, spillIndex)
	// 	spillIndex++
	// 	writeJSONFile(name, partitions[i])
	// }
	// mergeSpill(reply.Id, reply.NumReduceTask)
}

func doReduce(reduceTask *ReduceTask, reducef func(string, []string) string) {
	log.Println("executing reduce task")

	kva := []KeyValue{}
	for _, v := range reduceTask.IntermediateFilenames {
		data, err := readJSONFile(v)
		if err != nil {
			log.Printf("failed to read file: %s\n", err)
		}
		kva = append(kva, data...)
	}
	// pattern := fmt.Sprintf("mr-*-%d", reply.Id)
	// fileList, err := filepath.Glob(pattern)
	// if err != nil {
	// 	fmt.Printf("failed to open file: %s\n", err.Error())
	// }

	// result := mergeSortedSlices(kva)
	sort.Sort(ByKey(kva))

	// Write the final sorted list to a JSON file
	outputFilename := fmt.Sprintf("mr-out-%d", reduceTask.Id)
	outputFile, err := os.OpenFile(outputFilename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("failed to open output file")
	}
	defer outputFile.Close()
	l, r := 0, 0
	for r < len(kva) {
		for ; kva[l].Key == kva[r].Key; r++ {
			if r == len(kva)-1 {
				r = len(kva)
				break
			}
		}
		values := []string{}
		for l < r {
			values = append(values, kva[l].Value)
			l++
		}
		output := reducef(kva[r-1].Key, values)
		fmt.Fprintf(outputFile, "%v %v\n", kva[r-1].Key, output)
	}
	ok := call("Coordinator.FinishReduceTask", &FinishReduceTaskArgs{Id: reduceTask.Id}, &FinishReduceTaskReply{})
	if !ok {
		log.Fatal("failed to finish reduce task")
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Each Worker process will in a loop ask the coordinator for a task
	for {
		args := ApplyTaskArgs{}
		reply := ApplyTaskReply{}
		ok := call("Coordinator.DispatchTask", &args, &reply)
		if !ok {
			fmt.Printf("call failed!\n")
			os.Exit(0)
		}
		// log.Printf("Client Received %v\n", reply)

		if reply.Category == MAP {
			doMap(reply.MapTask, mapf)
		} else if reply.Category == REDUCE {
			doReduce(reply.ReduceTask, reducef)
		} else if reply.Category == WAITING {
			time.Sleep(1 * time.Second)
		} else if reply.Category == DONE {
			log.Println("all tasks completed successfully, now exit")
			os.Exit(0)
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	// args := ExampleArgs{}

	// fill in the argument(s).
	// args.X = 99

	// declare a reply structure.
	// reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	// ok := call("Coordinator.Example", &args, &reply)
	// if ok {
	// 	// reply.Y should be 100.
	// 	fmt.Printf("reply.Y %v\n", reply.Y)
	// } else {
	// 	fmt.Printf("call failed!\n")
	// }
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err == nil
	// if err == nil {
	// 	return true
	// }
	// return false
}
