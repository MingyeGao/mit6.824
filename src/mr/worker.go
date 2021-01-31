package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
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


type WorkerData struct {
	logger log.Logger
	ID int64
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	w := &WorkerData{}
	w.ID = time.Now().UnixNano()
	logFile, _ := os.Create(fmt.Sprintf("worker-log-%d", w.ID))
	w.logger.SetOutput(logFile)
	w.logger.SetFlags(log.Ltime)
	counter := 0
	for {
		counter++
		reply := w.AskForTask()
		w.logger.Printf("Worker %d, counter=%d", w.ID, counter)
		w.logger.Printf("Worker %d ask for command %v, taskID %d, fileName %v, nReduce=%d", w.ID, reply.Command,
			reply.TaskID, reply.File, reply.NReduce)
		switch reply.Command {
		case Map:
			intermediateFiles := w.WorkerDoMapTask(mapf, reply.File[0], reply.TaskID, reply.NReduce)
			returnMapTaskOutputArgs := &ReturnMapTaskOutputArgs{
				TaskID: reply.TaskID,
				WorkerID: w.ID,
				IntermediateFiles: intermediateFiles,
			}
			returnMapTaskOutputReply := &ReturnMapTaskOutputReply{}
			ret := call("Master.ReturnMapTaskOutput", returnMapTaskOutputArgs, returnMapTaskOutputReply)
			w.logger.Printf("Worker %d finished map task %d, rpc ret is %v", w.ID, reply.TaskID, ret)
		case Reduce:
			outputFile := w.WorkerDoReduceTask(reducef, reply.File, reply.TaskID)
			returnReduceTaskOutputArgs := &ReturnReduceTaskOutputArgs{
				WorkerID: w.ID,
				OutputFile: outputFile,
				TaskID: reply.TaskID,
			}
			returnReduceTaskOutputReply := &ReturnReduceTaskOutputReply{}
			call("Master.ReturnReduceTaskOutput", returnReduceTaskOutputArgs, returnReduceTaskOutputReply)
		case DoNothing:
			w.WorkerDoNothing()
		case Terminate:
			w.WorkerTerminateTask()
		}


	}


	// uncomment to send the Example RPC to the master.
	//CallExample()

}

func (w *WorkerData) WorkerDoMapTask(mapf func(string, string) []KeyValue, fileName string, mapTaskID int,
	nReduce int) (intermediateFiles []string) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))
	bucketKvData := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		bucketIndex := ihash(kv.Key) % nReduce
		bucketKvData[bucketIndex] = append(bucketKvData[bucketIndex], kv)
	}

	for bucketIndex, oneBucketData := range bucketKvData {
		intermediateFileName := genIntermediateFileName(mapTaskID, bucketIndex)
		tmpFileFd, _ := ioutil.TempFile("./", intermediateFileName)
		enc := json.NewEncoder(tmpFileFd)
		for _, kv := range oneBucketData {
			enc.Encode(&kv)
		}
		os.Rename(tmpFileFd.Name(), intermediateFileName)
		intermediateFiles = append(intermediateFiles, intermediateFileName)
	}
	return
}

func (w *WorkerData) WorkerDoReduceTask(reducef func(string, []string) string, intermediateFiles []string,
	bucketIndex int) (outputFile string){
	outputFile = genFinalOutputFileName(bucketIndex)
	tmpOutputFileFd, _ := ioutil.TempFile("./", outputFile)
	var kva []KeyValue
	for _, intermediateFile := range intermediateFiles {
		fd, _ := os.Open(intermediateFile)
		dec := json.NewDecoder(fd)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpOutputFileFd, "%v %v\n", kva[i].Key, output)

		i = j
	}

	os.Rename(tmpOutputFileFd.Name(), outputFile)
	tmpOutputFileFd.Close()
	return
}

func (w *WorkerData) WorkerTerminateTask() {
	w.logger.Printf("Worker %d received terminate command", w.ID)
	os.Exit(5)
}

func (w *WorkerData) WorkerDoNothing() {
	w.logger.Printf("Worker %d received do nothing command", w.ID)
	time.Sleep(time.Second * 2)
}

func genIntermediateFileName(mapTaskID int, outputBucket int) string {
	return fmt.Sprintf("mr-%d-%d", mapTaskID, outputBucket)
}

func parseIntermediateFileName(fileName string) (workerID int64, outputBucket int) {
	splits := strings.Split(fileName, "-")
	workerIDInt, _ := strconv.Atoi(splits[1])
	workerID = int64(workerIDInt)
	outputBucket, _ = strconv.Atoi(splits[2])
	return
}

func genFinalOutputFileName(outputBucket int) string {
	return fmt.Sprintf("mr-out-%d", outputBucket)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func (w *WorkerData) AskForTask() (reply *AskForTaskReply){
	args := &AskForTaskArgs{
		WorkerID: w.ID,
	}
	reply = &AskForTaskReply{}

	call("Master.AskForTask", &args, &reply)
	return
}


//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
