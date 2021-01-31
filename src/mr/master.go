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

type TaskStatus int

const (
	TaskStatus_Idle       = TaskStatus(1)
	TaskStatus_InProgress = TaskStatus(2)
	TaskStatus_Finished   = TaskStatus(3)
)

type MapTask struct {
	TaskID         int
	InputFile      string
	Status         TaskStatus
	FinishedBy     *int64
	OutputFiles    []string
	NowExecutingBy int64
}

type ReduceTask struct {
	TaskID         int
	InputFiles     []string
	NowExecutingBy int64
	FinishedBy     *int64
	Status         TaskStatus
}

type Master struct {
	// Your definitions here.
	nTotalMapTaskNum, nTotalReduceTaskNum               int
	mapTaskList                                         []*MapTask
	finishedMapTaskIDList                               []int64
	unfinishedMapTaskIDList, unfinishedReduceTaskIDList []int
	mapTaskMap                                          map[int]*MapTask
	reduceTaskList                                      []*ReduceTask
	reduceTaskMap                                       map[int]*ReduceTask
	nIdleMapTaskNum, nIdleReduceTaskNum                 int
	nFinishedMapTaskNum, nFinishedReduceTaskNum         int
	commonLog                                           log.Logger
	mutex                                               sync.Mutex
	intermediateFiles                                   map[int][]string
	workerBeatTickerMap                                 map[int64]*time.Ticker
	workerTaskMap                                       map[int64]interface{}
	nReduce                                             int
	mapTaskTickerMap                                    map[int]*time.Ticker
	reduceTaskTickerMap                                 map[int]*time.Ticker
}

const BeatMaxInterval = time.Second * 10

// Your code here -- RPC handlers for the worker to call.
func (m *Master) AskForTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	m.mutex.Lock()
	m.commonLog.Printf("AskForTask, nFinishedMapTask=%d, nFinishedReduceTask=%d, nIdleMapTask=%d, "+
		"nIdleReduceTask=%d", m.nFinishedMapTaskNum, m.nFinishedReduceTaskNum, m.nIdleMapTaskNum, m.nIdleReduceTaskNum)
	if _, exist := m.workerBeatTickerMap[args.WorkerID]; exist {
		m.workerBeatTickerMap[args.WorkerID].Stop()
	}
	m.workerBeatTickerMap[args.WorkerID] = time.NewTicker(BeatMaxInterval)
	reply.NReduce = m.nReduce
	if m.nIdleMapTaskNum > 0 {
		reply.Command = Map
		oneUnfinishedMapTask := m.mapTaskMap[m.unfinishedMapTaskIDList[0]]
		m.unfinishedMapTaskIDList = m.unfinishedMapTaskIDList[1:]
		m.nIdleMapTaskNum--
		reply.TaskID = oneUnfinishedMapTask.TaskID
		reply.File = []string{oneUnfinishedMapTask.InputFile}
		oneUnfinishedMapTask.NowExecutingBy = args.WorkerID
		oneUnfinishedMapTask.Status = TaskStatus_InProgress
		m.workerTaskMap[args.WorkerID] = oneUnfinishedMapTask
		m.mapTaskTickerMap[oneUnfinishedMapTask.TaskID] = time.NewTicker(BeatMaxInterval)

		m.commonLog.Printf("Map task %d assigned to worker %d, file name %s", oneUnfinishedMapTask.TaskID, args.WorkerID,
			oneUnfinishedMapTask.InputFile)
	} else if (m.nFinishedMapTaskNum >= m.nTotalMapTaskNum) && (m.nIdleReduceTaskNum > 0) {
		reply.Command = Reduce
		//m.mutex.Lock()
		oneReduceTask := m.reduceTaskList[0]
		m.reduceTaskList = m.reduceTaskList[1:]
		oneReduceTask.NowExecutingBy = args.WorkerID
		oneReduceTask.Status = TaskStatus_InProgress
		m.workerTaskMap[args.WorkerID] = oneReduceTask
		m.nIdleReduceTaskNum--
		m.reduceTaskTickerMap[oneReduceTask.TaskID] = time.NewTicker(BeatMaxInterval)
		//m.mutex.Unlock()
		m.commonLog.Printf("Reduce task %d assigned to worker %d, file names: %v", oneReduceTask.TaskID,
			args.WorkerID, oneReduceTask.InputFiles)
		reply.File = oneReduceTask.InputFiles
		reply.TaskID = oneReduceTask.TaskID
	} else if (m.nFinishedMapTaskNum < m.nTotalMapTaskNum) || (m.nFinishedReduceTaskNum < m.nTotalReduceTaskNum) {
		reply.Command = DoNothing
	} else {
		reply.Command = Terminate
		//m.mutex.Lock()
		if _, exist := m.workerBeatTickerMap[args.WorkerID]; exist {
			ticker := m.workerBeatTickerMap[args.WorkerID]
			ticker.Stop()
			delete(m.workerBeatTickerMap, args.WorkerID)
		}
		//m.mutex.Unlock()
	}
	m.mutex.Unlock()
	return nil
}

func (m *Master) ReturnMapTaskOutput(args *ReturnMapTaskOutputArgs, reply *ReturnMapTaskOutputReply) error {
	m.mutex.Lock()
	if _, exist := m.workerBeatTickerMap[args.WorkerID]; exist {
		m.workerBeatTickerMap[args.WorkerID].Stop()
	}
	m.workerBeatTickerMap[args.WorkerID] = time.NewTicker(BeatMaxInterval)
	m.mutex.Unlock()

	reply = &ReturnMapTaskOutputReply{}

	m.mutex.Lock()
	finishedMapTask := m.mapTaskMap[args.TaskID]
	if finishedMapTask.FinishedBy != nil {
		m.commonLog.Printf("Worker %d finished map task %d again, which is already finished by %d", args.WorkerID,
			args.TaskID, *finishedMapTask.FinishedBy)
		m.mutex.Unlock()
		return nil
	}
	finishedMapTask.FinishedBy = &args.WorkerID
	finishedMapTask.OutputFiles = args.IntermediateFiles
	finishedMapTask.Status = TaskStatus_Finished
	m.mutex.Unlock()
	for _, intermediateFile := range args.IntermediateFiles {
		mapTaskID, bucketID := parseIntermediateFileName(intermediateFile)
		m.commonLog.Printf("Worker %d returned map task %d result, in bucket %d, %s added", args.WorkerID, mapTaskID,
			bucketID, intermediateFile)
		m.mutex.Lock()

		if _, exist := m.intermediateFiles[bucketID]; !exist {
			m.intermediateFiles[bucketID] = []string{intermediateFile}
		} else {
			m.intermediateFiles[bucketID] = append(m.intermediateFiles[bucketID], intermediateFile)
		}
		if len(m.intermediateFiles[bucketID]) == m.nTotalMapTaskNum {
			m.commonLog.Printf("Intermediate files for reduce task %d is ready, files are %v", bucketID,
				m.intermediateFiles[bucketID])
			newReduceTask := &ReduceTask{
				TaskID:     bucketID,
				InputFiles: m.intermediateFiles[bucketID],
			}
			m.reduceTaskMap[bucketID] = newReduceTask
			m.reduceTaskList = append(m.reduceTaskList, newReduceTask)
			m.unfinishedReduceTaskIDList = append(m.unfinishedReduceTaskIDList, newReduceTask.TaskID)
		}
		m.mutex.Unlock()
	}
	m.mutex.Lock()
	m.nFinishedMapTaskNum++
	m.commonLog.Printf("m.nFinishedMapTaskNum=%d", m.nFinishedMapTaskNum)
	ticker := m.mapTaskTickerMap[args.TaskID]
	ticker.Stop()
	delete(m.mapTaskTickerMap, args.TaskID)
	m.mutex.Unlock()

	return nil
}

func (m *Master) ReturnReduceTaskOutput(args *ReturnReduceTaskOutputArgs, reply *ReturnReduceTaskOutputReply) error {

	m.mutex.Lock()
	if _, exist := m.workerBeatTickerMap[args.WorkerID]; exist {
		m.workerBeatTickerMap[args.WorkerID].Stop()
	}
	m.workerBeatTickerMap[args.WorkerID] = time.NewTicker(BeatMaxInterval)
	m.mutex.Unlock()

	reply = &ReturnReduceTaskOutputReply{}

	m.mutex.Lock()
	finishedReduceTask := m.reduceTaskMap[args.TaskID]
	if finishedReduceTask.FinishedBy != nil {
		m.commonLog.Printf("Worker %d finished reduce task %d, which is already finished by worker %d",
			args.WorkerID, args.TaskID, *finishedReduceTask.FinishedBy)
		return nil
	}
	finishedReduceTask.FinishedBy = &args.WorkerID
	m.nFinishedReduceTaskNum++
	ticker := m.reduceTaskTickerMap[args.TaskID]
	ticker.Stop()
	delete(m.reduceTaskTickerMap, args.TaskID)
	m.commonLog.Printf("m.nFinishedReduceTaskNum=%d", m.nFinishedReduceTaskNum)
	m.mutex.Unlock()
	m.commonLog.Printf("Reduce task %d is finished by worker %d, output file is %v", args.TaskID, args.WorkerID,
		args.OutputFile)
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	m.mutex.Lock()

	if m.nFinishedMapTaskNum == m.nTotalMapTaskNum && m.nFinishedReduceTaskNum == m.nTotalReduceTaskNum && len(m.
		mapTaskTickerMap) == 0 && len(m.reduceTaskTickerMap) == 0 {
		m.mutex.Unlock()
		return true
	}
	m.mutex.Unlock()

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.nTotalMapTaskNum = len(files)
	m.nTotalReduceTaskNum = nReduce
	m.nIdleMapTaskNum = len(files)
	m.nIdleReduceTaskNum = nReduce
	m.mapTaskMap = make(map[int]*MapTask)
	m.reduceTaskMap = make(map[int]*ReduceTask)
	m.workerBeatTickerMap = make(map[int64]*time.Ticker)
	m.workerTaskMap = make(map[int64]interface{})
	m.nReduce = nReduce
	m.mapTaskTickerMap = make(map[int]*time.Ticker)
	m.reduceTaskTickerMap = make(map[int]*time.Ticker)

	// 加载输入文件，新建任务
	for i, file := range files {
		newMapTask := &MapTask{
			TaskID:    i,
			InputFile: file,
			Status:    TaskStatus_Idle,
		}
		m.mapTaskList = append(m.mapTaskList, newMapTask)
		m.unfinishedMapTaskIDList = append(m.unfinishedMapTaskIDList, newMapTask.TaskID)
		m.mapTaskMap[newMapTask.TaskID] = newMapTask
	}

	// 设置log文件
	logFile, err := os.Create("master-log")
	if err != nil {
		panic(fmt.Sprintf("set master log failed, err=%s", err))
	}
	m.commonLog.SetOutput(logFile)
	m.commonLog.SetFlags(log.Ltime)

	// 中间文件的二维数组
	m.intermediateFiles = make(map[int][]string)
	m.commonLog.Printf("Master init succeeded, m.nReduce=%d", m.nReduce)

	//go watchWorkerHealth(&m)
	go watchTaskExecutedTime(&m)
	m.server()
	return &m
}

// 每隔一定时间，检查任务执行时长是否超时，
// 如果超时，则将任务回收，重新分配
func watchTaskExecutedTime(m *Master) {
	m.commonLog.Printf("goroutine watchTaskExecutedTime is running")
	for {
		time.Sleep(BeatMaxInterval)
		m.mutex.Lock()
		for mapTaskID, ticker := range m.mapTaskTickerMap {
			if len(ticker.C) > 0 {
				loseConnectionTime := <-ticker.C
				m.commonLog.Printf("Map task %d wasn't finished in 10s, at time %s", mapTaskID,
					loseConnectionTime.Format(time.RFC3339))
				delete(m.mapTaskTickerMap, mapTaskID)

				mapTask := m.mapTaskMap[mapTaskID]
				m.unfinishedMapTaskIDList = append(m.unfinishedMapTaskIDList, mapTask.TaskID)
				m.mapTaskList = append(m.mapTaskList, mapTask)
				m.nIdleMapTaskNum++
			}
		}

		for reduceTaskID, ticker := range m.reduceTaskTickerMap {
			if len(ticker.C) > 0 {
				loseConnectionTime := <-ticker.C
				m.commonLog.Printf("Reduce task %d wasn't finished in 10s, at time %s", reduceTaskID,
					loseConnectionTime.Format(time.RFC3339))
				delete(m.reduceTaskTickerMap, reduceTaskID)
				reduceTask := m.reduceTaskMap[reduceTaskID]
				m.unfinishedReduceTaskIDList = append(m.unfinishedReduceTaskIDList, reduceTask.TaskID)
				m.reduceTaskList = append(m.reduceTaskList, reduceTask)
				m.nIdleReduceTaskNum++
			}

		}
		m.mutex.Unlock()
	}
}

func watchWorkerHealth(m *Master) {
	m.commonLog.Printf("goroutine watchWorkerHealth is running")
	for {
		time.Sleep(BeatMaxInterval)
		m.mutex.Lock()
		for workerID, ticker := range m.workerBeatTickerMap {
			if len(ticker.C) > 0 {
				loseConnectionTime := <-ticker.C
				m.commonLog.Printf("Worker %d lose connection at %s", workerID, loseConnectionTime.Format(time.RFC3339))
				delete(m.workerBeatTickerMap, workerID)

				if _, exist := m.workerTaskMap[workerID]; exist {
					task := m.workerTaskMap[workerID]
					if _, ok := task.(*MapTask); ok {
						mapTask := task.(*MapTask)
						m.unfinishedMapTaskIDList = append(m.unfinishedMapTaskIDList, mapTask.TaskID)
						m.mapTaskList = append(m.mapTaskList, mapTask)
						m.nIdleMapTaskNum++
					} else if _, ok = task.(*ReduceTask); ok {
						reduceTask := task.(*ReduceTask)
						m.unfinishedReduceTaskIDList = append(m.unfinishedReduceTaskIDList, reduceTask.TaskID)
						m.reduceTaskList = append(m.reduceTaskList, reduceTask)
						m.nIdleReduceTaskNum++
					}
					delete(m.workerTaskMap, workerID)
				}
			}
		}
		m.mutex.Unlock()
	}
}
