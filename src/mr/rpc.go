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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskCmd int

const (
	Map       = TaskCmd(1)
	Reduce    = TaskCmd(2)
	DoNothing = TaskCmd(3)
	Terminate = TaskCmd(4)
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type AskForTaskArgs struct {
	WorkerID int64
}

type AskForTaskReply struct {
	TaskID  int
	Command TaskCmd
	File    []string
	NReduce int
	BucketIndex int
}

type ReturnMapTaskOutputArgs struct {
	TaskID int
	WorkerID          int64
	IntermediateFiles []string

}

type ReturnMapTaskOutputReply struct{}

type ReturnReduceTaskOutputArgs struct {
	WorkerID   int64
	OutputFile string
	TaskID     int
}

type ReturnReduceTaskOutputReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
