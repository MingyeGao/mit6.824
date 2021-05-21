package kvraft

import "fmt"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrRaftTimeout = "ErrRaftTimeout"
	ErrInternal    = "ErrInternal"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	IdempotentID int
}

func (args *PutAppendArgs) Cmd() string {
	return fmt.Sprintf("[%s, %s, %s]", args.Op, args.Key, args.Value)
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	IdempotentID int
}

type GetReply struct {
	Err   Err
	Value string
}
