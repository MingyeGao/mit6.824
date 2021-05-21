package kvraft

import (
	"../labrpc"
	"fmt"
	"log"
	"os"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lab3ALogger *log.Logger
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	logFile, err := os.Create(fmt.Sprintf("raft-log3A-Clerk-%d.txt", nrand()))
	ck.lab3ALogger = log.New(logFile, "", log.Ltime|log.Lmicroseconds)
	if err != nil {
		log.Panicf("create log failed, error=%v", err)
	}
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	result := ""
	args := &GetArgs{
		Key: key,
		IdempotentID: int(nrand()),
	}

	ck.lab3ALogger.Printf("Going to Get, key=%s", key)
	for {
		var gotReply bool
		for i, _ := range ck.servers {
			reply := &GetReply{}
			ok := ck.sendGetRpc(i, args, reply)
			ck.lab3ALogger.Printf("[Get %s] from server %d, ok=%v", key, i, ok)
			if !ok {
				continue
			}
			if reply.Err == ErrWrongLeader {
				ck.lab3ALogger.Printf("Get wrong leader %d, cmd:[Get %s]", i, key)
				continue
			}
			gotReply = true
			if reply.Err == ErrNoKey {
				ck.lab3ALogger.Printf("Get ErrNoKey, cmd:[Get %s]", key)
				break
			}
			if reply.Err == OK {
				ck.lab3ALogger.Printf("Get OK, leader=%d, cmd:[Get %s], result=%s", i, key, reply.Value)
				result = reply.Value
				break
			}
		}
		if gotReply {
			break
		}
	}
	// You will have to modify this function.
	return result
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{
		Key:          key,
		Value:        value,
		Op:           op,
		IdempotentID: int(nrand()),
	}

	ck.lab3ALogger.Printf("Going to PutAppend, key=%s, value=%s, op=%s", key, value, op)
	for {
		var gotReply bool
		for i, _ := range ck.servers {
			reply := &PutAppendReply{}
			ok := ck.sendPutAppendRpc(i, args, reply)
			ck.lab3ALogger.Printf("[%s %s %s] to server%d, ok=%v", op, key, value, i, ok)
			if !ok {
				continue
			}
			if reply.Err == ErrWrongLeader {
				ck.lab3ALogger.Printf("PutAppend wrong leader %d, cmd:[%s %s %s]", i, op, key, value)
				continue
			}
			gotReply = true
			if reply.Err == OK {
				ck.lab3ALogger.Printf("PutAppend ok, leader=%d, cmd:[%s %s %s]", i, op, key, value)
				break
			}
		}
		if gotReply {
			break
		}
	}
}

func (ck *Clerk) sendGetRpc(serverIndex int, args *GetArgs, reply *GetReply) (ok bool) {
	ch := make(chan bool)
	alarm := time.After(rpcTimeout)
	go func(ch chan bool) {
		ok := ck.servers[serverIndex].Call("KVServer.Get", args, reply)
		ch <- ok
	}(ch)

	select {
	case <-alarm:
		ck.lab3ALogger.Printf("Get to %d timeout", serverIndex)
		ok = false
	case ok = <-ch:
	}
	return
}

func (ck *Clerk) sendPutAppendRpc(serverIndex int, args *PutAppendArgs, reply *PutAppendReply) (ok bool) {
	ch := make(chan bool)
	alarm := time.After(rpcTimeout)
	go func(ch chan bool) {
		ok := ck.servers[serverIndex].Call("KVServer.PutAppend", args, reply)
		ch <- ok
	}(ch)

	select {
	case <-alarm:
		ck.lab3ALogger.Printf("PutAppend to %d timeout", serverIndex)
		ok = false
	case ok = <-ch:
	}
	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
