package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"container/list"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation    string
	Key          string
	Value        string
	RespCh       chan raftResp
	ServerID     int //处理rpc的serverID；如果msgCh中得到的消息中，serverID不等于me，则不向ch发送
	IdempotentID int
	ServerUniqueID string
}

type raftResp struct {
	Value    string
	Succeed  bool
	KeyExist bool
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	database                 map[string]string
	lab3ALogger              *log.Logger
	rpcIdempotentIDMap       map[int]Err
	rpcIdempotentIDQueue     *list.List
	rpcIdempotentIDQueueSize int
	cmdIdempotentIDMap       map[int]idempotentEntry
	cmdIdempotentIDQueue     *list.List
	cmdIdempotentIDQueueSize int
	uniqueID string
}

type idempotentEntry struct {
	resp        raftResp
	hasReturned bool // 是否已响应过该指令的请求
}

const rpcTimeout = time.Millisecond * 1000

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.lab3ALogger.Printf("Going to get, key=%s", args.Key)
	//if _, isLeader := kv.rf.GetState(); !isLeader {
	//	kv.lab3ALogger.Printf("%d is not leader", kv.me)
	//	reply.Err = ErrWrongLeader
	//	return
	//}
	//kv.lab3ALogger.Printf("%d is leader", kv.me)
	op := Op{
		Operation:    "Get",
		Key:          args.Key,
		ServerID:     kv.me,
		IdempotentID: args.IdempotentID,
		ServerUniqueID: kv.uniqueID,
	}
	op.RespCh = make(chan raftResp, 10)
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.lab3ALogger.Printf("%d is not leader", kv.me)
		reply.Err = ErrWrongLeader
		return
	}
	//timer := time.After(rpcTimeout)
	//select {
	raftResult := <-op.RespCh
	kv.lab3ALogger.Printf("Get %s read RespCh, value=%s", args.Key, raftResult.Value)
	if raftResult.KeyExist {
		reply.Err = OK
		reply.Value = raftResult.Value
	} else {
		reply.Err = ErrNoKey
	}
	kv.lab3ALogger.Printf("Get %s rpc succeed, reply.Err=%s, reply.Value=%s", args.Key, reply.Err, reply.Value)
	//case <-timer:
	//	kv.lab3ALogger.Printf("Get %s timeout, command:[Get %s]", args.Key, args.Key)
	//	reply.Err = ErrRaftTimeout
	//}
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.lab3ALogger.Printf("Going to PutAppend, key=%s, value=%s, operation=%s", args.Key, args.Value, args.Op)
	if _, isLeader := kv.rf.GetState(); !isLeader {
		kv.lab3ALogger.Printf("PutAppend WrongLeader, cmd:%s", args.Cmd())
		reply.Err = ErrWrongLeader
		return
	}
	//if kv.rpcIdempotentCheck(args.IdempotentID) {
	//	kv.lab3ALogger.Printf("PutAppend req duplicate, cmd:%s", args.Cmd())
	//	reply.Err = OK
	//	return
	//}
	op := Op{
		Operation:    args.Op,
		Value:        args.Value,
		Key:          args.Key,
		ServerID:     kv.me,
		IdempotentID: args.IdempotentID,
		ServerUniqueID: kv.uniqueID,
	}
	op.RespCh = make(chan raftResp, 10)
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	//timer := time.After(rpcTimeout)
	//select {
	//case <-op.RespCh:
	<-op.RespCh
	kv.lab3ALogger.Printf("PutAppend [%s, %s, %s] read RespCh", args.Op, args.Key, args.Value)
	reply.Err = OK
	kv.lab3ALogger.Printf("PutAppend [%s, %s, %s] succeed, reply.Err=%s", args.Op, args.Key, args.Value, reply.Err)
	//case <-timer:
	//	kv.lab3ALogger.Printf("PutAppend [%s, %s, %s] timeout", args.Op, args.Key, args.Value)
	//	reply.Err = ErrRaftTimeout
	//}
	//kv.rpcIdempotentRecord(args.IdempotentID, reply)
}

func (kv *KVServer) rpcIdempotentCheck(idempotentID int) (isDuplicate bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, exist := kv.rpcIdempotentIDMap[idempotentID]; exist {
		if kv.rpcIdempotentIDMap[idempotentID] == OK {
			isDuplicate = true
		}
	}

	return
}

func (kv *KVServer) generateUniqueID() string {
	return fmt.Sprintf("%d_%d", time.Now().UnixNano(), kv.me)
}

func (kv *KVServer) cmdIdempotentCheck(idempotentID int) (isDuplicate bool, info idempotentEntry) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, exist := kv.cmdIdempotentIDMap[idempotentID]; exist {
		isDuplicate = true
		info = kv.cmdIdempotentIDMap[idempotentID]
	}
	return
}

func (kv *KVServer) cmdIdempotentRecord(idempotentID int, resp raftResp) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.cmdIdempotentIDQueue.Len() >= kv.cmdIdempotentIDQueueSize {
		frontIdempotentID := kv.cmdIdempotentIDQueue.Front().Value.(int)
		delete(kv.cmdIdempotentIDMap, frontIdempotentID)
		kv.cmdIdempotentIDQueue.Remove(kv.cmdIdempotentIDQueue.Front())
	}
	kv.cmdIdempotentIDQueue.PushBack(idempotentID)
	kv.cmdIdempotentIDMap[idempotentID] = idempotentEntry{
		resp:        resp,
		hasReturned: false,
	}
}

func (kv *KVServer) hasCmdReturned(idempotentID int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	idempotentInfo := kv.cmdIdempotentIDMap[idempotentID]
	return idempotentInfo.hasReturned
}

func (kv *KVServer) rpcIdempotentRecord(idempotentID int, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// id之前已存在，直接更新错误即可
	if _, exist := kv.rpcIdempotentIDMap[idempotentID]; exist {
		kv.rpcIdempotentIDMap[idempotentID] = reply.Err
		return
	}
	if len(kv.rpcIdempotentIDMap) > kv.rpcIdempotentIDQueueSize {
		oldestID := kv.rpcIdempotentIDQueue.Remove(kv.rpcIdempotentIDQueue.Front()).(int)
		delete(kv.rpcIdempotentIDMap, oldestID)
	}
	kv.rpcIdempotentIDQueue.PushBack(idempotentID)
	kv.rpcIdempotentIDMap[idempotentID] = reply.Err
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.uniqueID = kv.generateUniqueID()
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.database = make(map[string]string)
	logFile, err := os.Create(fmt.Sprintf("raft-log3A-%d.txt", me))
	kv.lab3ALogger = log.New(logFile, "", log.Ltime|log.Lmicroseconds)
	if err != nil {
		log.Panicf("create log failed, error=%v", err)
	}
	kv.rpcIdempotentIDMap = make(map[int]Err)
	kv.rpcIdempotentIDQueue = list.New()
	kv.rpcIdempotentIDQueueSize = 10000

	kv.cmdIdempotentIDMap = make(map[int]idempotentEntry)
	kv.cmdIdempotentIDQueue = list.New()
	kv.cmdIdempotentIDQueueSize = 10000

	time.Sleep(time.Second)

	go databaseRoutine(kv)

	return kv
}

func databaseRoutine(server *KVServer) {
	for {
		if server.killed() {
			return
		}
		msg := <-server.applyCh
		op := msg.Command.(Op)
		server.lab3ALogger.Printf("from applyCh [%v]:[%v,%v,%v]", msg.CommandIndex, op.Operation, op.Key, op.Value)

		var resp raftResp
		isDuplicate, idempotentInfo := server.cmdIdempotentCheck(op.IdempotentID)
		if isDuplicate {
			resp = idempotentInfo.resp
		} else {
			result, keyExist := parseCommand(server, op)
			server.lab3ALogger.Printf("[gmy]before send to RespCh, len=%d", len(op.RespCh))
			resp = raftResp{
				Value:    result,
				KeyExist: keyExist,
			}
			server.cmdIdempotentRecord(op.IdempotentID, resp)
		}

		if op.ServerUniqueID == server.uniqueID {
			server.lab3ALogger.Printf("[gmy]isLeader, result=%v, keyExist=%v", resp.Value,
				resp.KeyExist)
			op.RespCh <- resp
		}
		server.lab3ALogger.Printf("[gmy]after send to RespCh")
	}
}

func parseCommand(server *KVServer, op Op) (result string, keyExist bool) {
	switch op.Operation {
	case "Get":
		result, keyExist = parseGetCommand(server, op)
	case "Put":
		parsePutCommand(server, op)
	case "Append":
		parseAppendCommand(server, op)
	}
	return
}

func parseGetCommand(server *KVServer, op Op) (result string, keyExist bool) {
	server.mu.Lock()
	defer server.mu.Unlock()
	server.lab3ALogger.Printf("GetCommand database:%v", server.database)
	value, exist := server.database[op.Key]
	result = value
	keyExist = exist
	server.lab3ALogger.Printf("Get [%s], value=%s, keyExist=%v", op.Key, result, keyExist)
	return
}

func parsePutCommand(server *KVServer, op Op) {
	server.mu.Lock()
	defer server.mu.Unlock()
	server.database[op.Key] = op.Value
	server.lab3ALogger.Printf("parsePutCommand Put [%s]", op.Key)
	server.lab3ALogger.Printf("PutCommand database:%v", server.database)
	return
}

func parseAppendCommand(server *KVServer, op Op) (result string, keyExist bool) {
	server.mu.Lock()
	defer server.mu.Unlock()
	originValue := server.database[op.Key]
	newValue := originValue + op.Value
	server.database[op.Key] = newValue
	server.lab3ALogger.Printf("Append [%s], originValue=%s, newValue=%s", op.Key, originValue, newValue)
	server.lab3ALogger.Printf("AppendCommand database:%v", server.database)
	return
}
