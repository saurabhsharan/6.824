package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Name      string // Either "Get", "Put", or "Append"
	ClientId  int64
	RequestId int64
	Key       string
	Value     string // empty if Name is "Get"
}

type PendingOp struct {
	op       Op
	term     int
	index    int
	doneChan chan string
	failChan chan bool
}

type ClientResult struct {
	requestId int64
	value     string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	kvMap            map[string]string
	lastClientResult map[int64]ClientResult // map from client id -> result of client's most recent Get()
	pendingOps       map[int]PendingOp      // map from commit index -> pending op struct
}

func makePendingOp(op Op, term int, index int) PendingOp {
	var pendingOp PendingOp
	pendingOp.op = op
	pendingOp.term = term
	pendingOp.index = index
	pendingOp.doneChan = make(chan string, 1)
	pendingOp.failChan = make(chan bool, 1)
	return pendingOp
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Check for duplicate client request
	kv.mu.Lock()
	clientResult, ok := kv.lastClientResult[args.ClientId]
	if ok && clientResult.requestId == args.RequestId {
		reply.WrongLeader = false
		reply.Err = OK
		reply.Value = clientResult.value
		kv.mu.Unlock()
		return
	}

	op := Op{"Get", args.ClientId, args.RequestId, args.Key, ""}
	kv.mu.Unlock()
	index, term, isLeader := kv.rf.Start(op)
	kv.mu.Lock()

	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}

	DPrintf("[kvserver %d] proposed request %d from client %d at index %d", kv.me, args.RequestId, args.ClientId, index)
	pendingOp := makePendingOp(op, term, index)

	oldPendingOp, ok := kv.pendingOps[index]
	if ok {
		// Allow existing RPC call to complete
		DPrintf("[kvserver %d] ABOUT TO clear existing pending op Get() at index %d", kv.me, index)
		oldPendingOp.failChan <- true
		DPrintf("[kvserver %d] DONE clearing existing pending op Get() at index %d", kv.me, index)
	}
	kv.pendingOps[index] = pendingOp
	kv.mu.Unlock()

	select {
	case result := <-pendingOp.doneChan:
		DPrintf("[kvserver %d] returning Get(%s) => %s", kv.me, args.Key, result)
		reply.WrongLeader = false
		reply.Value = result
		reply.Err = OK
	case <-pendingOp.failChan:
		reply.WrongLeader = true
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Check for duplicate client request
	kv.mu.Lock()
	clientResult, ok := kv.lastClientResult[args.ClientId]
	if ok && clientResult.requestId == args.RequestId {
		reply.WrongLeader = false
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	op := Op{args.Op, args.ClientId, args.RequestId, args.Key, args.Value}
	kv.mu.Unlock()
	index, term, isLeader := kv.rf.Start(op)
	kv.mu.Lock()

	if !isLeader {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}

	pendingOp := makePendingOp(op, term, index)

	oldPendingOp, ok := kv.pendingOps[index]
	if ok {
		// Allow existing RPC call to complete
		DPrintf("[kvserver %d] ABOUT TO clear existing pending op PutAppend() at index %d", kv.me, index)
		oldPendingOp.failChan <- true
		DPrintf("[kvserver %d] DONE clearing existing pending op PutAppend() at index %d", kv.me, index)
	}
	kv.pendingOps[index] = pendingOp
	kv.mu.Unlock()

	select {
	case <-pendingOp.doneChan:
		reply.WrongLeader = false
		reply.Err = OK
	case <-pendingOp.failChan:
		reply.WrongLeader = true
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// Blocks on applyCh. Should always be run in new goroutine.
func (kv *KVServer) ApplyCommands() {
	for {
		applyNext := <-kv.applyCh

		kv.mu.Lock()

		if !applyNext.CommandValid {
			kv.mu.Unlock()
			continue
		}

		// DPrintf("[%d] got apply %v", kv.me, applyNext)

		op := applyNext.Command.(Op)
		// Extract fields from command
		// var key, newValue string
		// var clientId, requestId int64
		// opName := op.Name
		// if opName == "Get" {
		//     args := op.Args.(GetArgs)
		//     key = args.Key
		//     clientId = args.ClientId
		//     requestId = args.RequestId
		// } else if opName == "PutAppend" {
		//     args := op.Args.(PutAppendArgs)
		//     if args.Op == "Put" {
		//         opName = "Put"
		//     } else if args.Op == "Append" {
		//         opName = "Append"
		//     }
		//     key = args.Key
		//     newValue = args.Value
		//     clientId = args.ClientId
		//     requestId = args.RequestId
		// }

		// Find pending op, if one exists
		pendingOp, havePendingOp := kv.pendingOps[applyNext.CommandIndex]

		if havePendingOp {
			DPrintf("[kvserver %d] has pending op %s at index %d", kv.me, op.Name, applyNext.CommandIndex)
		}

		// Check for term inconsistency
		if havePendingOp && pendingOp.term != applyNext.CommandTerm {
			DPrintf("[kvserver %d] ABOUT TO clear term conflicting pending op at index %d", kv.me, applyNext.CommandIndex)
			pendingOp.failChan <- true
			DPrintf("[kvserver %d] DONE clear term conflicting pending op at index %d", kv.me, applyNext.CommandIndex)
			delete(kv.pendingOps, applyNext.CommandIndex)
			kv.mu.Unlock()
			continue
		}

		currentValue, ok := kv.kvMap[op.Key]

		if op.Name == "Get" {
			if !ok {
				currentValue = ""
			}
			if havePendingOp {
				pendingOp.doneChan <- currentValue
				DPrintf("[kvserver %d] wrote value to done channel", kv.me)
			}
			kv.lastClientResult[op.ClientId] = ClientResult{op.RequestId, currentValue}
		} else if op.Name == "Put" {
			kv.kvMap[op.Key] = op.Value
			if havePendingOp {
				pendingOp.doneChan <- op.Value
			}
			kv.lastClientResult[op.ClientId] = ClientResult{op.RequestId, ""}
		} else if op.Name == "Append" {
			newValue := op.Value
			if ok {
				newValue = currentValue + newValue
			}
			kv.kvMap[op.Key] = newValue
			if havePendingOp {
				pendingOp.doneChan <- newValue
			}
			kv.lastClientResult[op.ClientId] = ClientResult{op.RequestId, ""}
		}

		kv.mu.Unlock()
	}
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

	kv.kvMap = make(map[string]string)
	kv.lastClientResult = make(map[int64]ClientResult)
	kv.pendingOps = make(map[int]PendingOp)

	kv.applyCh = make(chan raft.ApplyMsg, 10)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.ApplyCommands()

	return kv
}
