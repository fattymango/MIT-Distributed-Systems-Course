package kvraft

import (
	"bytes"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type Value struct {
	Value   string
	Version rpc.Tversion
}
type KVServer struct {
	me  int
	rsm *rsm.RSM

	// Your definitions here.
	store map[string]Value
	mu    sync.Mutex
	dedup map[int64]DedupEntry // ClientID
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch r := req.(type) {
	case rpc.GetArgs:
		return kv.doGet(&r)
	case rpc.PutArgs:
		return kv.doPut(&r)
	case rpc.TxnArgs:
		return kv.doTxn(&r)
	}
	return nil
}

func (kv *KVServer) doTxn(args *rpc.TxnArgs) *rpc.TxnReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	entry, ok := kv.dedup[args.ClientId]

	if ok && args.Seq <= entry.Seq {
		return &entry.Reply
	}
	reply := &rpc.TxnReply{}

	reply.Succeeded = true
	for _, c := range args.Compares {
		v := rpc.Tversion(0)
		if key, ok := kv.store[c.Key]; ok {
			v = key.Version
		}
		if c.Version != v {
			reply.Succeeded = false
			break
		}
	}

	var ops []rpc.TxnOp
	if reply.Succeeded {
		ops = args.Then
	} else {
		ops = args.Else
	}

	for _, op := range ops {
		result := rpc.TxnOpResult{}
		switch op.Type {
		case rpc.TxnGet:
			if val, ok := kv.store[op.Key]; ok {
				result = rpc.TxnOpResult{Value: val.Value, Version: val.Version, Err: rpc.OK}
			} else {
				result = rpc.TxnOpResult{Err: rpc.ErrNoKey}
			}
		case rpc.TxnPut:
			version := rpc.Tversion(0)
			if val, ok := kv.store[op.Key]; ok {
				version = val.Version
			}

			kv.store[op.Key] = Value{Value: op.Value, Version: version + 1}
			result = rpc.TxnOpResult{Version: version + 1, Err: rpc.OK, Value: op.Value}
		}
		reply.Results = append(reply.Results, result)
	}

	reply.Err = rpc.OK
	kv.dedup[args.ClientId] = DedupEntry{Seq: args.Seq, Reply: *reply}
	return reply

}

func (kv *KVServer) doGet(args *rpc.GetArgs) *rpc.GetReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := &rpc.GetReply{}
	if v, ok := kv.store[args.Key]; ok {
		reply.Value = v.Value
		reply.Version = v.Version
		reply.Err = rpc.OK
		return reply
	}

	reply.Err = rpc.ErrNoKey
	return reply

}
func (kv *KVServer) doPut(args *rpc.PutArgs) *rpc.PutReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := &rpc.PutReply{}
	if v, ok := kv.store[args.Key]; ok {
		if args.Version == v.Version {
			kv.store[args.Key] = Value{Value: args.Value, Version: args.Version + 1}
			reply.Err = rpc.OK
			return reply
		}
		reply.Err = rpc.ErrVersion
		return reply
	}

	if args.Version == 0 {
		kv.store[args.Key] = Value{Value: args.Value, Version: 1}
		reply.Err = rpc.OK
		return reply
	}

	reply.Err = rpc.ErrNoKey
	return reply
}
func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	encoder := labgob.NewEncoder(w)
	encoder.Encode(kv.store)
	encoder.Encode(kv.dedup)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(r)

	var newStore map[string]Value
	if err := decoder.Decode(&newStore); err != nil {
		panic(err)
	}
	if newStore == nil {
		newStore = make(map[string]Value)
	}
	kv.store = newStore

	var newDedup map[int64]DedupEntry
	if err := decoder.Decode(&newDedup); err != nil || newDedup == nil {
		newDedup = make(map[int64]DedupEntry)
	}
	kv.dedup = newDedup
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	rep, ok := res.(*rpc.GetReply)
	if !ok {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	reply.Err = rep.Err
	reply.Value = rep.Value
	reply.Version = rep.Version
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	// Your code here.
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	rep, ok := res.(*rpc.PutReply)
	if !ok {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	reply.Err = rep.Err
}
func (kv *KVServer) Txn(args *rpc.TxnArgs, reply *rpc.TxnReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	// Your code here.
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	rep, ok := res.(*rpc.TxnReply)
	if !ok {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	reply.Err = rep.Err
	reply.Succeeded = rep.Succeeded
	reply.Results = rep.Results
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(rpc.TxnArgs{})
	kv := &KVServer{me: me}

	kv.store = make(map[string]Value)
	kv.dedup = make(map[int64]DedupEntry)

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
