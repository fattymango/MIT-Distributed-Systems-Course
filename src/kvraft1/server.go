package kvraft

import (
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type value struct {
	value   string
	version rpc.Tversion
}
type KVServer struct {
	me  int
	rsm *rsm.RSM

	// Your definitions here.
	store map[string]value
	mu    sync.Mutex
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
	}
	return nil
}

func (kv *KVServer) doGet(args *rpc.GetArgs) *rpc.GetReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := &rpc.GetReply{}
	if v, ok := kv.store[args.Key]; ok {
		reply.Value = v.value
		reply.Version = v.version
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
		if args.Version == v.version {
			kv.store[args.Key] = value{value: args.Value, version: args.Version + 1}
			reply.Err = rpc.OK
			return reply
		}
		reply.Err = rpc.ErrVersion
		return reply
	}

	if args.Version == 0 {
		kv.store[args.Key] = value{value: args.Value, version: 1}
		reply.Err = rpc.OK
		return reply
	}

	reply.Err = rpc.ErrNoKey
	return reply
}
func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
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

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	kv.store = make(map[string]value)
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
