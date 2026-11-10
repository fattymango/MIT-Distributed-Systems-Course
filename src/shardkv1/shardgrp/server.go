package shardgrp

import (
	"bytes"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const (
	ENVKEY = "65840ENV"
)

type Value struct {
	Value   string
	Version rpc.Tversion
}
type ShardState struct {
	Data   map[string]Value
	Owned  bool
	Frozen bool
	Num    shardcfg.Tnum
}
type KVServer struct {
	me  int
	rsm *rsm.RSM
	gid tester.Tgid

	// Your code here
	mu     sync.Mutex
	shards map[shardcfg.Tshid]*ShardState
}

func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch r := req.(type) {
	case rpc.GetArgs:
		return kv.doGet(&r)
	case rpc.PutArgs:
		return kv.doPut(&r)
	case shardrpc.DeleteShardArgs:
		return kv.doDelete(&r)
	case shardrpc.InstallShardArgs:
		return kv.doInstall(&r)
	case shardrpc.FreezeShardArgs:
		return kv.doFreeze(&r)
	}
	return nil
}
func (kv *KVServer) doDelete(args *shardrpc.DeleteShardArgs) *shardrpc.DeleteShardReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := &shardrpc.DeleteShardReply{}
	if shard, ok := kv.shards[args.Shard]; ok {
		if args.Num < shard.Num {
			reply.Err = rpc.OK
			return reply
		}
		shard.Data = make(map[string]Value)
		shard.Owned = false
		shard.Frozen = false
		shard.Num = args.Num
		reply.Err = rpc.OK
		return reply
	} else {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}
}
func (kv *KVServer) doInstall(args *shardrpc.InstallShardArgs) *shardrpc.InstallShardReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := &shardrpc.InstallShardReply{}
	if shard, ok := kv.shards[args.Shard]; ok {
		if args.Num <= shard.Num {
			reply.Err = rpc.OK
			return reply
		}
		r := bytes.NewBuffer(args.State)
		decoder := labgob.NewDecoder(r)
		data := make(map[string]Value)
		err := decoder.Decode(&data)
		if err != nil {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}
		shard.Data = data
		shard.Owned = true
		shard.Frozen = false
		shard.Num = args.Num
		reply.Err = rpc.OK
		return reply
	} else {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}
}
func (kv *KVServer) doFreeze(args *shardrpc.FreezeShardArgs) *shardrpc.FreezeShardReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := &shardrpc.FreezeShardReply{}
	if shard, ok := kv.shards[args.Shard]; ok {
		if args.Num < shard.Num {
			reply.Err = rpc.ErrWrongGroup
			reply.Num = shard.Num
			return reply
		}
		shard.Frozen = true
		shard.Num = args.Num
		buf := make([]byte, 0)
		buffer := bytes.NewBuffer(buf)
		encoder := labgob.NewEncoder(buffer)
		err := encoder.Encode(shard.Data)
		if err != nil {
			reply.Err = rpc.ErrWrongGroup
			reply.Num = shard.Num
			return reply
		}
		reply.State = buffer.Bytes()
		reply.Num = shard.Num
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrWrongGroup
	}
	return reply
}

func (kv *KVServer) doGet(args *rpc.GetArgs) *rpc.GetReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := &rpc.GetReply{}
	if shard, ok := kv.shards[shardcfg.Key2Shard(args.Key)]; ok {
		if !shard.Owned || shard.Frozen {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}
		if v, ok := shard.Data[args.Key]; ok {
			reply.Value = v.Value
			reply.Version = v.Version
			reply.Err = rpc.OK
			return reply
		}
	}

	reply.Err = rpc.ErrNoKey
	return reply

}
func (kv *KVServer) doPut(args *rpc.PutArgs) *rpc.PutReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := &rpc.PutReply{}
	if shard, ok := kv.shards[shardcfg.Key2Shard(args.Key)]; ok {
		if !shard.Owned || shard.Frozen {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}
		if v, ok := shard.Data[args.Key]; ok {
			if args.Version == v.Version {
				shard.Data[args.Key] = Value{Value: args.Value, Version: args.Version + 1}
				reply.Err = rpc.OK
				return reply
			}
			reply.Err = rpc.ErrVersion
			return reply
		}
		if args.Version == 0 {
			shard.Data[args.Key] = Value{Value: args.Value, Version: 1}
			reply.Err = rpc.OK
			return reply
		}
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
	encoder.Encode(kv.shards)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(r)

	var newStore map[shardcfg.Tshid]*ShardState
	if err := decoder.Decode(&newStore); err != nil {
		panic(err)
	}
	for _, shardState := range newStore {
		if shardState.Data == nil {
			shardState.Data = make(map[string]Value)
		}
	}
	kv.shards = newStore
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

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	rep, ok := res.(*shardrpc.FreezeShardReply)
	if !ok {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	reply.Err = rep.Err
	reply.Num = rep.Num
	reply.State = rep.State
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	rep, ok := res.(*shardrpc.InstallShardReply)
	if !ok {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	reply.Err = rep.Err
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	rep, ok := res.(*shardrpc.DeleteShardReply)
	if !ok {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	reply.Err = rep.Err
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me}
	kv.shards = make(map[shardcfg.Tshid]*ShardState)
	for s := range shardcfg.NShards {
		kv.shards[shardcfg.Tshid(s)] = &ShardState{Data: make(map[string]Value)}
		if gid == shardcfg.Gid1 {
			kv.shards[shardcfg.Tshid(s)].Owned = true
		}
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
