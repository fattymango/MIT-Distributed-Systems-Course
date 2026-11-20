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

type PendingTxn struct {
	Then  []rpc.TxnOp
	Else  []rpc.TxnOp
	CmpOK bool
}
type DoneTxn struct {
	Decision string
	Results  []rpc.TxnOpResult
}

type Value struct {
	Value   string
	Version rpc.Tversion
}
type ShardState struct {
	Data    map[string]Value
	Owned   bool
	Frozen  bool
	Num     shardcfg.Tnum
	Locks   map[string]string
	Pending map[string]PendingTxn
	Done    map[string]DoneTxn
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
	case shardrpc.TxnPrepareArgs:
		return kv.doPrepare(&r)
	case shardrpc.TxnFinishArgs:
		return kv.doFinish(&r)
	}
	return nil
}
func (kv *KVServer) doFinish(args *shardrpc.TxnFinishArgs) *shardrpc.TxnFinishReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := &shardrpc.TxnFinishReply{}
	if shard, ok := kv.shards[args.Shard]; ok {
		if !shard.Owned || shard.Frozen {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}

		if done, ok := shard.Done[args.TxnID]; ok {
			reply.Err = rpc.OK
			reply.Results = done.Results
			return reply
		}

		if _, ok := shard.Pending[args.TxnID]; !ok {
			shard.Done[args.TxnID] = DoneTxn{Decision: args.Decision}
			reply.Err = rpc.OK
			return reply
		}

		p := shard.Pending[args.TxnID]
		var ops []rpc.TxnOp
		switch args.Decision {
		case shardrpc.DecisionElse:
			ops = p.Else
		case shardrpc.DecisionThen:
			ops = p.Then
		case shardrpc.DecisionAbort:
		}

		for _, op := range ops {
			result := rpc.TxnOpResult{}
			switch op.Type {
			case rpc.TxnGet:
				if val, ok := shard.Data[op.Key]; ok {
					result = rpc.TxnOpResult{Value: val.Value, Version: val.Version, Err: rpc.OK}
				} else {
					result = rpc.TxnOpResult{Err: rpc.ErrNoKey}
				}
			case rpc.TxnPut:
				version := rpc.Tversion(0)
				if val, ok := shard.Data[op.Key]; ok {
					version = val.Version
				}

				shard.Data[op.Key] = Value{Value: op.Value, Version: version + 1}
				result = rpc.TxnOpResult{Version: version + 1, Err: rpc.OK, Value: op.Value}
			}
			reply.Results = append(reply.Results, result)
		}

		for k, v := range shard.Locks {
			if v == args.TxnID {
				delete(shard.Locks, k)
			}
		}

		delete(shard.Pending, args.TxnID)
		shard.Done[args.TxnID] = DoneTxn{Decision: args.Decision, Results: reply.Results}
		reply.Err = rpc.OK

		return reply
	}

	return &shardrpc.TxnFinishReply{Err: rpc.ErrWrongGroup}

}
func (kv *KVServer) doPrepare(args *shardrpc.TxnPrepareArgs) *shardrpc.TxnPrepareReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := &shardrpc.TxnPrepareReply{}
	if shard, ok := kv.shards[args.Shard]; ok {
		if !shard.Owned || shard.Frozen {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}
		if _, ok := shard.Done[args.TxnID]; ok {
			reply.Err = rpc.OK
			reply.Vote = shardrpc.VoteConflict
			return reply
		}
		if ptx, ok := shard.Pending[args.TxnID]; ok {
			reply.Err = rpc.OK
			reply.Vote = shardrpc.VoteOK
			reply.CmpOK = ptx.CmpOK
			return reply
		}

		keys := make([]string, 0)
		for _, v := range args.Compares {
			keys = append(keys, v.Key)
		}
		for _, v := range args.Then {
			keys = append(keys, v.Key)

		}
		for _, v := range args.Else {
			keys = append(keys, v.Key)

		}
		for _, key := range keys {
			if holder, ok := shard.Locks[key]; ok && holder != args.TxnID {
				reply.Err = rpc.OK
				reply.Vote = shardrpc.VoteConflict
				reply.Holder = holder
				return reply
			}
		}
		for _, key := range keys {
			shard.Locks[key] = args.TxnID
		}

		cmpOK := true
		for _, compare := range args.Compares {
			version := rpc.Tversion(0)
			if data, ok := shard.Data[compare.Key]; ok {
				version = data.Version
			}
			if version != compare.Version {
				cmpOK = false
				break
			}
		}
		shard.Pending[args.TxnID] = PendingTxn{Then: args.Then, Else: args.Else, CmpOK: cmpOK}
		reply.Err = rpc.OK
		reply.Vote = shardrpc.VoteOK
		reply.CmpOK = cmpOK
		return reply
	}
	return &shardrpc.TxnPrepareReply{Err: rpc.ErrWrongGroup}
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
		shard.Locks = make(map[string]string)
		shard.Pending = make(map[string]PendingTxn)
		shard.Done = make(map[string]DoneTxn)
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

		locks := make(map[string]string)
		err = decoder.Decode(&locks)
		if err != nil {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}
		shard.Locks = locks

		pending := make(map[string]PendingTxn)
		err = decoder.Decode(&pending)
		if err != nil {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}
		shard.Pending = pending

		done := make(map[string]DoneTxn)
		err = decoder.Decode(&done)
		if err != nil {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}
		shard.Done = done

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
		err = encoder.Encode(shard.Locks)
		if err != nil {
			reply.Err = rpc.ErrWrongGroup
			reply.Num = shard.Num
			return reply
		}
		err = encoder.Encode(shard.Pending)
		if err != nil {
			reply.Err = rpc.ErrWrongGroup
			reply.Num = shard.Num
			return reply
		}
		err = encoder.Encode(shard.Done)
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

		if _, ok := shard.Locks[args.Key]; ok {
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
		if shardState.Locks == nil {
			shardState.Locks = make(map[string]string)
		}
		if shardState.Pending == nil {
			shardState.Pending = make(map[string]PendingTxn)
		}
		if shardState.Done == nil {
			shardState.Done = make(map[string]DoneTxn)
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

func (kv *KVServer) TxnPrepare(args *shardrpc.TxnPrepareArgs, reply *shardrpc.TxnPrepareReply) {
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	rep, ok := res.(*shardrpc.TxnPrepareReply)
	if !ok {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	reply.Err = rep.Err
	reply.CmpOK = rep.CmpOK
	reply.Holder = rep.Holder
	reply.Vote = rep.Vote
}
func (kv *KVServer) TxnFinish(args *shardrpc.TxnFinishArgs, reply *shardrpc.TxnFinishReply) {
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	rep, ok := res.(*shardrpc.TxnFinishReply)
	if !ok {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	reply.Err = rep.Err
	reply.Results = rep.Results
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
	labgob.Register(shardrpc.TxnPrepareArgs{})
	labgob.Register(shardrpc.TxnFinishArgs{})
	kv := &KVServer{gid: gid, me: me}
	kv.shards = make(map[shardcfg.Tshid]*ShardState)
	for s := range shardcfg.NShards {
		kv.shards[shardcfg.Tshid(s)] = &ShardState{
			Data:    make(map[string]Value),
			Locks:   make(map[string]string),
			Pending: make(map[string]PendingTxn),
			Done:    make(map[string]DoneTxn),
		}
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
