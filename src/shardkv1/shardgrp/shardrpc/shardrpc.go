package shardrpc

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
)

const (
	VoteOK        = "OK"
	VoteConflict  = "Conflict"
	DecisionThen  = "THEN"
	DecisionElse  = "ELSE"
	DecisionAbort = "ABORT"
)

type FreezeShardArgs struct {
	Shard shardcfg.Tshid
	Num   shardcfg.Tnum
}

type FreezeShardReply struct {
	State []byte
	Num   shardcfg.Tnum
	Err   rpc.Err
}

type InstallShardArgs struct {
	Shard shardcfg.Tshid
	State []byte
	Num   shardcfg.Tnum
}

type InstallShardReply struct {
	Err rpc.Err
}

type DeleteShardArgs struct {
	Shard shardcfg.Tshid
	Num   shardcfg.Tnum
}

type DeleteShardReply struct {
	Err rpc.Err
}

type TxnPrepareArgs struct {
	TxnID    string
	Shard    shardcfg.Tshid
	Compares []rpc.TxnCompare
	Then     []rpc.TxnOp
	Else     []rpc.TxnOp
}
type TxnPrepareReply struct {
	Err    rpc.Err
	Vote   string
	CmpOK  bool
	Holder string
}
type TxnFinishArgs struct {
	TxnID    string
	Shard    shardcfg.Tshid
	Decision string
}
type TxnFinishReply struct {
	Err     rpc.Err
	Results []rpc.TxnOpResult
}
