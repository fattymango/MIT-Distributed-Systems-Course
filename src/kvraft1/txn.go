package kvraft

import "6.5840/kvsrv1/rpc"

type DedupEntry struct {
	Seq   int64
	Reply rpc.TxnReply
}
