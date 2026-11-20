package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"fmt"
	"math/rand/v2"
	"time"

	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/shardkv1/shardgrp/shardrpc"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardctrler"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	rcks map[tester.Tgid]*shardgrp.Clerk
	// You will have to modify this struct.
	clientId int64
	seq      int64
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	ck.rcks = make(map[tester.Tgid]*shardgrp.Clerk)
	ck.clientId = rand.Int64()
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) GetClerk(gid tester.Tgid) (*shardgrp.Clerk, bool) {
	rck, ok := ck.rcks[gid]
	return rck, ok
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.

	for {
		cfg := ck.sck.Query()
		shard := shardcfg.Key2Shard(key)
		gid, srvs, ok := cfg.GidServers(shard)
		if !ok {
			time.Sleep(100 * time.Millisecond)

			continue
		}
		if c, ok := ck.rcks[gid]; ok {
			resp, version, err := c.Get(key)
			if err == rpc.ErrWrongGroup {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			return resp, version, err
		}
		ck.rcks[gid] = shardgrp.MakeClerk(ck.clnt, srvs)
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	retried := false
	for {
		cfg := ck.sck.Query()
		shard := shardcfg.Key2Shard(key)
		gid, srvs, ok := cfg.GidServers(shard)
		if !ok {
			time.Sleep(100 * time.Millisecond)

			continue
		}
		if c, ok := ck.rcks[gid]; ok {
			err := c.Put(key, value, version)
			if err == rpc.ErrWrongGroup {
				retried = true
				time.Sleep(100 * time.Millisecond)
				continue
			} else if err == rpc.ErrVersion && retried {
				return rpc.ErrMaybe
			}
			return err
		}
		ck.rcks[gid] = shardgrp.MakeClerk(ck.clnt, srvs)
	}
}

func (ck *Clerk) Txn(compares []rpc.TxnCompare, then, els []rpc.TxnOp) rpc.TxnReply {
	conflicts := 0
	for {
		ck.seq++
		txnID := fmt.Sprintf("%d-%d", ck.clientId, ck.seq)
		parts := map[shardcfg.Tshid]*shardrpc.TxnPrepareArgs{}
		thenPos := map[shardcfg.Tshid][]int{}
		elsePos := map[shardcfg.Tshid][]int{}

		for _, c := range compares {
			s := shardcfg.Key2Shard(c.Key)
			if _, ok := parts[s]; !ok {
				parts[s] = &shardrpc.TxnPrepareArgs{TxnID: txnID, Shard: s}
			}
			parts[s].Compares = append(parts[s].Compares, c)
		}

		for i, op := range then {
			s := shardcfg.Key2Shard(op.Key)
			if _, ok := parts[s]; !ok {
				parts[s] = &shardrpc.TxnPrepareArgs{TxnID: txnID, Shard: s}
			}
			parts[s].Then = append(parts[s].Then, op)
			thenPos[s] = append(thenPos[s], i)
		}
		for i, op := range els {
			s := shardcfg.Key2Shard(op.Key)
			if _, ok := parts[s]; !ok {
				parts[s] = &shardrpc.TxnPrepareArgs{TxnID: txnID, Shard: s}
			}
			parts[s].Else = append(parts[s].Else, op)
			elsePos[s] = append(elsePos[s], i)
		}

		conflict := false
		allCmp := true
		holder := ""
		holderShard := shardcfg.Tshid(0)
		for s, args := range parts {
			var reply shardrpc.TxnPrepareReply
			for {
				cfg := ck.sck.Query()
				gid, srvs, ok := cfg.GidServers(s)
				if !ok {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				clk, ok := ck.GetClerk(gid)
				if !ok {
					clk = shardgrp.MakeClerk(ck.clnt, srvs)
					ck.rcks[gid] = clk
				}
				reply = clk.TxnPrepare(*args)
				if reply.Err == rpc.ErrWrongGroup {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				break
			}

			if reply.Vote == shardrpc.VoteConflict {
				conflict = true
				holder = reply.Holder
				holderShard = s
				break
			}
			allCmp = allCmp && reply.CmpOK
		}

		decision := shardrpc.DecisionElse
		if conflict {
			decision = shardrpc.DecisionAbort
		} else if allCmp {
			decision = shardrpc.DecisionThen
		}
		key := "txn-" + txnID
		err := ck.sck.Put(key, decision, 0)
		if err != rpc.OK {
			val, _, _ := ck.sck.Get(key)
			decision = val
		}

		var results []rpc.TxnOpResult
		if decision == shardrpc.DecisionThen {
			results = make([]rpc.TxnOpResult, len(then))
		} else if decision == shardrpc.DecisionElse {
			results = make([]rpc.TxnOpResult, len(els))
		}

		for s := range parts {
			var reply shardrpc.TxnFinishReply
			for {
				cfg := ck.sck.Query()
				gid, srvs, ok := cfg.GidServers(s)
				if !ok {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				clk, ok := ck.GetClerk(gid)
				if !ok {
					clk = shardgrp.MakeClerk(ck.clnt, srvs)
					ck.rcks[gid] = clk
				}
				reply = clk.TxnFinish(shardrpc.TxnFinishArgs{TxnID: txnID, Shard: s, Decision: decision})
				if reply.Err == rpc.ErrWrongGroup {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				break
			}

			if decision == shardrpc.DecisionThen {
				for j, pos := range thenPos[s] {
					results[pos] = reply.Results[j]
				}
			} else if decision == shardrpc.DecisionElse {
				for j, pos := range elsePos[s] {
					results[pos] = reply.Results[j]
				}
			}
		}

		if decision != shardrpc.DecisionAbort {
			return rpc.TxnReply{Err: rpc.OK, Succeeded: decision == shardrpc.DecisionThen, Results: results}
		}

		if conflict && holder != "" {
			conflicts++
			if conflicts >= 5 {
				conflicts = 0
				hkey := "txn-" + holder
				ck.sck.Put(hkey, shardrpc.DecisionAbort, 0)
				hdecision, _, _ := ck.sck.Get(hkey)
				for {
					cfg := ck.sck.Query()
					gid, srvs, ok := cfg.GidServers(holderShard)
					if !ok {
						time.Sleep(100 * time.Millisecond)
						continue
					}
					clk, ok := ck.GetClerk(gid)
					if !ok {
						clk = shardgrp.MakeClerk(ck.clnt, srvs)
						ck.rcks[gid] = clk
					}
					reply := clk.TxnFinish(shardrpc.TxnFinishArgs{TxnID: holder, Shard: holderShard, Decision: hdecision})
					if reply.Err == rpc.ErrWrongGroup {
						time.Sleep(100 * time.Millisecond)
						continue
					}
					break
				}
			}
		}
		time.Sleep(time.Duration(50+rand.IntN(100)) * time.Millisecond)
	}
}
