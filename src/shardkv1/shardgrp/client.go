package shardgrp

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type Clerk struct {
	*tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can  add to this struct.
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{Clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	args := rpc.GetArgs{Key: key}
	server := ck.leader
	rounds := 0
	for rounds < 3 {
		reply := rpc.GetReply{}
		ok := ck.Call(ck.servers[server], "KVServer.Get", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			server = (server + 1) % len(ck.servers)
			if server == 0 {
				rounds++
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		ck.leader = server
		return reply.Value, reply.Version, reply.Err

	}
	return "", 0, rpc.ErrWrongGroup
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	server := ck.leader
	sentBefore := false

	rounds := 0
	for rounds < 3 {
		reply := rpc.PutReply{}
		ok := ck.Call(ck.servers[server], "KVServer.Put", &args, &reply)
		if ok {
			switch reply.Err {
			case rpc.OK, rpc.ErrNoKey:
				ck.leader = server
				return reply.Err
			case rpc.ErrVersion:
				ck.leader = server
				if sentBefore {
					return rpc.ErrMaybe
				}
				return rpc.ErrVersion
			case rpc.ErrWrongGroup:
				return reply.Err
			}
		}
		sentBefore = true
		server = (server + 1) % len(ck.servers)
		if server == 0 {
			rounds++
		}
		time.Sleep(100 * time.Millisecond)
	}

	return rpc.ErrWrongGroup
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	args := shardrpc.FreezeShardArgs{Shard: s, Num: num}
	server := ck.leader
	rounds := 0
	for rounds < 3 {
		reply := shardrpc.FreezeShardReply{}
		ok := ck.Call(ck.servers[server], "KVServer.FreezeShard", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			server = (server + 1) % len(ck.servers)
			if server == 0 {
				rounds++
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		ck.leader = server
		return reply.State, reply.Err

	}
	return nil, rpc.ErrWrongGroup
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := shardrpc.InstallShardArgs{Shard: s, Num: num, State: state}
	server := ck.leader
	rounds := 0
	for rounds < 3 {
		reply := shardrpc.InstallShardReply{}
		ok := ck.Call(ck.servers[server], "KVServer.InstallShard", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			server = (server + 1) % len(ck.servers)
			if server == 0 {
				rounds++
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		ck.leader = server
		return reply.Err

	}
	return rpc.ErrWrongGroup
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := shardrpc.DeleteShardArgs{Shard: s, Num: num}
	server := ck.leader
	rounds := 0
	for rounds < 3 {
		reply := shardrpc.DeleteShardReply{}
		ok := ck.Call(ck.servers[server], "KVServer.DeleteShard", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			server = (server + 1) % len(ck.servers)
			if server == 0 {
				rounds++
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		ck.leader = server
		return reply.Err

	}
	return rpc.ErrWrongGroup

}
