package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"fmt"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

const (
	CONFIG_KEY = "CONFIG_KEY"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.

}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	sck.Put(CONFIG_KEY, cfg.String(), 0)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.
	value, version, err := sck.Get(CONFIG_KEY)
	if err != rpc.OK {
		panic(err)
	}
	old := shardcfg.FromString(value)
	for s := 0; s < shardcfg.NShards; s++ {
		oldGid, oldSrvs, _ := old.GidServers(shardcfg.Tshid(s))
		newGid, newSrvs, _ := new.GidServers(shardcfg.Tshid(s))
		if oldGid == newGid {
			continue
		}
		oldCk := shardgrp.MakeClerk(sck.clnt, oldSrvs)
		newCk := shardgrp.MakeClerk(sck.clnt, newSrvs)
		state, _ := oldCk.FreezeShard(shardcfg.Tshid(s), new.Num)
		newCk.InstallShard(shardcfg.Tshid(s), state, new.Num)
		oldCk.DeleteShard(shardcfg.Tshid(s), new.Num)
	}
	sck.Put(CONFIG_KEY, new.String(), version)

}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	config, _, err := sck.Get(CONFIG_KEY)
	if err != rpc.OK {
		panic(fmt.Sprintf("error getting config: %v", err))
	}

	return shardcfg.FromString(config)
}
