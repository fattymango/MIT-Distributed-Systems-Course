package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"fmt"
	"time"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

const (
	CONFIG_KEY      = "CONFIG_KEY"
	NEXT_CONFIG_KEY = "NEXT_CONFIG_KEY"
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
	currValue, currVersion, err := sck.Get(CONFIG_KEY)
	if err == rpc.ErrNoKey {
		return
	}

	nextValue, _, err := sck.Get(NEXT_CONFIG_KEY)
	if err == rpc.ErrNoKey {
		return
	}

	curr := shardcfg.FromString(currValue)
	next := shardcfg.FromString(nextValue)

	if curr.Num < next.Num {
		sck.moveShards(curr, next)
		sck.Put(CONFIG_KEY, next.String(), currVersion)
	}
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
	value, configVersion, err := sck.Get(CONFIG_KEY)
	if err != rpc.OK {
		panic(err)
	}

	current := shardcfg.FromString(value)
	if new.Num-current.Num != 1 {
		return
	}
	value, newVersion, err := sck.Get(NEXT_CONFIG_KEY)
	if err == rpc.OK {
		next := shardcfg.FromString(value)
		if next.Num >= new.Num {
			return
		}
	} else if err != rpc.ErrNoKey {
		return
	}
	err = sck.Put(NEXT_CONFIG_KEY, new.String(), newVersion)
	switch err {
	case rpc.ErrVersion:
		return
	case rpc.ErrMaybe:
		value, _, _ := sck.Get(NEXT_CONFIG_KEY)
		if value != new.String() {
			return
		}
	}
	sck.moveShards(current, new)
	sck.Put(CONFIG_KEY, new.String(), configVersion)
}

func (sck *ShardCtrler) moveShards(old, new *shardcfg.ShardConfig) {
	loopUntilSuccess := func(fn func() rpc.Err, duration time.Duration) bool {
		for {
			err := fn()
			if err == rpc.OK {
				return true
			}
			if sck.Query().Num >= new.Num {
				return false
			}
			time.Sleep(duration)
		}
	}
	clkMap := make(map[tester.Tgid]*shardgrp.Clerk, 0)
	for s := 0; s < shardcfg.NShards; s++ {
		oldGid, oldSrvs, _ := old.GidServers(shardcfg.Tshid(s))
		newGid, newSrvs, _ := new.GidServers(shardcfg.Tshid(s))
		if oldGid == newGid {
			continue
		}
		oldCk, ok := clkMap[oldGid]
		if !ok {
			oldCk = shardgrp.MakeClerk(sck.clnt, oldSrvs)
			clkMap[oldGid] = oldCk
		}

		newCk, ok := clkMap[newGid]
		if !ok {
			newCk = shardgrp.MakeClerk(sck.clnt, newSrvs)
			clkMap[newGid] = newCk
		}
		var state []byte
		freezeStatefn := func() rpc.Err {
			st, err := oldCk.FreezeShard(shardcfg.Tshid(s), new.Num)
			state = st
			return err
		}
		if !loopUntilSuccess(freezeStatefn, 100*time.Millisecond) {
			return
		}

		if !loopUntilSuccess(func() rpc.Err {
			return newCk.InstallShard(shardcfg.Tshid(s), state, new.Num)
		}, 100*time.Millisecond) {
			return
		}

		if !loopUntilSuccess(func() rpc.Err {
			return oldCk.DeleteShard(shardcfg.Tshid(s), new.Num)
		}, 100*time.Millisecond) {
			return
		}
	}
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
