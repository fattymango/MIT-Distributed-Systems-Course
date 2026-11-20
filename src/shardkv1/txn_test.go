package shardkv

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

// Tests for cross-shard transactions (two-phase commit across shardgrps).

const TXN_NACCT = 10

func txnClerk(ck kvtest.IKVClerk) *Clerk {
	return ck.(*kvtest.TestClerk).IKVClerk.(*Clerk)
}

func tget(k string) rpc.TxnOp {
	return rpc.TxnOp{Type: rpc.TxnGet, Key: k}
}

func tput(k, v string) rpc.TxnOp {
	return rpc.TxnOp{Type: rpc.TxnPut, Key: k, Value: v}
}

func tcmp(k string, ver rpc.Tversion) rpc.TxnCompare {
	return rpc.TxnCompare{Key: k, Version: ver}
}

func txnAccounts() []string {
	accts := make([]string, TXN_NACCT)
	for i := range accts {
		accts[i] = "acct" + strconv.Itoa(i)
	}
	return accts
}

func (ts *Test) checkTxnResult(what string, r rpc.TxnOpResult, val string, ver rpc.Tversion, err rpc.Err) {
	if r.Err != err || (err == rpc.OK && (r.Value != val || r.Version != ver)) {
		ts.t.Fatalf("%s: got (%q, v%d, %v), want (%q, v%d, %v)", what, r.Value, r.Version, r.Err, val, ver, err)
	}
}

// start Gid1 plus two more groups, so that keys are spread over 3 groups
func (ts *Test) setupTxnService() []tester.Tgid {
	gid1 := ts.setupKVService()
	gids := ts.groups(2)
	if ok := ts.joinGroups(ts.ShardCtrler(), gids); !ok {
		ts.t.Fatalf("joinGroups failed")
	}
	return append([]tester.Tgid{gid1}, gids...)
}

// check that a set of keys really lives on more than one group,
// otherwise the test wouldn't exercise cross-group transactions
func (ts *Test) checkSpread(keys []string) {
	cfg := ts.ShardCtrler().Query()
	gids := map[tester.Tgid]bool{}
	for _, k := range keys {
		gids[cfg.Shards[shardcfg.Key2Shard(k)]] = true
	}
	if len(gids) < 2 {
		ts.t.Fatalf("keys %v all live on one group; test is useless", keys)
	}
}

// Basic semantics across shards: compares, Then/Else, results in the
// original order, all-or-nothing writes, and interop with Get/Put.
func TestTxnBasic5D(t *testing.T) {
	ts := MakeTest(t, "Test (5D): cross-shard txn basic ...", true)
	defer ts.Cleanup()
	ts.setupTxnService()

	tck := ts.MakeClerk()
	ck := txnClerk(tck)
	keys := []string{"a", "b", "c", "d", "e"}
	ts.checkSpread(keys)

	// 1. all keys missing: Then creates all of them at v1
	cmps := []rpc.TxnCompare{}
	puts := []rpc.TxnOp{}
	for i, k := range keys {
		cmps = append(cmps, tcmp(k, 0))
		puts = append(puts, tput(k, strconv.Itoa(i)))
	}
	r := ck.Txn(cmps, puts, nil)
	if r.Err != rpc.OK || !r.Succeeded || len(r.Results) != len(keys) {
		ts.t.Fatalf("create txn: %+v", r)
	}
	for i, k := range keys {
		ts.checkTxnResult("create "+k, r.Results[i], strconv.Itoa(i), 1, rpc.OK)
	}

	// 2. same compares again: Else runs, results come back in Else's order
	r = ck.Txn(cmps, []rpc.TxnOp{tput("a", "X")}, []rpc.TxnOp{tget("e"), tget("missing"), tget("a"), tget("c")})
	if r.Err != rpc.OK || r.Succeeded || len(r.Results) != 4 {
		ts.t.Fatalf("stale txn: %+v", r)
	}
	ts.checkTxnResult("else e", r.Results[0], "4", 1, rpc.OK)
	ts.checkTxnResult("else missing", r.Results[1], "", 0, rpc.ErrNoKey)
	ts.checkTxnResult("else a", r.Results[2], "0", 1, rpc.OK)
	ts.checkTxnResult("else c", r.Results[3], "2", 1, rpc.OK)

	// 3. all compares hold except one on another group: nothing is written anywhere
	bad := []rpc.TxnCompare{tcmp("a", 1), tcmp("b", 1), tcmp("c", 1), tcmp("d", 1), tcmp("e", 7)}
	r = ck.Txn(bad, []rpc.TxnOp{tput("a", "X"), tput("b", "X"), tput("c", "X"), tput("d", "X")}, nil)
	if r.Err != rpc.OK || r.Succeeded {
		ts.t.Fatalf("half-matching txn: %+v", r)
	}
	for i, k := range keys {
		if v, ver, err := tck.Get(k); err != rpc.OK || v != strconv.Itoa(i) || ver != 1 {
			ts.t.Fatalf("%s changed by failed txn: (%q, v%d, %v)", k, v, ver, err)
		}
	}

	// 4. no compares: Then always runs; ops on the same key apply in order
	r = ck.Txn(nil, []rpc.TxnOp{tput("a", "10"), tget("b"), tget("a"), tput("a", "11")}, nil)
	if r.Err != rpc.OK || !r.Succeeded || len(r.Results) != 4 {
		ts.t.Fatalf("no-compare txn: %+v", r)
	}
	ts.checkTxnResult("put a", r.Results[0], "10", 2, rpc.OK)
	ts.checkTxnResult("get b", r.Results[1], "1", 1, rpc.OK)
	ts.checkTxnResult("get a", r.Results[2], "10", 2, rpc.OK)
	ts.checkTxnResult("put a again", r.Results[3], "11", 3, rpc.OK)

	// 5. interop with plain Get/Put, and locks are released after the txn
	if v, ver, err := tck.Get("a"); err != rpc.OK || v != "11" || ver != 3 {
		ts.t.Fatalf("Get after txn: (%q, v%d, %v)", v, ver, err)
	}
	if err := tck.Put("a", "12", 3); err != rpc.OK {
		ts.t.Fatalf("Put after txn: %v", err)
	}
	r = ck.Txn([]rpc.TxnCompare{tcmp("a", 4)}, []rpc.TxnOp{tget("a")}, nil)
	if !r.Succeeded {
		ts.t.Fatalf("txn didn't see plain Put: %+v", r)
	}
	ts.checkTxnResult("get after put", r.Results[0], "12", 4, rpc.OK)
}

// A coordinator prepares a shard and then dies without deciding. Its
// lock must not block the key forever: another client's Txn must
// force-abort it, and plain Puts must work again afterwards.
func TestTxnStuckCoordinator5D(t *testing.T) {
	ts := MakeTest(t, "Test (5D): cross-shard txn stuck coordinator ...", true)
	defer ts.Cleanup()
	ts.setupTxnService()

	tck := ts.MakeClerk()
	ck := txnClerk(tck)
	if err := tck.Put("k", "v1", 0); err != rpc.OK {
		ts.t.Fatalf("Put: %v", err)
	}

	// the "dead" coordinator: prepare k's shard directly and never finish
	cfg := ts.ShardCtrler().Query()
	s := shardcfg.Key2Shard("k")
	gid, srvs, _ := cfg.GidServers(s)
	gck := shardgrp.MakeClerk(ts.Config.MakeClient(), srvs)
	rep := gck.TxnPrepare(shardrpc.TxnPrepareArgs{TxnID: "dead-1", Shard: s, Then: []rpc.TxnOp{tput("k", "dead")}})
	if rep.Err != rpc.OK || rep.Vote != shardrpc.VoteOK {
		ts.t.Fatalf("prepare on gid %d: %+v", gid, rep)
	}

	done := make(chan rpc.TxnReply)
	go func() {
		done <- ck.Txn([]rpc.TxnCompare{tcmp("k", 1)}, []rpc.TxnOp{tput("k", "v2")}, nil)
	}()
	select {
	case r := <-done:
		if !r.Succeeded {
			ts.t.Fatalf("txn after stuck coordinator: %+v", r)
		}
	case <-time.After(20 * time.Second):
		ts.t.Fatalf("txn blocked forever by a dead coordinator's lock")
	}

	if d, _, err := ts.ShardCtrler().Get("txn-dead-1"); err != rpc.OK || d != shardrpc.DecisionAbort {
		ts.t.Fatalf("dead txn decision = (%q, %v), want ABORT", d, err)
	}
	if v, ver, err := tck.Get("k"); err != rpc.OK || v != "v2" || ver != 2 {
		ts.t.Fatalf("Get k: (%q, v%d, %v)", v, ver, err)
	}
	if err := tck.Put("k", "v3", 2); err != rpc.OK {
		ts.t.Fatalf("Put after forced abort: %v", err)
	}
}

// joins and leaves groups until stop is closed, so that shards (and
// any prepared transactions in them) move around under the clients
func (ts *Test) reconfigurer(stop chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	sck := ts.ShardCtrler()
	for {
		select {
		case <-stop:
			return
		default:
		}
		gid := ts.newGid()
		if ok := ts.joinGroups(sck, []tester.Tgid{gid}); !ok {
			ts.t.Errorf("join %d failed", gid)
			return
		}
		if ok := ts.leaveGroups(sck, []tester.Tgid{gid}); !ok {
			ts.t.Errorf("leave %d failed", gid)
			return
		}
	}
}

// Bank transfers between accounts on different groups, with an
// auditor that reads all accounts in one transaction. The total must
// never change, and the auditor must never see a half-done transfer.
func (ts *Test) txnTransferTest(nclnt int, reconfig bool) {
	const (
		INITIAL = 100
		NITER   = 2
		NSEC    = 3 * time.Second
	)
	defer ts.Cleanup()
	ts.setupTxnService()

	accts := txnAccounts()
	ts.checkSpread(accts)
	total := TXN_NACCT * INITIAL

	readAll := func(ck *Clerk) (int, []rpc.TxnOpResult) {
		ops := make([]rpc.TxnOp, TXN_NACCT)
		for i, a := range accts {
			ops[i] = tget(a)
		}
		r := ck.Txn(nil, ops, nil)
		sum := 0
		for _, res := range r.Results {
			n, _ := strconv.Atoi(res.Value)
			sum += n
		}
		return sum, r.Results
	}

	ck := txnClerk(ts.MakeClerk())
	cmps := make([]rpc.TxnCompare, TXN_NACCT)
	puts := make([]rpc.TxnOp, TXN_NACCT)
	for i, a := range accts {
		cmps[i] = tcmp(a, 0)
		puts[i] = tput(a, strconv.Itoa(INITIAL))
	}
	if r := ck.Txn(cmps, puts, nil); !r.Succeeded {
		ts.t.Fatalf("couldn't create accounts: %+v", r)
	}

	var transfers int64
	for iter := 0; iter < NITER; iter++ {
		var bad atomic.Value
		stop := make(chan struct{})
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			ack := txnClerk(ts.MakeClerk())
			for {
				select {
				case <-stop:
					return
				default:
				}
				if sum, res := readAll(ack); sum != total {
					bad.Store(fmt.Sprintf("auditor saw total %d, want %d: %+v", sum, total, res))
					return
				}
			}
		}()

		var rwg sync.WaitGroup
		rstop := make(chan struct{})
		if reconfig {
			rwg.Add(1)
			go ts.reconfigurer(rstop, &rwg)
		}

		ts.SpawnClientsAndWait(nclnt, NSEC, func(cli int, tck kvtest.IKVClerk, done chan struct{}) kvtest.ClntRes {
			ck := txnClerk(tck)
			res := kvtest.ClntRes{}
			for {
				select {
				case <-done:
					return res
				default:
				}
				i := rand.Intn(TXN_NACCT)
				j := (i + 1 + rand.Intn(TXN_NACCT-1)) % TXN_NACCT
				r := ck.Txn(nil, []rpc.TxnOp{tget(accts[i]), tget(accts[j])}, nil)
				bi, _ := strconv.Atoi(r.Results[0].Value)
				bj, _ := strconv.Atoi(r.Results[1].Value)
				if bi == 0 {
					continue
				}
				amt := 1 + rand.Intn(bi)
				r = ck.Txn(
					[]rpc.TxnCompare{tcmp(accts[i], r.Results[0].Version), tcmp(accts[j], r.Results[1].Version)},
					[]rpc.TxnOp{tput(accts[i], strconv.Itoa(bi-amt)), tput(accts[j], strconv.Itoa(bj+amt))},
					nil)
				if r.Succeeded {
					res.Nok++
					atomic.AddInt64(&transfers, 1)
				} else {
					res.Nmaybe++
				}
			}
		})

		close(rstop)
		rwg.Wait()
		close(stop)
		wg.Wait()
		if msg := bad.Load(); msg != nil {
			ts.t.Fatalf("%v", msg)
		}
		if sum, res := readAll(txnClerk(ts.MakeClerk())); sum != total {
			ts.t.Fatalf("after iteration %d: total %d, want %d: %+v", iter, sum, total, res)
		}
	}
	if transfers == 0 {
		ts.t.Fatalf("no transfer succeeded")
	}
}

func TestTxnTransfer5D(t *testing.T) {
	ts := MakeTest(t, "Test (5D): cross-shard txn transfers ...", true)
	ts.txnTransferTest(5, false)
}

func TestTxnTransferUnreliable5D(t *testing.T) {
	ts := MakeTest(t, "Test (5D): cross-shard txn transfers ...", false)
	ts.txnTransferTest(5, false)
}

func TestTxnTransferReconfig5D(t *testing.T) {
	ts := MakeTest(t, "Test (5D): cross-shard txn transfers while shards move ...", true)
	ts.txnTransferTest(5, true)
}

func TestTxnTransferReconfigUnreliable5D(t *testing.T) {
	ts := MakeTest(t, "Test (5D): cross-shard txn transfers while shards move ...", false)
	ts.txnTransferTest(5, true)
}

// Clients increment two counters on different groups in the same
// transaction, over an unreliable network while shards move. Both
// counters must always be equal (atomicity), and must equal the number
// of increments that reported success (no lost or doubled commits).
func (ts *Test) txnCounterTest(reconfig bool) {
	const (
		NCLNT = 5
		NSEC  = 3 * time.Second
	)
	defer ts.Cleanup()
	ts.setupTxnService()

	keys := []string{"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"}
	cfg := ts.ShardCtrler().Query()
	k1 := keys[0]
	k2 := ""
	for _, k := range keys[1:] {
		if cfg.Shards[shardcfg.Key2Shard(k)] != cfg.Shards[shardcfg.Key2Shard(k1)] {
			k2 = k
			break
		}
	}
	if k2 == "" {
		ts.t.Fatalf("couldn't find two keys on different groups")
	}

	var rwg sync.WaitGroup
	rstop := make(chan struct{})
	if reconfig {
		rwg.Add(1)
		go ts.reconfigurer(rstop, &rwg)
	}

	var successes int64
	ts.SpawnClientsAndWait(NCLNT, NSEC, func(cli int, tck kvtest.IKVClerk, done chan struct{}) kvtest.ClntRes {
		ck := txnClerk(tck)
		res := kvtest.ClntRes{}
		for {
			select {
			case <-done:
				return res
			default:
			}
			r := ck.Txn(nil, []rpc.TxnOp{tget(k1), tget(k2)}, nil)
			n1, _ := strconv.Atoi(r.Results[0].Value)
			n2, _ := strconv.Atoi(r.Results[1].Value)
			if n1 != n2 {
				ts.t.Errorf("client %d read %s=%d %s=%d in one txn", cli, k1, n1, k2, n2)
				return res
			}
			r = ck.Txn(
				[]rpc.TxnCompare{tcmp(k1, r.Results[0].Version), tcmp(k2, r.Results[1].Version)},
				[]rpc.TxnOp{tput(k1, strconv.Itoa(n1+1)), tput(k2, strconv.Itoa(n2+1))},
				nil)
			if r.Succeeded {
				res.Nok++
				atomic.AddInt64(&successes, 1)
			} else {
				res.Nmaybe++
			}
		}
	})
	close(rstop)
	rwg.Wait()

	r := txnClerk(ts.MakeClerk()).Txn(nil, []rpc.TxnOp{tget(k1), tget(k2)}, nil)
	n1, _ := strconv.Atoi(r.Results[0].Value)
	n2, _ := strconv.Atoi(r.Results[1].Value)
	if n1 != n2 || int64(n1) != successes {
		ts.t.Fatalf("%s=%d %s=%d but %d increments reported success", k1, n1, k2, n2, successes)
	}
	if successes == 0 {
		ts.t.Fatalf("no increment succeeded")
	}
}

func TestTxnCounterUnreliable5D(t *testing.T) {
	ts := MakeTest(t, "Test (5D): cross-shard txn counters ...", false)
	ts.txnCounterTest(false)
}

func TestTxnCounterReconfigUnreliable5D(t *testing.T) {
	ts := MakeTest(t, "Test (5D): cross-shard txn counters while shards move ...", false)
	ts.txnCounterTest(true)
}
