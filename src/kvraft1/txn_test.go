package kvraft

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
	tester "6.5840/tester1"
)

// Tests for the transaction extension (Part D).
//
// A transaction is: IF all Compares hold, run Then, else run Else.
// The whole transaction is one Raft log entry, so it is atomic.

func txnClerk(ck kvtest.IKVClerk) *Clerk {
	return ck.(*kvtest.TestClerk).IKVClerk.(*Clerk)
}

func get(k string) rpc.TxnOp {
	return rpc.TxnOp{Type: rpc.TxnGet, Key: k}
}

func put(k, v string) rpc.TxnOp {
	return rpc.TxnOp{Type: rpc.TxnPut, Key: k, Value: v}
}

func cmp(k string, ver rpc.Tversion) rpc.TxnCompare {
	return rpc.TxnCompare{Key: k, Version: ver}
}

func (ts *Test) checkResult(what string, r rpc.TxnOpResult, val string, ver rpc.Tversion, err rpc.Err) {
	if r.Err != err || (err == rpc.OK && (r.Value != val || r.Version != ver)) {
		ts.t.Fatalf("%s: got (%q, v%d, %v), want (%q, v%d, %v)", what, r.Value, r.Version, r.Err, val, ver, err)
	}
}

// Basic semantics: compares, Then/Else branches, Get/Put results,
// and that regular Get/Put see transaction writes (and vice versa).
func TestTxnBasic5D(t *testing.T) {
	ts := MakeTest(t, "5D txn basic", 1, 5, true, false, false, -1, false)
	tester.AnnotateTest("TestTxnBasic5D", ts.nservers)
	defer ts.Cleanup()

	tck := ts.MakeClerk()
	ck := txnClerk(tck)

	// 1. keys don't exist (version 0): Then runs, creates both keys at v1.
	r := ck.Txn([]rpc.TxnCompare{cmp("a", 0), cmp("b", 0)},
		[]rpc.TxnOp{put("a", "1"), put("b", "2")},
		[]rpc.TxnOp{get("a")})
	if r.Err != rpc.OK || !r.Succeeded || len(r.Results) != 2 {
		ts.t.Fatalf("create txn: %+v", r)
	}
	ts.checkResult("create a", r.Results[0], "1", 1, rpc.OK)
	ts.checkResult("create b", r.Results[1], "2", 1, rpc.OK)

	// 2. same compares again: they no longer hold, Else runs, nothing written.
	r = ck.Txn([]rpc.TxnCompare{cmp("a", 0), cmp("b", 0)},
		[]rpc.TxnOp{put("a", "X"), put("b", "X")},
		[]rpc.TxnOp{get("a"), get("b"), get("missing")})
	if r.Err != rpc.OK || r.Succeeded || len(r.Results) != 3 {
		ts.t.Fatalf("stale txn: %+v", r)
	}
	ts.checkResult("else a", r.Results[0], "1", 1, rpc.OK)
	ts.checkResult("else b", r.Results[1], "2", 1, rpc.OK)
	ts.checkResult("else missing", r.Results[2], "", 0, rpc.ErrNoKey)

	// 3. one compare holds, one doesn't: whole Then must be skipped.
	r = ck.Txn([]rpc.TxnCompare{cmp("a", 1), cmp("b", 5)},
		[]rpc.TxnOp{put("a", "X"), put("b", "X")},
		nil)
	if r.Err != rpc.OK || r.Succeeded || len(r.Results) != 0 {
		ts.t.Fatalf("half-matching txn: %+v", r)
	}
	if v, ver, err := tck.Get("a"); err != rpc.OK || v != "1" || ver != 1 {
		ts.t.Fatalf("a changed by failed txn: (%q, v%d, %v)", v, ver, err)
	}

	// 4. no compares: Then always runs. Ops apply in order (put then get).
	r = ck.Txn(nil, []rpc.TxnOp{put("a", "10"), get("a"), put("a", "11")}, nil)
	if r.Err != rpc.OK || !r.Succeeded || len(r.Results) != 3 {
		ts.t.Fatalf("no-compare txn: %+v", r)
	}
	ts.checkResult("put a", r.Results[0], "10", 2, rpc.OK)
	ts.checkResult("get a", r.Results[1], "10", 2, rpc.OK)
	ts.checkResult("put a again", r.Results[2], "11", 3, rpc.OK)

	// 5. interop with plain Get/Put.
	if v, ver, err := tck.Get("a"); err != rpc.OK || v != "11" || ver != 3 {
		ts.t.Fatalf("Get after txn: (%q, v%d, %v)", v, ver, err)
	}
	if err := tck.Put("a", "12", 3); err != rpc.OK {
		ts.t.Fatalf("Put after txn: %v", err)
	}
	r = ck.Txn([]rpc.TxnCompare{cmp("a", 4)}, []rpc.TxnOp{get("a")}, nil)
	if !r.Succeeded {
		ts.t.Fatalf("txn didn't see plain Put: %+v", r)
	}
	ts.checkResult("get after put", r.Results[0], "12", 4, rpc.OK)
}

// Bank transfers between accounts, done with transactions, while an
// auditor keeps reading all accounts in one transaction. The total
// must never change, and the auditor must never see a half-done transfer.
func (ts *Test) transferTest() {
	const (
		NACCT   = 5
		INITIAL = 100
		NITER   = 3
		NSEC    = 2 * time.Second
	)
	defer ts.Cleanup()

	accts := make([]string, NACCT)
	for i := range accts {
		accts[i] = "acct" + strconv.Itoa(i)
	}
	total := NACCT * INITIAL

	readAll := func(ck *Clerk) (int, []rpc.TxnOpResult) {
		ops := make([]rpc.TxnOp, NACCT)
		for i, a := range accts {
			ops[i] = get(a)
		}
		r := ck.Txn(nil, ops, nil)
		sum := 0
		for _, res := range r.Results {
			n, _ := strconv.Atoi(res.Value)
			sum += n
		}
		return sum, r.Results
	}

	// create all accounts in one transaction
	ck := txnClerk(ts.MakeClerk())
	compares := make([]rpc.TxnCompare, NACCT)
	puts := make([]rpc.TxnOp, NACCT)
	for i, a := range accts {
		compares[i] = cmp(a, 0)
		puts[i] = put(a, strconv.Itoa(INITIAL))
	}
	if r := ck.Txn(compares, puts, nil); !r.Succeeded {
		ts.t.Fatalf("couldn't create accounts: %+v", r)
	}

	var transfers int64
	for iter := 0; iter < NITER; iter++ {
		var bad atomic.Value
		stop := make(chan struct{})
		var wg sync.WaitGroup

		// auditor
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

		// partitioner
		chPart := make(chan bool)
		if ts.partitions {
			go ts.Partitioner(Gid, chPart)
		}

		ts.SpawnClientsAndWait(ts.nclients, NSEC, func(cli int, tck kvtest.IKVClerk, done chan struct{}) kvtest.ClntRes {
			ck := txnClerk(tck)
			res := kvtest.ClntRes{}
			for {
				select {
				case <-done:
					return res
				default:
				}
				i := rand.Intn(NACCT)
				j := (i + 1 + rand.Intn(NACCT-1)) % NACCT
				r := ck.Txn(nil, []rpc.TxnOp{get(accts[i]), get(accts[j])}, nil)
				bi, _ := strconv.Atoi(r.Results[0].Value)
				bj, _ := strconv.Atoi(r.Results[1].Value)
				if bi == 0 {
					continue
				}
				amt := 1 + rand.Intn(bi)
				r = ck.Txn(
					[]rpc.TxnCompare{cmp(accts[i], r.Results[0].Version), cmp(accts[j], r.Results[1].Version)},
					[]rpc.TxnOp{put(accts[i], strconv.Itoa(bi-amt)), put(accts[j], strconv.Itoa(bj+amt))},
					nil)
				if r.Succeeded {
					res.Nok++
					atomic.AddInt64(&transfers, 1)
				} else {
					res.Nmaybe++
				}
			}
		})

		if ts.partitions {
			chPart <- true
			<-chPart
			ts.Group(Gid).ConnectAll()
			time.Sleep(kvtest.ElectionTimeout)
		}
		close(stop)
		wg.Wait()
		if msg := bad.Load(); msg != nil {
			ts.t.Fatalf("%v", msg)
		}

		if ts.crash {
			ts.killall()
			time.Sleep(kvtest.ElectionTimeout)
			ts.restartall()
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
	ts := MakeTest(t, "5D txn transfer", 5, 5, true, false, false, -1, false)
	tester.AnnotateTest("TestTxnTransfer5D", ts.nservers)
	ts.transferTest()
}

func TestTxnTransferUnreliable5D(t *testing.T) {
	ts := MakeTest(t, "5D txn transfer", 5, 5, false, false, false, -1, false)
	tester.AnnotateTest("TestTxnTransferUnreliable5D", ts.nservers)
	ts.transferTest()
}

func TestTxnTransferPartition5D(t *testing.T) {
	ts := MakeTest(t, "5D txn transfer", 5, 5, false, false, true, -1, false)
	tester.AnnotateTest("TestTxnTransferPartition5D", ts.nservers)
	ts.transferTest()
}

func TestTxnTransferRestartSnapshot5D(t *testing.T) {
	ts := MakeTest(t, "5D txn transfer", 5, 5, false, true, true, 1000, false)
	tester.AnnotateTest("TestTxnTransferRestartSnapshot5D", ts.nservers)
	ts.transferTest()
}

// Clients increment a shared counter with compare-and-put transactions
// over an unreliable network, and count how many of their transactions
// reported success. The final counter must equal the total number of
// successes. Without duplicate detection this fails: a retried
// transaction whose first attempt was applied would report failure
// (its compare no longer holds) even though the increment happened.
func (ts *Test) exactlyOnceTest() {
	const (
		NITER = 3
		NSEC  = 2 * time.Second
		KEY   = "counter"
	)
	defer ts.Cleanup()

	var successes int64
	for iter := 0; iter < NITER; iter++ {
		chPart := make(chan bool)
		if ts.partitions {
			go ts.Partitioner(Gid, chPart)
		}

		ts.SpawnClientsAndWait(ts.nclients, NSEC, func(cli int, tck kvtest.IKVClerk, done chan struct{}) kvtest.ClntRes {
			ck := txnClerk(tck)
			res := kvtest.ClntRes{}
			for {
				select {
				case <-done:
					return res
				default:
				}
				r := ck.Txn(nil, []rpc.TxnOp{get(KEY)}, nil)
				n, _ := strconv.Atoi(r.Results[0].Value)
				ver := r.Results[0].Version
				r = ck.Txn([]rpc.TxnCompare{cmp(KEY, ver)}, []rpc.TxnOp{put(KEY, strconv.Itoa(n+1))}, nil)
				if r.Succeeded {
					res.Nok++
					atomic.AddInt64(&successes, 1)
				} else {
					res.Nmaybe++
				}
			}
		})

		if ts.partitions {
			chPart <- true
			<-chPart
			ts.Group(Gid).ConnectAll()
			time.Sleep(kvtest.ElectionTimeout)
		}
		if ts.crash {
			ts.killall()
			time.Sleep(kvtest.ElectionTimeout)
			ts.restartall()
		}

		r := txnClerk(ts.MakeClerk()).Txn(nil, []rpc.TxnOp{get(KEY)}, nil)
		n, _ := strconv.Atoi(r.Results[0].Value)
		if int64(n) != successes {
			ts.t.Fatalf("after iteration %d: counter = %d but %d increments reported success", iter, n, successes)
		}
	}
	if successes == 0 {
		ts.t.Fatalf("no increment succeeded")
	}
}

func TestTxnExactlyOnceUnreliable5D(t *testing.T) {
	ts := MakeTest(t, "5D txn exactly-once", 5, 5, false, false, false, -1, false)
	tester.AnnotateTest("TestTxnExactlyOnceUnreliable5D", ts.nservers)
	ts.exactlyOnceTest()
}

func TestTxnExactlyOncePartition5D(t *testing.T) {
	ts := MakeTest(t, "5D txn exactly-once", 5, 5, false, false, true, -1, false)
	tester.AnnotateTest("TestTxnExactlyOncePartition5D", ts.nservers)
	ts.exactlyOnceTest()
}

func TestTxnExactlyOnceRestartSnapshot5D(t *testing.T) {
	ts := MakeTest(t, "5D txn exactly-once", 5, 5, false, true, true, 1000, false)
	tester.AnnotateTest("TestTxnExactlyOnceRestartSnapshot5D", ts.nservers)
	ts.exactlyOnceTest()
}
