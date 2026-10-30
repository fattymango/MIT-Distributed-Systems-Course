package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockName string
	id       string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.lockName = lockname
	lk.id = kvtest.RandValue(8)
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		val, version, err := lk.ck.Get(lk.lockName)
		if err != rpc.OK {
			switch err {
			case rpc.ErrNoKey:
				err = lk.ck.Put(lk.lockName, lk.id, 0)
				if err == rpc.OK {
					return
				}
				continue
			default:
				panic(err)
			}
		}
		if val == lk.id {
			// this means that some error happened and then we retried, so its actually acquired.
			return
		}
		if val != "" && val != lk.id {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		err = lk.ck.Put(lk.lockName, lk.id, version)
		if err == rpc.OK {
			return
		}
	}
}

func (lk *Lock) Release() {
	// Your code here

	val, version, err := lk.ck.Get(lk.lockName)
	if err != rpc.OK {
		// this means a release without a acquire.
		panic(err)
	}

	if val == lk.id {
		err = lk.ck.Put(lk.lockName, "", version)
		if err != rpc.OK && err != rpc.ErrMaybe {
			panic(err)
		}
		return
	}

}
