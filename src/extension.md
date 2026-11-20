# Lab 5D Extension: Transactions in kvraft and cross-shard transactions in shardkv

## Single-group transactions (kvraft)

I added etcd-style transactions to kvraft. A client calls `Txn(compares, then, else)`: if every
`(key, version)` in `compares` matches the key's current version (version 0 means the key must not
exist), the server runs the `then` list of Get/Put operations, otherwise the `else` list, and
returns whether the compares held plus one result per operation. The whole transaction is
submitted to Raft as one log entry and applied by `doTxn` under the server lock, so it is atomic
and every replica applies it at the same log position; it generalizes versioned Put to several
keys. A retried transaction would see changed versions and wrongly take the `else` branch, so each
clerk has a random client ID and a sequence number, and the server keeps the last sequence number
and reply per client (included in snapshots) and returns the saved reply for duplicates, which
makes transactions exactly-once. Tests are in `kvraft1/txn_test.go`.

## Cross-shard transactions (shardkv)

I then extended shardkv so one transaction can touch keys in different shards and shardgrps, using
two-phase commit with two-phase locking. Each shard is a participant. The shardkv clerk acts as
coordinator: it splits the transaction by shard and sends each shard a `TxnPrepare` with only its
compares and operations. The shardgrp runs Prepare through Raft: it refuses if another transaction
holds any of the keys, otherwise it locks them, checks its compares, stores the pending operations,
and votes. The coordinator then decides THEN (all locked, all compares held), ELSE (all locked, some
compare failed) or ABORT (a lock conflict), and records the decision in the controller's kvsrv
under `txn-<id>` with a versioned Put at version 0, so the first writer wins and everyone obeys the
stored value. Finally it sends `TxnFinish` with the decision to every shard, which applies the
chosen branch, releases the locks, and remembers the outcome so repeated Prepare/Finish RPCs are
idempotent; results are reassembled in the caller's original order. Plain Puts on locked keys are
rejected so a prepared transaction's compares stay valid. Locks, pending transactions and finished
transactions are stored inside each shard's state, so they move with the shard during Freeze/Install
and survive snapshots. If a coordinator dies after Prepare, a client that keeps hitting its lock
writes ABORT into the dead transaction's decision key (or finds the decision that won) and finishes
that shard itself, so locks cannot be held forever. Tests in `shardkv1/txn_test.go` check the basic
semantics across groups, recovery from a dead coordinator, a bank-transfer workload whose total an
auditor must always see unchanged, and paired counters that must stay equal, under unreliable
networks and while groups join and leave and shards (with prepared transactions in them) move.