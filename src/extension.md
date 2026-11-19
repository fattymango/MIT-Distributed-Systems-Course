# Lab 5D Extension: Transactions in kvraft

I added etcd-style transactions to kvraft. A client sends `Txn(compares, then, else)`:
if every `(key, version)` in `compares` matches the server's current version (version 0
meaning the key must not exist), the server runs the `then` list of Get/Put operations,
otherwise it runs the `else` list, and returns whether the compares held plus one result
per operation. The whole transaction is submitted to Raft as a single log entry and
applied by `doTxn` under the server lock, so it is atomic and every replica applies it at
the same point in the log; this generalizes versioned Put to multiple keys. Because a
retried transaction would see changed versions and wrongly take the `else` branch, each
clerk has a random client ID and a sequence number, and the server keeps the last
sequence number and reply per client (included in snapshots), returning the saved reply
for duplicates, which makes transactions exactly-once. Tests in `kvraft1/txn_test.go`
check the basic semantics, a bank-transfer workload where an auditor reading all accounts
in one transaction must always see a constant total (under unreliable networks,
partitions, and restarts with snapshots), and an exactly-once counter test that fails if
duplicate detection is removed.