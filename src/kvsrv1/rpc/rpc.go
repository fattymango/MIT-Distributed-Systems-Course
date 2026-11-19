package rpc

type Err string

const (
	// Err's returned by server and Clerk
	OK         = "OK"
	ErrNoKey   = "ErrNoKey"
	ErrVersion = "ErrVersion"

	// Err returned by Clerk only
	ErrMaybe = "ErrMaybe"

	// For future kvraft lab
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongGroup  = "ErrWrongGroup"

	// For Transactions
	TxnGet = "Get"
	TxnPut = "Put"
)

type Tversion uint64

type PutArgs struct {
	Key     string
	Value   string
	Version Tversion
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value   string
	Version Tversion
	Err     Err
}

type TxnCompare struct {
	Key     string
	Version Tversion
}

type TxnOp struct {
	Type  string
	Key   string
	Value string
}

type TxnOpResult struct {
	Value   string
	Version Tversion
	Err     Err
}
type TxnArgs struct {
	ClientId int64
	Seq      int64
	Compares []TxnCompare
	Then     []TxnOp
	Else     []TxnOp
}

type TxnReply struct {
	Err       Err
	Succeeded bool
	Results   []TxnOpResult
}
