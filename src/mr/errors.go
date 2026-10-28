package mr

import "errors"

var (
	ErrCallFailed      = errors.New("call failed")
	ErrNoAvailabeFiles = errors.New("No available files")
	ErrShutdown        = errors.New("shutdown")
)
