package tiny_kvDB

import "errors"

var (
	ErrKeyIsEmpty        = errors.New("key is empty")
	ErrIndexUpdateFailed = errors.New("fail to update index")
	ErrKeyNotFound       = errors.New("key is not found")
	ErrDataFileNotFound  = errors.New("data file is not found ")
)
