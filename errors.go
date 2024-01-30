package tiny_kvDB

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("key is empty")
	ErrIndexUpdateFailed      = errors.New("fail to update index")
	ErrKeyNotFound            = errors.New("key is not found")
	ErrDataFileNotFound       = errors.New("data file is not found ")
	ErrDatabaseDirIsEmpty     = errors.New("database dir is empty")
	ErrDataSizeIsInvalid      = errors.New("data size is not valid")
	ErrDataDirectoryCorrupted = errors.New("the database directory maybe corrupted")
	ErrExceedMaxBatchNum      = errors.New("exceed max batch number")
)
