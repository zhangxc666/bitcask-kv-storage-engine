package tiny_kvDB

import "os"

type Options struct {
	DirPath      string    // 数据库数据目录
	DataFileSize int64     // 数据文件的大小
	SyncWrites   bool      // 每次写入数据是持久化
	IndexType    IndexType // 索引类型
}

type IteratorOptions struct {
	Prefix  []byte // 遍历前缀为指定值的key
	Reverse bool   // 反向遍历，默认false是正向
}

type IndexType = int8

const (
	Btree IndexType = iota + 1
	ART
)

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024,
	SyncWrites:   false,
	IndexType:    Btree,
}
var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}
