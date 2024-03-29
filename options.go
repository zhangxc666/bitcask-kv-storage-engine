package tiny_kvDB

import "os"

// Options db配置
type Options struct {
	DirPath            string    // 数据库数据目录
	DataFileSize       int64     // 数据文件的大小
	SyncWrites         bool      // 每次写入数据是持久化
	BytePerSync        uint      // 累计写入到多少字节进行持久化
	IndexType          IndexType // 索引类型
	MMapAtStartup      bool      // 启动的时候是否加载MMap
	DataFileMergeRatio float32   // 数据merge时的比例
}

// IteratorOptions 迭代器配置
type IteratorOptions struct {
	Prefix  []byte // 遍历前缀为指定值的key
	Reverse bool   // 反向遍历，默认false是正向
}

// WriteBatchOptions 批量写配置
type WriteBatchOptions struct {
	MaxBatchNum uint //一个批次中最大的数据量
	SyncWrite   bool // 提交事务是否持久化
}

type IndexType = int8

const (
	Btree IndexType = iota + 1
	ART
	BPlusTree
)

var DefaultOptions = Options{
	DirPath:            os.TempDir(),
	DataFileSize:       256 * 1024 * 1024,
	BytePerSync:        0,
	SyncWrites:         false,
	IndexType:          Btree,
	MMapAtStartup:      true,
	DataFileMergeRatio: 0.5,
}
var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}
var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrite:   true,
}
