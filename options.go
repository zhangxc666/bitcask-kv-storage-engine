package tiny_kvDB

type Options struct {
	DirPath      string    // 数据库数据目录
	DataFileSize int64     // 数据文件的大小
	SyncWrites   bool      // 每次写入数据是持久化
	IndexType    IndexType // 索引类型
}

type IndexType = int8

const (
	Btree IndexType = iota + 1
	ART
)
