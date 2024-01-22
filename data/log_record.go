package data

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// LogRecordPos 数据内存索引，主要描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件id，表示数据存储到哪个文件中
	Offset int64  // 偏移，表示数据存在文件哪个位置
}

// LogRecord 写入到数据文件的记录
// 因为数据是追加写入的，类似于日志，故称之为日志记录
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// EncodeLogRecord 对应logRecord进行编码
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}
