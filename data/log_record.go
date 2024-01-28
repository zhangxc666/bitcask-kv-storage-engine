package data

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

const maxLogRecordHeaderSize = 15

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

// LogRecord的头部信息
type logRecordHeader struct {
	crc        uint32        // crc校验值
	recordType LogRecordType // 标识是否删除
	keySize    uint32        // key的长度
	valueSize  uint32        // value的长度
}

// EncodeLogRecord 对应logRecord进行编码
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}

func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	return nil, 0
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	return 0
}
