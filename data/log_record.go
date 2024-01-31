package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished
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

// TransactionRecord 事务记录，存储logRecord和索引信息
type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

// logRecord 4    1           5        5         key     value
//
//	crc  recordType  keySize  valueSize
//
// EncodeLogRecord 对应logRecord进行编码，根据logRecord自动补充上头部信息编码存到disk中
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	headerBytes := make([]byte, maxLogRecordHeaderSize)
	headerBytes[4] = logRecord.Type
	index := 5
	// 装 keySize 和 valueSize 到headerBytes中
	index += binary.PutVarint(headerBytes[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(headerBytes[index:], int64(len(logRecord.Value)))

	sumLen := index + len(logRecord.Key) + len(logRecord.Value)
	retBytes := make([]byte, sumLen)
	// 拷贝 headerBytes 到 retBytes中去
	copy(retBytes[:index], headerBytes[:index])
	// 将logRecord的key和value拷贝到retBytes
	copy(retBytes[index:index+len(logRecord.Key)], logRecord.Key)
	copy(retBytes[index+len(logRecord.Key):], logRecord.Value)
	//进行crc校验
	crc := crc32.ChecksumIEEE(retBytes[4:])
	binary.LittleEndian.PutUint32(retBytes[:4], crc)

	//fmt.Printf("header's total len: %d , crc is %d\n", index, crc)
	return retBytes, int64(sumLen)
}

func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32+binary.MaxVarintLen64)
	var index = 0
	index += binary.PutVarint(buf[index:], int64(pos.Fid))
	index += binary.PutVarint(buf[index:], pos.Offset)
	return buf[:index]
}

func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	if len(buf) <= 4 { // crc未传到
		return nil, 0
	}
	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}
	// 从字节流中取出keySize和valueSize
	index := 5
	keySize, n := binary.Varint(buf[index:])
	index += n
	valueSize, n := binary.Varint(buf[index:])
	index += n
	header.keySize = uint32(keySize)
	header.valueSize = uint32(valueSize)
	return header, int64(index)
}

func DecodeLogRecordPos(buf []byte) *LogRecordPos {
	var index = 0
	fileID, n := binary.Varint(buf[index:])
	index += n
	offset, _ := binary.Varint(buf[index:])
	return &LogRecordPos{Fid: uint32(fileID), Offset: offset}
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}
	crc := crc32.ChecksumIEEE(header[:]) // 此时传入的header已经取出了crc字节
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)
	return crc
}
