package data

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
	"tiny-kvDB/fio"
)

const (
	DataFileNameSuffix    = ".data"
	HintFileName          = "hint-index"
	MergeFinishedFileName = "merge-finished"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value, log record maybe corrupted")
)

// DataFile 数据文件
type DataFile struct {
	FileID    uint32        //文件ID
	WriteOff  int64         // 文件写入的位置
	IOManager fio.IOManager // io读写管理
}

// OpenDataFile 打开新的数据文件
func OpenDataFile(dirPath string, fileID uint32) (*DataFile, error) {
	fileName := GetDataFileName(dirPath, fileID)
	return newDataFile(fileName, fileID)
}

// OpenHintFile 打开新的hint索引文件
func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return newDataFile(fileName, 0)
}

// OpenMergeFinishedFile 打开标识merge完成的文件
func OpenMergeFinishedFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFinishedFileName)
	return newDataFile(fileName, 0)
}

func GetDataFileName(dirPath string, fileID uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileID)+DataFileNameSuffix)
}
func newDataFile(fileName string, fileID uint32) (*DataFile, error) {
	// 初始化IO管理器
	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileID:    fileID,
		WriteOff:  0,
		IOManager: ioManager,
	}, nil
}

func (df *DataFile) Sync() error {
	return df.IOManager.Sync()
}

func (df *DataFile) Close() error {
	return df.IOManager.Close()
}
func (df *DataFile) Write(buf []byte) error {
	n, err := df.IOManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n)
	return nil
}

// WriteHintRecord 写入索引信息至hint文件中
func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	logRecord := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
	}
	encBuf, _ := EncodeLogRecord(logRecord)
	return df.Write(encBuf)
}

// 往 offset 位置读n个字节
func (df *DataFile) readNBtyes(n int64, offset int64) ([]byte, error) {
	b := make([]byte, n)
	_, err := df.IOManager.Read(b, offset)
	return b, err
}

// ReadLogRecord 读记录LogRecord
// 思路是先读取头部，根据头部信息读取key和value，返回LogRecord
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	// 读取header信息
	fileSize, err := df.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}
	var headerBytes int64 = maxLogRecordHeaderSize
	// 如果最大的header长度已经超过了文件的长度，则只需要读取到文件的末尾即可
	// 当文件是删除记录时，满足上述情况，需要特判
	if offset+int64(maxLogRecordHeaderSize) > fileSize {
		headerBytes = fileSize - offset
	}

	headerBuf, err := df.readNBtyes(headerBytes, offset)
	if err != nil {
		return nil, 0, nil
	}
	header, headerSize := decodeLogRecordHeader(headerBuf)
	// 下面的两个条件标识读取到了文件的末尾
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}
	// 总记录长度
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	recordSize := keySize + valueSize + headerSize

	logRecord := &LogRecord{Type: header.recordType}
	// 读取实际的key和value
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBtyes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		// 解出key和value
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	// 校验数据的有效性
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}
	return logRecord, recordSize, nil
}
