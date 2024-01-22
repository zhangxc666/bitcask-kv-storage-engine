package tiny_kvDB

import (
	"sync"
	"tiny-kvDB/data"
	"tiny-kvDB/index"
)

type DB struct {
	options    Options
	mu         *sync.Mutex
	activeFile *data.DataFile            // 当前活跃文件，用于写入
	olderFile  map[uint32]*data.DataFile // 旧的数据文件，仅用于读
	index      index.Indexer             // 内存索引
}

// Put DB写入Key、Value
func (db *DB) Put(key []byte, value []byte) error {
	// key为空时
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	// 构造LogRecord
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 追加写入到当前活跃数据文件中
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}
	// 更新索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// Get 获取对应的key的数据
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	logRecordPos := db.index.Get(key)
	fileID := logRecordPos.Fid
	offset := logRecordPos.Offset
	// key不存在
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}
	var dataFile *data.DataFile
	if db.activeFile.FileID == fileID {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFile[fileID]
	}
	// dataFile不存在
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}
	// 读取对应的文件
	logRecord, err := dataFile.ReadLogRecord(offset)
	if err != nil {
		return nil, err
	}
	// 已经被删除了
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	return logRecord.Value, nil
}

// appendLogRecord 追加写入到活跃文件中
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 判断活跃文件是否存在
	// 如果为空初始化活跃文件
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	// 写入数据编码
	encRecord, size := data.EncodeLogRecord(logRecord)
	// 如果当前写入数据大小+活跃文件大小 超过了 活跃文件的上限值，关闭活跃文件，打开新的活跃文件
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		// 持久化活跃文件
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		// 将活跃文件转换为旧的活跃文件
		db.olderFile[db.activeFile.FileID] = db.activeFile
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}
	// 根据用户配置决定是否持久化写入
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}
	// 构造内存索引信息
	pos := &data.LogRecordPos{Fid: db.activeFile.FileID, Offset: writeOff}
	return pos, nil

}

// setActiveDataFile 设置当前的活跃文件，并发访问需要加锁
func (db *DB) setActiveDataFile() error {
	var initialFileID uint32 = 0
	if db.activeFile != nil {
		initialFileID = db.activeFile.FileID + 1
	}
	// 打开新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileID)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}
