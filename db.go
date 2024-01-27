package tiny_kvDB

import (
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"tiny-kvDB/data"
	"tiny-kvDB/index"
)

type DB struct {
	options    Options
	mu         *sync.Mutex
	fileIDs    []int                     // 文件ID列表，仅用于加载索引的使用，不能在其他地方更新和使用
	activeFile *data.DataFile            // 当前活跃文件，用于写入
	olderFile  map[uint32]*data.DataFile // 旧的数据文件，仅用于读
	index      index.Indexer             // 内存索引
}

// Open 打开kv存储引擎
func Open(option Options) (*DB, error) {
	if err := checkOptions(option); err != nil {
		return nil, err
	}

	// 判断目录是否存在，如果不存在的话就创建这个目录
	if _, err := os.Stat(option.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(option.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	db := &DB{
		options:   option,
		mu:        new(sync.Mutex),
		olderFile: make(map[uint32]*data.DataFile),
		index:     index.NewIndexer(option.IndexType),
	}

	// 加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// 加载数据索引

	if err := db.loadIndexerFromDataFile(); err != nil {
		return nil, err
	}

	return db, nil
}

// 从磁盘中加载数据文件
func (db *DB) loadDataFiles() error {
	// 找到对应文件夹下的所有文件
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}
	var fileIDs []int
	// 遍历目录中的所有文件，找到所有以.data结尾的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			str := strings.Split(entry.Name(), ".")
			fileID, err := strconv.Atoi(str[0])
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIDs = append(fileIDs, fileID)
		}
	}

	// 对文件ID排序，从小到大依次加载数据文件
	sort.Ints(fileIDs)
	db.fileIDs = fileIDs
	for i, fileID := range fileIDs {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fileID))
		if err != nil {
			return err
		}
		if i == len(fileIDs)-1 { // 活跃文件
			db.activeFile = dataFile
		} else { // 老的文件
			db.olderFile[uint32(fileID)] = dataFile
		}

	}

	return nil
}

func checkOptions(option Options) error {
	if option.DirPath == "" {
		return ErrDatabaseDirIsEmpty
	}
	if option.DataFileSize <= 0 {
		return ErrDataSizeIsInvalid
	}
	return nil
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
	// key不存在
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}
	fileID := logRecordPos.Fid
	offset := logRecordPos.Offset
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
	logRecord, _, err := dataFile.ReadLogRecord(offset)
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

// 加载数据索引
func (db *DB) loadIndexerFromDataFile() error {
	if len(db.fileIDs) == 0 {
		return nil
	}
	// 遍历所有的dataFile
	for i, fid := range db.fileIDs {
		fileID := uint32(fid)
		var dataFile *data.DataFile
		if fileID == db.activeFile.FileID {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFile[fileID]
		}

		// 通过索引从头开始遍历
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			//构建内存索引并保存
			logRecordPos := &data.LogRecordPos{Fid: fileID, Offset: offset}

			// 如果当前的记录是被删除的
			if logRecord.Type == data.LogRecordDeleted {
				db.index.Delete(logRecord.Key)
			} else {
				db.index.Put(logRecord.Key, logRecordPos)
			}

			offset += size
		}

		// 当前如果是活跃文件，需要重新修改活跃文件的写入指针
		if uint32(i) == db.activeFile.FileID {
			db.activeFile.WriteOff = offset
		}
	}
	return nil
}
