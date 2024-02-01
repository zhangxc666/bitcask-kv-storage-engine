package tiny_kvDB

import (
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"tiny-kvDB/data"
	"tiny-kvDB/index"
)

const seqNoKey = "seq.no"

type DB struct {
	options         Options
	mu              *sync.RWMutex
	fileIDs         []int                     // 文件ID列表，仅用于加载索引的使用，不能在其他地方更新和使用
	activeFile      *data.DataFile            // 当前活跃文件，用于写入
	olderFile       map[uint32]*data.DataFile // 旧的数据文件，仅用于读
	index           index.Indexer             // 内存索引
	seqNo           uint64                    // 事务序列号，全局递增
	isMerging       bool                      // 是否正在merge
	seqNoFileExists bool                      // seqNo文件是否存在
	isInitial       bool                      // 是否是第一次初始化当前数据库
}

// Open 打开kv存储引擎
func Open(option Options) (*DB, error) {
	if err := checkOptions(option); err != nil {
		return nil, err
	}

	var isInit bool
	// 判断目录是否存在，如果不存在的话就创建这个目录
	if _, err := os.Stat(option.DirPath); os.IsNotExist(err) {
		// 文件夹不存在，首次初始化
		isInit = true
		if err := os.MkdirAll(option.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	entries, err := os.ReadDir(option.DirPath)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		// 文件夹存在，但是没有任何数据，也算首次初始化
		isInit = true
	}

	db := &DB{
		options:   option,
		mu:        new(sync.RWMutex),
		olderFile: make(map[uint32]*data.DataFile),
		index:     index.NewIndexer(option.IndexType, option.DirPath, option.SyncWrites),
		isInitial: isInit,
	}

	// 加载merge数据目录
	if err := db.loadMergeFile(); err != nil {
		return nil, err
	}

	// 加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// 如果是B+树索引，就无需从数据文件中加载索引了
	if option.IndexType == BPlusTree {
		// 定位事务序列号
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}
		// 不会执行loadIndexerFromDataFile函数，故不会更新活跃文件的offset
		// 此时要自己手动设置
		if db.activeFile != nil {
			size, err := db.activeFile.IOManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOff = size
		}
	}

	// 从hint文件中加载索引文件
	if err := db.loadIndexFromHintFile(); err != nil {
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

// 从磁盘中加载数据索引
func (db *DB) loadIndexerFromDataFile() error {
	if len(db.fileIDs) == 0 {
		return nil
	}

	hasMerge, nonMergeFileID := false, uint32(0)
	mergeFinishedFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinishedFileName); err == nil {
		fileID, err := db.getNonMergeFileID(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileID = fileID
	}

	// 更新内存索引函数
	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var ok bool
		// 如果当前的记录是被删除的
		if typ == data.LogRecordDeleted {
			ok = db.index.Delete(key)
		} else {
			ok = db.index.Put(key, pos)
		}
		if !ok {
			panic("failed to update index!")
		}
	}

	//暂存事务的数据
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo = nonTransactionSeqNo

	// 遍历所有的dataFile
	for i, fid := range db.fileIDs {
		fileID := uint32(fid)

		// 当前fileID比nonMergeFileID，此时索引已经从hint文件加载过了
		if hasMerge && fileID < nonMergeFileID {
			continue
		}

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

			// 解析当前key的事务序列号
			realKey, seqNo := parseLogRecordKey(logRecord.Key)

			if seqNo == nonTransactionSeqNo { // 如果不是事务提交的数据
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else { // 事务提交的数据
				// 事务完成，更新所有数据
				if logRecord.Type == data.LogRecordTxnFinished {
					// tips: 索引的key里面不保存事务信息，此时的key都是去除seqNo的realKey
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else { // 提交到缓存区里
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}

			// 更新事务序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			offset += size
		}

		// 当前如果是活跃文件，需要重新修改活跃文件的写入指针
		if uint32(i) == db.activeFile.FileID {
			db.activeFile.WriteOff = offset
		}
	}
	db.seqNo = currentSeqNo
	return nil
}

// 在B+树索引模式下加载事务序列号
func (db *DB) loadSeqNo() error {
	fileName := filepath.Join(db.options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}
	seqNoFile, err := data.OpenSeqNodFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record, _, err := seqNoFile.ReadLogRecord(0)
	if err != nil {
		return err
	}
	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}
	db.seqNo = seqNo
	db.seqNoFileExists = true
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

func (db *DB) Close() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	// 关闭索引（主要是在b+树模式下关闭，art和b树吴影响）
	if err := db.index.Close(); err != nil {
		return err
	}

	// 在B+树索引模式下保存当前事务的序列号
	seqNoFile, err := data.OpenSeqNodFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record := &data.LogRecord{
		Key:   []byte(seqNoKey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)),
	}
	encRecord, _ := data.EncodeLogRecord(record)
	if err := seqNoFile.Write(encRecord); err != nil {
		return err
	}
	if err := seqNoFile.Sync(); err != nil {
		return err
	}

	// 关闭当前活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}
	// 关闭旧的数据文件
	for _, file := range db.olderFile {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

// Put DB写入Key、Value
func (db *DB) Put(key []byte, value []byte) error {
	// key为空时
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	// 构造LogRecord
	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 追加写入到当前活跃数据文件中
	pos, err := db.appendLogRecordWithLock(logRecord)
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
	// 从数据文件中获取value
	return db.getValueByPosition(logRecordPos)
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if pos := db.index.Get(key); pos == nil {
		return nil
	}
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted,
	}

	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// 从内存索引中将对应的key删除
	if ok := db.index.Delete(key); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// appendLogRecord 追加写入到活跃文件中
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
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

func (db *DB) getValueByPosition(pos *data.LogRecordPos) ([]byte, error) {
	fileID := pos.Fid
	offset := pos.Offset
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

// ListKeys 获取数据库中的所有key
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	defer iterator.Close()
	keys := make([][]byte, db.index.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}

// Fold 获取所有的数据，根据用户自定义的函数进行操作
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	iter := db.index.Iterator(false)
	defer iter.Close()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		value, err := db.getValueByPosition(iter.Value())
		if err != nil {
			return err
		}
		if !fn(iter.Key(), value) {
			break
		}
	}
	return nil
}
