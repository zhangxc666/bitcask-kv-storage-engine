package tiny_kvDB

import (
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"tiny-kvDB/data"
	"tiny-kvDB/utils"
)

const (
	mergePathName    = "-merge"
	mergeFinishedKey = "merge.finished"
)

// Merge 清理无效文件生成Hint文件
func (db *DB) Merge() error {
	// 数据库为空
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()

	// 如果merge正在进行，直接返回
	if db.isMerging {
		db.mu.Unlock()
		return ErrMergeIsProgress
	}
	db.isMerging = true
	defer func() { db.isMerging = false }()

	totalSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		return err
	}
	// 查看当前merge的数据是否达到了阈值
	if float32(db.reclaimSize)/float32(totalSize) < db.options.DataFileMergeRatio {
		db.mu.Unlock()
		return ErrMergeRatioUnreached
	}

	// 查看剩余空间容量时否可以容量merge后的文件大小
	availableSize, err := utils.AvailableDiskSize()
	if err != nil {
		db.mu.Unlock()
		return err
	}
	if uint64(totalSize-db.reclaimSize) >= availableSize {
		db.mu.Unlock()
		return ErrNoEnoughSpaceForMerge
	}

	// 持久化活跃文件
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}
	db.olderFile[db.activeFile.FileID] = db.activeFile
	if err := db.setActiveDataFile(); err != nil {
		db.mu.Unlock()
		return nil
	}
	// 记录最近没有参与的merge文件的ID，用后续的merge完成标识
	nonMergeFileID := db.activeFile.FileID

	// 取出所有需要的merge文件
	var mergeFile []*data.DataFile
	for _, file := range db.olderFile {
		mergeFile = append(mergeFile, file)
	}
	db.mu.Unlock()

	// 将merge文件从小到大进行排序，一次merge
	sort.Slice(mergeFile, func(i, j int) bool {
		return mergeFile[i].FileID < mergeFile[j].FileID
	})

	mergePath := db.getMergePath()
	// 如果目录存在，之前经过merge，将其删掉
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}

	// 新建一个merge path目录
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	// 打开一个新建的bitcask实例
	mergeOptions := db.options
	mergeOptions.DirPath = mergePath
	mergeOptions.SyncWrites = false
	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}

	// 打开hint文件存储索引
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}

	// 遍历每个数据文件
	for _, dataFile := range mergeFile {
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			// 解析拿到的key
			realKey, _ := parseLogRecordKey(logRecord.Key)
			logRecordPos := db.index.Get(realKey)

			// 此时内存索引的数据和当前数据文件的数据是一样的，表示当前数据是有效的
			if logRecordPos != nil && logRecordPos.Offset == offset && logRecordPos.Fid == dataFile.FileID {
				// 清除事务标记
				logRecord.Key = logRecordKeyWithSeq(realKey, nonTransactionSeqNo)
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}

				// 将位置索引写到hint文件中
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}
			}
			offset += size
		}
	}

	// 保证持久化
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDB.Sync(); err != nil {
		return err
	}

	// 写标识merge过程完成的文件
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	mergeFinRecord := &data.LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileID))),
	}
	encRecord, _ := data.EncodeLogRecord(mergeFinRecord)
	if err := mergeFinishedFile.Write(encRecord); err != nil {
		return err
	}

	return nil
}

// 获取merge路径
func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.options.DirPath)) // 取出当前数据的父级目录
	base := path.Base(db.options.DirPath)
	return filepath.Join(dir, base+mergePathName)
}

// 加载merge数据目录
func (db *DB) loadMergeFile() error {
	mergePath := db.getMergePath()
	// merge不存在直接返回
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}

	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	// 读取merge中所有文件
	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// 查找标识merge完成的文件是否存在
	var (
		mergeFinished  bool
		mergeFileNames []string
	)

	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
			break
		}
		if entry.Name() == data.SeqNoFileName {
			continue
		}
		if entry.Name() == fileLockName {
			continue
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}

	// merge未完成
	if !mergeFinished {
		return nil
	}

	nonMergeFileID, err := db.getNonMergeFileID(mergePath)
	if err != nil {
		return err
	}

	// 删除旧的标识文件
	var fileID uint32 = 0
	for ; fileID < nonMergeFileID; fileID++ {
		fileName := data.GetDataFileName(db.options.DirPath, fileID)
		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}

	// 移动新的数据文件至数据目录中
	for _, fileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, fileName)
		destPath := filepath.Join(db.options.DirPath, fileName)
		if err := os.Rename(srcPath, destPath); err != nil {
			return err
		}
	}

	return nil
}

// 获取最近没有参数merge的文件ID
func (db *DB) getNonMergeFileID(dirPath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}
	record, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	nonMergeFileID, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}
	return uint32(nonMergeFileID), nil
}

// 从hint文件中加载索引文件
func (db *DB) loadIndexFromHintFile() error {
	// 查看索引文件是否存在
	hintFileName := filepath.Join(db.options.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	// 打开hint索引文件
	hintFile, err := data.OpenHintFile(db.options.DirPath)
	if err != nil {
		return err
	}

	// 读取hint文件中的索引
	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// 解码拿到索引信息
		pos := data.DecodeLogRecordPos(logRecord.Value)
		db.index.Put(logRecord.Key, pos)
		offset += size
	}

	return nil
}
