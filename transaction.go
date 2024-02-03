package tiny_kvDB

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
	"tiny-kvDB/data"
)

var txnFinKey = []byte("txn-fin")

const nonTransactionSeqNo uint64 = 0

type WriteBatch struct {
	options       WriteBatchOptions
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord // 暂存写入的数据
}

// NewWriteBatch 初始化WriteBatch
func (db *DB) NewWriteBatch(opt WriteBatchOptions) *WriteBatch {
	// 在B+树索引模式下，seqNo文件不存在，且不是第一次加载
	// 此时无法执行事务操作
	if db.options.IndexType == BPlusTree && !db.seqNoFileExists && !db.isInitial {
		panic("can not use write batch, seq-no file not exists")
	}
	return &WriteBatch{
		options:       opt,
		mu:            new(sync.Mutex),
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// Put 批量写入数据
func (wb *WriteBatch) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 暂存logRecord
	logRecord := &data.LogRecord{Key: key, Value: value}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Delete 删除数据
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 数据不存在直接返回
	logPos := wb.db.index.Get(key)
	if logPos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	// 暂存删除的数据
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Commit 提交事务，将批量数据全部写入磁盘，更新内存索引
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 当前无任何数据
	if len(wb.pendingWrites) == 0 {
		return nil
	}

	// 超过最大的配置的数量
	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}

	// 加锁保证事务的串行化
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()
	// 获取当前的事务序列号
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	// 开始写到数据文件中
	logPosMap := make(map[string]*data.LogRecordPos)
	for _, record := range wb.pendingWrites {
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWithSeq(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		logPosMap[string(record.Key)] = logRecordPos
	}

	// 写一条标识事务完成的数据
	finRecord := &data.LogRecord{Key: logRecordKeyWithSeq(txnFinKey, seqNo), Type: data.LogRecordTxnFinished}
	if _, err := wb.db.appendLogRecord(finRecord); err != nil {
		return err
	}
	// 根据配置文件决定是否持久化
	if wb.options.SyncWrite && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// 更新索引
	for _, record := range wb.pendingWrites {
		var oldPos *data.LogRecordPos
		if record.Type == data.LogRecordNormal {
			pos := logPosMap[string(record.Key)]
			oldPos = wb.db.index.Put(record.Key, pos)
		} else if record.Type == data.LogRecordDeleted {
			oldPos, _ = wb.db.index.Delete(record.Key)
		}
		if oldPos != nil {
			wb.db.reclaimSize += int64(oldPos.Size)
		}
	}

	// 清空暂存数据
	wb.pendingWrites = make(map[string]*data.LogRecord)
	return nil
}

// 把key中加上seqNo
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)
	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key[:])
	return encKey
}

// 解析LogRecord的key，获取对应的key和事务序列号
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
