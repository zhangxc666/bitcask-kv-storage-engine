package tiny_kvDB

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"tiny-kvDB/utils"
)

func TestDB_WriteBatch(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-watchbatch-1")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	value := utils.RandomValue(1)
	err = wb.Put(utils.GetTestKey(1), value)
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(2))
	assert.Nil(t, err)

	// 未提交，没有数据
	val, err := db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, val)

	// 提交数据
	err = wb.Commit()
	assert.Nil(t, err)

	//获取提交后的数据
	val, err = db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, value, val)

	// 事务删除数据
	wb2 := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb2.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)
	err = wb2.Commit()
	assert.Nil(t, err)

	// 查不到数据了
	val2, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, val2)
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestDB_WriteBatchRestart(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-watchbatch-2")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(1), utils.RandomValue(1))
	assert.Nil(t, err)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	value := utils.RandomValue(2)
	err = wb.Put(utils.GetTestKey(2), value)
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)

	err = wb.Put(utils.GetTestKey(10), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Commit()
	assert.Nil(t, err)

	// 重启，此时仅有数据2
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	assert.Nil(t, err)
	assert.Equal(t, uint64(2), db2.seqNo)

	val, err := db2.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, val)

	val, err = db2.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.Equal(t, value, val)
}

func TestDB_WriteBatch3(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-watchbatch-3")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	//wbOpts := DefaultWriteBatchOptions
	//wbOpts.MaxBatchNum = 10000000
	//wb := db.NewWriteBatch(wbOpts)
	//for i := 0; i < 500000; i++ {
	//	err := wb.Put(utils.GetTestKey(i), utils.RandomValue(1024))
	//	assert.Nil(t, err)
	//}
	//
	//err = wb.Commit()
	//assert.Nil(t, err)
	//
	//err = db.Close()
	//assert.Nil(t, err)

	keys := db.ListKeys()
	assert.Equal(t, 0, len(keys))
}
