package tiny_kvDB

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"tiny-kvDB/utils"
)

func TestDB_NewIterator(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, false, iterator.Valid())
}

func TestDB_NewIterator_OneValue(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 一条数据
	key, value := utils.GetTestKey(1), utils.RandomValue(1)
	err = db.Put(key, value)
	assert.Nil(t, err)

	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, true, iterator.Valid())
	iterValue, err := iterator.Value()
	assert.Nil(t, err)
	assert.Equal(t, key, iterator.Key())
	assert.Equal(t, iterValue, value)
}

func TestDB_NewIterator_MultiValue(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 多条数据
	err = db.Put([]byte("baa"), utils.RandomValue(1))
	assert.Nil(t, err)
	err = db.Put([]byte("abb"), utils.RandomValue(1))
	assert.Nil(t, err)
	err = db.Put([]byte("cc"), utils.RandomValue(1))
	assert.Nil(t, err)
	iter := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iter)

	// 正向遍历
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
	}

	iter.Rewind()
	for iter.Seek([]byte("b")); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
	}

	// 反向迭代
	opt2 := DefaultIteratorOptions
	opt2.Reverse = true
	iter2 := db.NewIterator(opt2)
	assert.NotNil(t, iter2)
	for iter2.Rewind(); iter2.Valid(); iter2.Next() {
		assert.NotNil(t, iter2.Key())
	}

	for iter2.Seek([]byte("b")); iter2.Valid(); iter2.Next() {
		assert.NotNil(t, iter2.Key())
	}

	// 测试prefix
	opt3 := DefaultIteratorOptions
	opt3.Prefix = []byte("b")
	iter3 := db.NewIterator(opt3)
	assert.NotNil(t, iter3)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}
}
