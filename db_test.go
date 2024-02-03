package tiny_kvDB

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
	"tiny-kvDB/utils"
)

// 测试完成之后销毁DB数据目录
func destroyDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			if err := db.Close(); err != nil {
			}
		}
		err := os.RemoveAll(db.options.DirPath)
		if err != nil {
			panic(err)
		}
	}
}

func TestOpen(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
}

func TestMuchDatas(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	fmt.Println(dir)
	opts.DirPath = dir
	db, err := Open(opts)
	//defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 10000000; i++ {
		err := db.Put(utils.GetTestKey(1), utils.RandomValue(1))
		assert.Nil(t, err)
	}

}

// 测试使用mmap打开的时间差
func TestOpen2(t *testing.T) {
	opt := DefaultOptions
	opt.DirPath = "/tmp/bitcask-go843619074"
	opt.MMapAtStartup = false

	now := time.Now()
	db, err := Open(opt)
	t.Log("open times ", time.Since(now))

	assert.Nil(t, err)
	assert.NotNil(t, db)
}

func TestDB_Put(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	//1、正常Put一条数据
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)

	//2、重复Put key相同的数据
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val, err = db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)

	//3.key为空
	err = db.Put(nil, utils.RandomValue(24))
	assert.Equal(t, err, ErrKeyIsEmpty)

	//4.value为空
	err = db.Put(utils.GetTestKey(22), nil)
	assert.Nil(t, err)

	//5.写到数据文件进行了转化
	for i := 0; i < 1000000; i++ {
		err := db.Put(utils.GetTestKey(1), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	assert.Equal(t, 2, len(db.olderFile))

	err = db.Close()
	assert.Nil(t, err)

	//6、重启后再Put
	db2, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	val = utils.RandomValue(128)
	db2.Put(utils.GetTestKey(10), val)
	val1, _ := db2.Get(utils.GetTestKey(10))
	assert.Equal(t, val1, val)
}

func TestDB_Get(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	//1、读取不存在的key
	val, err := db.Get([]byte("key"))
	assert.Nil(t, val)
	assert.Equal(t, ErrKeyNotFound, err)

	//2、重复Put后读取
	db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val, err = db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)

	//3、删除后再读取
	err = db.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)
	val, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, err, ErrKeyNotFound)
	assert.Equal(t, 0, len(val))
}

func TestDB_Delete(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	//删除不存在的一个key
	err = db.Delete([]byte("name"))
	assert.Nil(t, err)

	//空key
	err = db.Delete(nil)
	assert.Equal(t, err, ErrKeyIsEmpty)

	//删除后再Put
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Delete([]byte(utils.GetTestKey(1)))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)

	err = db.Close()
	assert.Nil(t, err)

	//重启之后在进行校验
	db2, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	val1, _ := db2.Get(utils.GetTestKey(1))
	assert.Equal(t, val1, val)
}

func TestDB_Fold(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-listkeys")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(10), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(11))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(12), utils.RandomValue(12))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(13), utils.RandomValue(13))
	assert.Nil(t, err)
	f := func(key []byte, value []byte) bool {
		assert.NotNil(t, key)
		assert.NotNil(t, value)
		if bytes.Compare(key, utils.GetTestKey(12)) == 0 {
			return false
		}
		return true
	}
	err = db.Fold(f)
	assert.Nil(t, err)
}

func TestDB_ListKeys(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-listkeys")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 为空
	keys := db.ListKeys()
	assert.Equal(t, 0, len(keys))

	// 只有一条数据
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(1))
	assert.Nil(t, err)
	key2 := db.ListKeys()
	assert.Equal(t, 1, len(key2))

	// 多条数据
	err = db.Put(utils.GetTestKey(12), utils.RandomValue(12))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(13), utils.RandomValue(13))
	assert.Nil(t, err)
	key3 := db.ListKeys()
	assert.Nil(t, err)
	assert.Equal(t, 3, len(key3))
}

func TestDB_Close(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-close")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(1), utils.RandomValue(1))
	assert.Nil(t, err)
}

func TestDB_Sync(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-close")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(1), utils.RandomValue(1))
	assert.Nil(t, err)

	err = db.Sync()
	assert.Nil(t, err)
}

func TestDB_FileLock(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-close")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	_, err = Open(opts)
	assert.Equal(t, ErrDataBaseIsUsing, err)

	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db2)

	err = db2.Close()
	assert.Nil(t, err)
}

func TestDB_Stat(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-close")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 100; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}

	for i := 100; i < 1000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}

	for i := 2000; i < 5000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}

	stat := db.Stat()
	assert.NotNil(t, stat)
}
