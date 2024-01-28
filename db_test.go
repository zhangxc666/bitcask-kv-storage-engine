package tiny_kvDB

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"tiny-kvDB/utils"
)

// 测试完成之后销毁DB数据目录
func destroyDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			_ = db.activeFile.Close()
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
	//6、重启后再Put
	err = db.activeFile.Close()
	assert.Nil(t, err)

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

	//重启之后在进行校验
	db2, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	val1, _ := db2.Get(utils.GetTestKey(1))
	assert.Equal(t, val1, val)
}
