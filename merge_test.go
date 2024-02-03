package tiny_kvDB

import (
	"github.com/stretchr/testify/assert"
	"os"
	"strconv"
	"sync"
	"testing"
	"tiny-kvDB/utils"
)

func TestWriteWhileMerge(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.DirPath = dir
	opts.DataFileSize = 1024 * 1024
	opts.DataFileMergeRatio = 0
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 0; i < 50000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}

	wg := new(sync.WaitGroup)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 60000; i < 70000; i++ {
			err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
			assert.Nil(t, err)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 50000; i++ {
			err := db.Delete(utils.GetTestKey(i))
			assert.Nil(t, err)
		}
	}()
	//time.Sleep(time.Millisecond * 100)
	//t.Log(len(db.ListKeys()))
	err = db.Merge()
	assert.Nil(t, err)
	wg.Wait()

	keys := db.ListKeys()
	assert.Equal(t, 10000, len(keys))

	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	defer func() {
		_ = db2.Close()
	}()
	assert.Nil(t, err)
	keys = db2.ListKeys()
	assert.Equal(t, 10000, len(keys))

	for i := 60000; i < 70000; i++ {
		val, err := db2.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.NotNil(t, val)
	}
}

// 测试merge和put过程并发执行
func TestMerge2(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.DirPath = dir
	opts.DataFileSize = 1024 * 1024
	opts.DataFileMergeRatio = 0
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	//先执行
	for i := 0; i < 1000000; i++ {
		err = db.Put(utils.GetTestKey(i), []byte(strconv.Itoa(i)))
		assert.Nil(t, err)
	}
	for i := 0; i < 600000; i++ {
		err = db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}
	var wg sync.WaitGroup
	num := 30
	wg.Add(num + 1)
	// merge的同时进行写操作
	go func() {
		err = db.Merge()
		assert.Nil(t, err)
		wg.Done()
	}()
	for i := 0; i < num; i++ {
		go func(i int) {
			for j := i * (600000 / num); j < (i+1)*(600000/num); j++ {
				err = db.Put(utils.GetTestKey(j), []byte(strconv.Itoa(j+10000)))
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	err = db.Close()
	assert.Nil(t, err)

	db, err = Open(opts)
	//再执行
	for i := 0; i < 600000; i++ {
		val, err := db.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.Equal(t, []byte(strconv.Itoa(i+10000)), val)
	}
	for i := 600000; i < 1000000; i++ {
		val, err := db.Get(utils.GetTestKey(i))
		assert.Nil(t, err)
		assert.Equal(t, []byte(strconv.Itoa(i)), val)
	}

}
