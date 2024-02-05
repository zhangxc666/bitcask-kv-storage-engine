package redis

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
	tiny_kvDB "tiny-kvDB"
	"tiny-kvDB/utils"
)

func TestRedisDataStructure_Set(t *testing.T) {
	opts := tiny_kvDB.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	if err != nil {
		panic(err)
	}

	v1, v2 := utils.RandomValue(1), utils.RandomValue(2)
	err = rds.Set(utils.GetTestKey(1), v1, 0)
	assert.Nil(t, err)
	err = rds.Set(utils.GetTestKey(2), v2, time.Second*5)
	assert.Nil(t, err)

	value1, err := rds.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, v1, value1)

	time.Sleep(time.Second * 6)
	value2, err := rds.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.Nil(t, value2)

	value3, err := rds.Get(utils.GetTestKey(3))
	assert.Equal(t, tiny_kvDB.ErrKeyNotFound, err)
	assert.Nil(t, value3)
}

func TestRedisDataStructure_Del(t *testing.T) {
	opts := tiny_kvDB.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	if err != nil {
		panic(err)
	}

	// 删除不存在的key
	err = rds.Del(utils.GetTestKey(111))
	assert.Equal(t, nil, err)

	//
	err = rds.Set(utils.GetTestKey(1), utils.RandomValue(100), 0)
	assert.Nil(t, err)
	err = rds.Del(utils.GetTestKey(1))
	assert.Nil(t, err)

	v, err := rds.Get(utils.GetTestKey(1))
	assert.Equal(t, tiny_kvDB.ErrKeyNotFound, err)
	assert.Nil(t, v)
}

func TestRedisDataStructure_Type(t *testing.T) {
	opts := tiny_kvDB.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	if err != nil {
		panic(err)
	}

	err = rds.Set(utils.GetTestKey(1), utils.RandomValue(100), 0)
	assert.Nil(t, err)

	dataType, err := rds.Type(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, String, dataType)
}
