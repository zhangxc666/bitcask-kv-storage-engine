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

func TestRedisDataStructure_HGet(t *testing.T) {
	opts := tiny_kvDB.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	if err != nil {
		panic(err)
	}

	v1, v2 := utils.RandomValue(1), utils.RandomValue(2)
	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("k1"), v1)
	assert.Nil(t, err)
	assert.True(t, ok1)

	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("k1"), v2)
	assert.Nil(t, err)
	assert.False(t, ok2)

	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("k2"), v2)
	assert.Nil(t, err)
	assert.True(t, ok3)

	value1, err := rds.HGet(utils.GetTestKey(1), []byte("k1"))
	assert.Nil(t, err)
	assert.Equal(t, v2, value1)

	value2, err := rds.HGet(utils.GetTestKey(1), []byte("k2"))
	assert.Nil(t, err)
	assert.Equal(t, v2, value2)
}

func TestRedisDataStructure_HDel(t *testing.T) {
	opts := tiny_kvDB.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	if err != nil {
		panic(err)
	}

	ok, err := rds.HDel(utils.GetTestKey(1), []byte("ss"))
	assert.Nil(t, err)
	assert.False(t, ok)

	v1, v2 := utils.RandomValue(1), utils.RandomValue(2)
	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("k1"), v1)
	assert.Nil(t, err)
	assert.True(t, ok1)

	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("k1"), v2)
	assert.Nil(t, err)
	assert.False(t, ok2)

	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("k2"), v2)
	assert.Nil(t, err)
	assert.True(t, ok3)

	ok4, err := rds.HDel(utils.GetTestKey(1), []byte("k1"))
	assert.True(t, ok4)
	assert.Nil(t, err)

	value, err := rds.HGet(utils.GetTestKey(1), []byte("k1"))
	assert.Equal(t, tiny_kvDB.ErrKeyNotFound, err)
	assert.Nil(t, value)
}

func TestRedisDataStructure_SAdd(t *testing.T) {
	opts := tiny_kvDB.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	if err != nil {
		panic(err)
	}

	v1, v2, v3 := []byte("v1"), []byte("v1"), []byte("v2")
	ok, err := rds.SAdd(utils.GetTestKey(1), v1)
	assert.True(t, ok)
	assert.Nil(t, err)
	ok, err = rds.SAdd(utils.GetTestKey(1), v2)
	assert.False(t, ok)
	assert.Nil(t, err)
	ok, err = rds.SAdd(utils.GetTestKey(1), v3)
	assert.True(t, ok)
	assert.Nil(t, err)
}

func TestRedisDataStructure_SIsMember(t *testing.T) {
	opts := tiny_kvDB.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	if err != nil {
		panic(err)
	}

	v1, v2, v3 := []byte("v1"), []byte("v1"), []byte("v2")

	ok, err := rds.SAdd(utils.GetTestKey(1), v1)
	assert.True(t, ok)
	assert.Nil(t, err)

	ok, err = rds.SAdd(utils.GetTestKey(1), v2)
	assert.False(t, ok)
	assert.Nil(t, err)

	ok, err = rds.SAdd(utils.GetTestKey(1), v3)
	assert.True(t, ok)
	assert.Nil(t, err)

	ok, err = rds.SIsMember(utils.GetTestKey(2), v1)
	assert.False(t, ok)
	assert.Nil(t, err)

	ok, err = rds.SIsMember(utils.GetTestKey(1), v1)
	assert.True(t, ok)
	assert.Nil(t, err)

	ok, err = rds.SIsMember(utils.GetTestKey(1), v2)
	assert.True(t, ok)
	assert.Nil(t, err)

	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("zxc666"))
	assert.False(t, ok)
	assert.Nil(t, err)
}

func TestRedisDataStructure_SRem(t *testing.T) {
	opts := tiny_kvDB.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	if err != nil {
		panic(err)
	}

	v1, v2, v3 := []byte("v1"), []byte("v1"), []byte("v2")

	ok, err := rds.SAdd(utils.GetTestKey(1), v1)
	assert.True(t, ok)
	assert.Nil(t, err)

	ok, err = rds.SAdd(utils.GetTestKey(1), v2)
	assert.False(t, ok)
	assert.Nil(t, err)

	ok, err = rds.SAdd(utils.GetTestKey(1), v3)
	assert.True(t, ok)
	assert.Nil(t, err)

	ok, err = rds.SRem(utils.GetTestKey(2), v1)
	assert.Nil(t, err)
	assert.False(t, ok)

	ok, err = rds.SRem(utils.GetTestKey(1), v1)
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SRem(utils.GetTestKey(1), []byte("zxc666"))
	assert.Nil(t, err)
	assert.False(t, ok)
}
