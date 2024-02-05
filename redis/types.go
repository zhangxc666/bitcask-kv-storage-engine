package redis

import (
	"encoding/binary"
	"errors"
	"time"
	tiny_kvDB "tiny-kvDB"
)

var (
	ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value.")
)

type redisDataType = byte

const (
	String redisDataType = iota
	Hash
	Set
	List
	ZSet
)

// RedisDataStructure redis数据结构服务
type RedisDataStructure struct {
	db *tiny_kvDB.DB
}

// NewRedisDataStructure 初始化redis数据结构服务
func NewRedisDataStructure(option tiny_kvDB.Options) (*RedisDataStructure, error) {
	db, err := tiny_kvDB.Open(option)
	if err != nil {
		return nil, err
	}
	return &RedisDataStructure{db: db}, nil
}

/*
string
value: type(1B)+expire(8B)+value(N B)
*/

func (rds *RedisDataStructure) Set(key, value []byte, ttl time.Duration) error {
	if value == nil {
		return nil
	}

	// 编码value : type+expire +payload
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = String
	var index = 1
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buf[index:], expire)
	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)

	// 调用存储引擎的接口进行写入
	return rds.db.Put(key, encValue)
}

func (rds *RedisDataStructure) Get(key []byte) ([]byte, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}

	// 取出value进行判断是否正确类型，是否过期，最后取出对应的值
	dataType := encValue[0]
	// 判断是否为string
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}

	// 取出过期时间
	var index = 1
	expire, n := binary.Varint(encValue[index:])
	index += n
	// 判断是否过期
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}
	// 合法，返回对应的value
	return encValue[index:], nil
}
