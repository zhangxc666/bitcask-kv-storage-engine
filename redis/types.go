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

/*
	Hash 数据结构
	meta
	<key+version+field,value>
*/

// HSet Hash 分成两部分，<key,meta>,<key+meta.version+field,value>
func (rds *RedisDataStructure) HSet(key, field, value []byte) (bool, error) {
	meta, err := rds.findMetaData(key, Hash)
	if err != nil {
		return false, err
	}
	// 构造Hash数据部分的key，根据field查找value
	hk := &hashInternalKey{
		key:     key,
		field:   field,
		version: meta.version,
	}
	encKey := hk.encode()
	// 查找对应key的field的数据是否存在
	var exist = true
	if _, err = rds.db.Get(encKey); errors.Is(err, tiny_kvDB.ErrKeyNotFound) {
		exist = false
	}

	// 不存在更新原数据（size增加）
	// 还要新增<field,value>
	// 以上操作需要保证原子性，使用writeBatch

	// 更新元数据
	wb := rds.db.NewWriteBatch(tiny_kvDB.DefaultWriteBatchOptions)
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}

	// 更新<key+version+field,value>
	// 不管存不存在都要更新，存在就是修改，不存在就是新增
	_ = wb.Put(encKey, value)
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

// HGet 找到<key+version+field,value>
func (rds *RedisDataStructure) HGet(key, field []byte) ([]byte, error) {
	meta, err := rds.findMetaData(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encKey := hk.encode()
	return rds.db.Get(encKey)
}

func (rds *RedisDataStructure) HDel(key []byte, field []byte) (bool, error) {
	meta, err := rds.findMetaData(key, Hash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encKey := hk.encode()
	// 先看是否存在
	var exist = true
	if _, err := rds.db.Get(encKey); errors.Is(err, tiny_kvDB.ErrKeyNotFound) {
		exist = false
	}
	// 更新元数据
	if exist {
		wb := rds.db.NewWriteBatch(tiny_kvDB.DefaultWriteBatchOptions)
		meta.size--
		_ = wb.Put(key, meta.encode())
		_ = wb.Delete(encKey)
		if err = wb.Commit(); err != nil {
			return true, err
		}
	}
	return exist, nil
}

/*
	SET数据结构
*/

func (rds *RedisDataStructure) SAdd(key, member []byte) (bool, error) {
	meta, err := rds.findMetaData(key, Set)
	if err != nil {
		return false, err
	}
	// 构造数据部分的key
	// key+version+member+member_size
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	// 如果不存在，就新增，存在就不用管了
	var ok bool
	if _, err := rds.db.Get(sk.encode()); errors.Is(err, tiny_kvDB.ErrKeyNotFound) {
		wb := rds.db.NewWriteBatch(tiny_kvDB.DefaultWriteBatchOptions)
		meta.size++
		_ = wb.Put(key, meta.encode())
		_ = wb.Put(sk.encode(), nil)
		if err := wb.Commit(); err != nil {
			return false, err
		}
		ok = true
	}
	return ok, nil
}

func (rds *RedisDataStructure) SIsMember(key, member []byte) (bool, error) {
	meta, err := rds.findMetaData(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	if _, err := rds.db.Get(sk.encode()); err != nil {
		if errors.Is(err, tiny_kvDB.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (rds *RedisDataStructure) SRem(key, member []byte) (bool, error) {
	meta, err := rds.findMetaData(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	// 报错
	if _, err := rds.db.Get(sk.encode()); err != nil {
		if errors.Is(err, tiny_kvDB.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}

	// 存在更新
	wb := rds.db.NewWriteBatch(tiny_kvDB.DefaultWriteBatchOptions)
	meta.size--
	_ = wb.Put(key, meta.encode())
	_ = wb.Put(sk.encode(), nil)
	if err := wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// 找到元数据
func (rds *RedisDataStructure) findMetaData(key []byte, dataType redisDataType) (*metadata, error) {
	metaBuf, err := rds.db.Get(key)
	if err != nil && !errors.Is(err, tiny_kvDB.ErrKeyNotFound) {
		return nil, err
	}
	var meta *metadata
	var exist = true
	if errors.Is(err, tiny_kvDB.ErrKeyNotFound) {
		exist = false
	} else {
		meta = decodeMetaData(metaBuf)
		// 判断数据类型是否匹配
		if meta.dataType != dataType {
			return nil, ErrWrongTypeOperation
		}
		// 判断过期时间
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}
	if !exist {
		meta = &metadata{
			dataType: dataType,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
		if dataType == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}
	return meta, nil
}
