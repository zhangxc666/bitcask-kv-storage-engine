package redis

import (
	"encoding/binary"
	"math"
	"tiny-kvDB/utils"
)

const (
	maxMetaDataSize   = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	extraListMetaSize = binary.MaxVarintLen64 * 2
	initialListMark   = math.MaxUint64 / 2
)

// 元数据
type metadata struct {
	dataType byte   // 数据类型 1B
	expire   int64  //过期时间 最大10B
	version  int64  // 版本号 最大10B
	size     uint32 // 数据量 5B
	head     uint64 // List专用
	tail     uint64 // List专用
}

// 对元数据编码
func (md *metadata) encode() []byte {
	size := maxMetaDataSize
	if md.dataType == List {
		size += extraListMetaSize
	}
	buf := make([]byte, size)
	buf[0] = md.dataType
	index := 1
	index += binary.PutVarint(buf[index:], md.expire)
	index += binary.PutVarint(buf[index:], md.version)
	index += binary.PutVarint(buf[index:], int64(md.size))

	if md.dataType == List {
		index += binary.PutUvarint(buf[index:], md.head)
		index += binary.PutUvarint(buf[index:], md.tail)
	}
	return buf[:index]
}

// 对元数据解码
func decodeMetaData(buf []byte) *metadata {
	dataType := buf[0]
	index := 1
	expire, n := binary.Varint(buf[index:])
	index += n
	version, n := binary.Varint(buf[index:])
	index += n
	size, n := binary.Varint(buf[index:])
	index += n
	var tail, head uint64 = 0, 0
	if dataType == List {
		head, n = binary.Uvarint(buf[index:])
		index += n
		tail, _ = binary.Uvarint(buf[index:])
	}
	return &metadata{
		dataType: dataType,
		expire:   expire,
		version:  version,
		size:     uint32(size),
		tail:     tail,
		head:     head,
	}
}

type hashInternalKey struct {
	key     []byte
	version int64
	field   []byte
}

// 编码hash查找field的key=<key+version+field>
func (hk *hashInternalKey) encode() []byte {
	buf := make([]byte, len(hk.key)+len(hk.field)+8) // version是固定长度
	var index = 0
	// 放key
	copy(buf[index:index+len(hk.key)], hk.key[:])
	index += len(hk.key)
	// 放version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(hk.version))
	index += 8
	copy(buf[index:index+len(hk.field)], hk.field[:])
	index += len(hk.field)
	return buf
}

type setInternalKey struct {
	key     []byte
	version int64
	member  []byte
}

func (sk *setInternalKey) encode() []byte {
	buf := make([]byte, len(sk.key)+len(sk.member)+8+4) // 还要额外四个字节存储member的长度
	var index = 0
	// key
	copy(buf[index:index+len(sk.key)], sk.key)
	index += len(sk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(sk.version))
	index += 8

	//member
	copy(buf[index:index+len(sk.member)], sk.member)
	index += len(sk.member)

	// member size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(sk.member)))
	return buf
}

type listInternalKey struct {
	key     []byte
	version int64
	index   uint64
}

func (lk *listInternalKey) encode() []byte {
	buf := make([]byte, len(lk.key)+8+8)
	var index = 0

	// key
	copy(buf[index:index+len(lk.key)], lk.key)
	index += len(lk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(lk.version))
	index += 8

	// index
	binary.LittleEndian.PutUint64(buf[index:], lk.index)

	return buf
}

type zSetInternalKey struct {
	key     []byte
	version int64
	member  []byte
	score   float64
}

func (zk *zSetInternalKey) encodeWithMember() []byte {
	// key+version+member
	buf := make([]byte, len(zk.key)+8+len(zk.member))

	// key
	index := 0
	copy(buf[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zk.version))
	index += 8

	// member
	copy(buf[index:index+len(zk.member)], zk.member)
	return buf
}

func (zk *zSetInternalKey) encodeWithScore() []byte {
	scoreBuf := utils.Float64ToBytes(zk.score)
	// key+version+score+member+member_size
	// 用于根据score的分数范围，查找对应的member
	buf := make([]byte, len(zk.key)+8+len(scoreBuf)+len(zk.member)+4)

	// key
	index := 0
	copy(buf[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zk.version))
	index += 8

	// score
	copy(buf[index:index+len(scoreBuf)], scoreBuf)
	index += len(scoreBuf)

	// member
	copy(buf[index:index+len(zk.member)], zk.member)
	index += len(zk.member)

	// member_size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(zk.member)))
	return buf
}
