package redis

import (
	"encoding/binary"
	"math"
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
		index += binary.PutVarint(buf[index:], int64(md.head))
		index += binary.PutVarint(buf[index:], int64(md.tail))
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
	var tail, head uint64
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
