package index

import (
	"bytes"
	"github.com/google/btree"
	"tiny-kvDB/data"
)

// Indexer 抽象索引接口，后续想加入数据结构实现当前接口
type Indexer interface {
	// Put 向索引中存储key对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool
	// Get 根据key存储索引位置信息
	Get(key []byte) *data.LogRecordPos
	// Delete 根据key删除对应的索引位置信息
	Delete(key []byte) bool
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

// IndexType 相当于起了个别名
type IndexType = int8

const (
	// Btree 索引类型
	Btree IndexType = iota + 1
	// ART 自适应基数树索引
	ART
)

func NewIndexer(indexType IndexType) Indexer {
	switch indexType {
	case Btree:
		return NewBTree()
	case ART:
		// TODO
		return nil
	default:
		panic("unsupported index type")
	}
}
