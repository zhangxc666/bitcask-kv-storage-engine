package test

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"tiny-kvDB/data"
	"tiny-kvDB/index"
)

func TestBTree_Delete(t *testing.T) {
	bt := index.NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)
	res2 := bt.Delete(nil)
	assert.True(t, res2)

	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 2, Offset: 100})
	assert.True(t, res3)
	res4 := bt.Delete([]byte("a"))
	assert.True(t, res4)
}

func TestBTree_Get(t *testing.T) {
	bt := index.NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)
	item1 := bt.Get(nil)
	assert.Equal(t, uint32(1), item1.Fid)
	assert.Equal(t, int64(100), item1.Offset)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)
	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.True(t, res3)

	item2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), item2.Fid)
	assert.Equal(t, int64(3), item2.Offset)

}

func TestBTree_Put(t *testing.T) {
	bt := index.NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)
	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)
}

func TestBtree_Iterator(t *testing.T) {
	bt1 := index.NewBTree()
	// bt1为空的情况
	iter1 := bt1.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	// Btree有数据的情况
	bt1.Put([]byte("code"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter2 := bt1.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	// 仅有一条数据，下一条为false
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())
	// 测试多条数据
	bt1.Put([]byte("aaa"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("bbb"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("ccc"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter3 := bt1.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}
	iter4 := bt1.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		assert.NotNil(t, iter4.Key())
	}

	// 测试seek
	iter5 := bt1.Iterator(false)
	for iter5.Seek([]byte("bb")); iter5.Valid(); iter5.Next() {
		assert.NotNil(t, iter5.Key())
	}

	// 反向遍历的情况
	iter6 := bt1.Iterator(true)
	for iter6.Seek([]byte("bb")); iter6.Valid(); iter6.Next() {
		assert.NotNil(t, iter6.Key())
	}
}
