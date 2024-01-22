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
