package index

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"tiny-kvDB/data"
)

func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewART()
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 12})
}

func TestAdaptiveRadixTree_Get(t *testing.T) {
	art := NewART()
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	logPos := art.Get([]byte("key-1"))
	assert.NotNil(t, logPos)

	pos1 := art.Get([]byte("not exist"))
	assert.Nil(t, pos1)

	pos2 := art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 2, Offset: 12})
	assert.NotNil(t, pos2)
}

func TestAdaptiveRadixTree_Delete(t *testing.T) {
	art := NewART()
	res1 := art.Delete([]byte("not exist"))
	assert.False(t, res1)

	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	res2 := art.Delete([]byte("key-1"))
	assert.True(t, res2)
	pos := art.Get([]byte("key-1"))
	assert.Nil(t, pos)

}

func TestAdaptiveRadixTree_Size(t *testing.T) {
	art := NewART()
	art.Put([]byte("k1"), &data.LogRecordPos{Fid: 1, Offset: 1})
	art.Put([]byte("k2"), &data.LogRecordPos{Fid: 2, Offset: 2})
	art.Put([]byte("k1"), &data.LogRecordPos{Fid: 1, Offset: 1})
	assert.Equal(t, 2, art.Size())
}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	art := NewART()
	art.Put([]byte("k1"), &data.LogRecordPos{Fid: 1, Offset: 1})
	art.Put([]byte("k2"), &data.LogRecordPos{Fid: 2, Offset: 2})
	art.Put([]byte("k3"), &data.LogRecordPos{Fid: 1, Offset: 1})
	art.Put([]byte("k4"), &data.LogRecordPos{Fid: 2, Offset: 2})
	iter := art.Iterator(false)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
	}
	iter.Close()
}
