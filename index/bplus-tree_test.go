package index

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
	"tiny-kvDB/data"
	"tiny-kvDB/utils"
)

func TestNew(t *testing.T) {
	path := filepath.Join("/tmp")
	tree := NewBPlusTree(path, false)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree.Put(utils.GetTestKey(1), &data.LogRecordPos{Fid: 1, Offset: 12})
	tree.Put(utils.GetTestKey(2), &data.LogRecordPos{Fid: 2, Offset: 12})
	tree.Put(utils.GetTestKey(3), &data.LogRecordPos{Fid: 3, Offset: 12})
	val := tree.Get(utils.GetTestKey(2))
	t.Log(val)

	tree.Delete(utils.GetTestKey(2))
	val = tree.Get(utils.GetTestKey(2))
	t.Log(val)
	t.Log(tree.Size())

	iter := tree.Iterator(false)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		t.Log(string(iter.Key()))
	}
	iter.Close()
}

func TestBPlussTree_Put(t *testing.T) {
	art := NewBTree()
	res1 := art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.Nil(t, res1)

	res2 := art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.Nil(t, res2)

	res3 := art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 2, Offset: 13})
	assert.NotNil(t, res3)
	assert.Equal(t, uint32(1), res3.Fid)
	assert.Equal(t, int64(12), res3.Offset)
}
