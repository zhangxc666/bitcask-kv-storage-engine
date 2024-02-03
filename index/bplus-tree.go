package index

import (
	"go.etcd.io/bbolt"
	"path/filepath"
	"tiny-kvDB/data"
)

const bptreeIndexFileName = "bptree-index"

var indexBucketName = []byte("bitcask-index")

// BPlusTree B+树索引，将索引存到磁盘上
type BPlusTree struct {
	tree *bbolt.DB
}

func NewBPlusTree(dirPath string, syncWrite bool) *BPlusTree {
	opts := bbolt.DefaultOptions
	opts.NoSync = !syncWrite
	bptree, err := bbolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0644, opts)
	if err != nil {
		panic("failed to open bptree")
	}

	// 创建对应的bucket
	if err := bptree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("failed to open bptree")
	}

	return &BPlusTree{
		tree: bptree,
	}
}

func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	var olderIt []byte
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		olderIt = bucket.Get(key)
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	}); err != nil {
		panic("failed to put value in bptree")
	}
	if len(olderIt) == 0 {
		return nil
	}
	return data.DecodeLogRecordPos(olderIt)
}

// Get 根据key存储索引位置信息
func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			pos = data.DecodeLogRecordPos(value)
		}
		return nil
	}); err != nil {
		panic("failed to get value in bptree")
	}
	return pos
}

// Delete 根据key删除对应的索引位置信息
func (bpt *BPlusTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	var (
		olderIt []byte
	)
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)

		if olderIt = bucket.Get(key); len(olderIt) != 0 {
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to delete value in bptree")
	}
	if len(olderIt) == 0 {
		return nil, false
	}
	return data.DecodeLogRecordPos(olderIt), true
}

func (bpt *BPlusTree) Size() int {
	var size int
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to get value in bptree")
	}
	return size
}

func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}

func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBptreeIterator(bpt.tree, reverse)
}

// B+树迭代器
type bptreeIterator struct {
	tx       *bbolt.Tx
	cursor   *bbolt.Cursor
	reverse  bool
	curKey   []byte
	curValue []byte
}

func newBptreeIterator(tree *bbolt.DB, reverse bool) *bptreeIterator {
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin a transaction")
	}
	bpi := &bptreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}
	bpi.Rewind()
	return bpi
}

func (bpi *bptreeIterator) Rewind() {
	if bpi.reverse {
		bpi.curKey, bpi.curValue = bpi.cursor.Last()
	} else {
		bpi.curKey, bpi.curValue = bpi.cursor.First()
	}
}
func (bpi *bptreeIterator) Seek(key []byte) {
	bpi.curKey, bpi.curValue = bpi.cursor.Seek(key)
}
func (bpi *bptreeIterator) Next() {
	if bpi.reverse {
		bpi.curKey, bpi.curValue = bpi.cursor.Next()
	} else {
		bpi.curKey, bpi.curValue = bpi.cursor.Prev()
	}
}
func (bpi *bptreeIterator) Valid() bool {
	return len(bpi.curKey) != 0
}
func (bpi *bptreeIterator) Key() []byte {
	return bpi.curKey
}
func (bpi *bptreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(bpi.curValue)
}
func (bpi *bptreeIterator) Close() {
	_ = bpi.tx.Rollback()
}
