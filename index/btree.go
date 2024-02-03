package index

import (
	"bytes"
	"github.com/google/btree"
	"sort"
	"sync"
	"tiny-kvDB/data"
)

type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}
func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	olderIt := bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	if olderIt == nil {
		return nil
	}
	return olderIt.(*Item).pos
}
func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	bt.lock.Lock()
	btreeItem := bt.tree.Get(it)
	bt.lock.Unlock()
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}
func (bt *BTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	it := &Item{key: key}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	oldItem := bt.tree.Delete(it)
	if oldItem == nil {
		return nil, false
	}
	return oldItem.(*Item).pos, true
}

func (bt *BTree) Size() int {
	return bt.tree.Len()
}
func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	return newBTreeIterator(bt.tree, reverse)
}

func (bt *BTree) Close() error {
	return nil
}

// Btree索引迭代器
type btreeIterator struct {
	curIndex int     // 当前遍历的下标位置
	reverse  bool    // 是否是反向遍历
	values   []*Item // 对应key的索引值
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	// 将数组放到数组中
	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}
	// 逆序放置
	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}
	return &btreeIterator{
		curIndex: 0,
		reverse:  reverse,
		values:   values,
	}
}

func (bti *btreeIterator) Rewind() {
	bti.curIndex = 0
}
func (bti *btreeIterator) Seek(key []byte) {
	if bti.reverse {
		bti.curIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else {
		bti.curIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}
}
func (bti *btreeIterator) Next() {
	bti.curIndex++
}
func (bti *btreeIterator) Valid() bool {
	return bti.curIndex < len(bti.values)
}
func (bti *btreeIterator) Key() []byte {
	return bti.values[bti.curIndex].key
}
func (bti *btreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.curIndex].pos
}
func (bti *btreeIterator) Close() {
	bti.values = nil
}
