package index

import (
	"bytes"
	art "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
	"tiny-kvDB/data"
)

type AdaptiveRadixTree struct {
	tree art.Tree
	lock *sync.RWMutex
}

func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: art.New(),
		lock: new(sync.RWMutex),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	art.lock.Lock()
	olderIt, _ := art.tree.Insert(key, pos)
	art.lock.Unlock()
	if olderIt == nil {
		return nil
	}
	return olderIt.(*data.LogRecordPos)
}
func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	value, ok := art.tree.Search(key)
	if !ok {
		return nil
	}
	return value.(*data.LogRecordPos)
}
func (art *AdaptiveRadixTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	art.lock.Lock()
	olderIt, del := art.tree.Delete(key)
	art.lock.Unlock()
	if olderIt == nil {
		return nil, false
	}
	return olderIt.(*data.LogRecordPos), del
}

func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	size := art.tree.Size()
	art.lock.RUnlock()
	return size
}
func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return newArtIterator(art.tree, reverse)
}

func (art *AdaptiveRadixTree) Close() error {
	return nil
}

type artIterator struct {
	curIndex int     // 当前遍历的下标位置
	reverse  bool    // 是否是反向遍历
	values   []*Item // 对应key的索引值
}

func newArtIterator(tree art.Tree, reverse bool) *artIterator {
	var idx int
	values := make([]*Item, tree.Size())
	if reverse {
		idx = tree.Size() - 1
	}
	saveValues := func(node art.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true

	}
	tree.ForEach(saveValues)
	return &artIterator{
		curIndex: 0,
		reverse:  reverse,
		values:   values,
	}
}

func (art *artIterator) Rewind() {
	art.curIndex = 0
}
func (art *artIterator) Seek(key []byte) {
	if art.reverse {
		art.curIndex = sort.Search(len(art.values), func(i int) bool {
			return bytes.Compare(art.values[i].key, key) <= 0
		})
	} else {
		art.curIndex = sort.Search(len(art.values), func(i int) bool {
			return bytes.Compare(art.values[i].key, key) >= 0
		})
	}
}
func (art *artIterator) Next() {
	art.curIndex++
}
func (art *artIterator) Valid() bool {
	return art.curIndex < len(art.values)
}
func (art *artIterator) Key() []byte {
	return art.values[art.curIndex].key
}
func (art *artIterator) Value() *data.LogRecordPos {
	return art.values[art.curIndex].pos
}
func (art *artIterator) Close() {
	art.values = nil
}
