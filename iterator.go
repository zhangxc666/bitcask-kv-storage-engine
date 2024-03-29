package tiny_kvDB

import (
	"bytes"
	"tiny-kvDB/index"
)

type Iterator struct {
	indexIter index.Iterator // 索引迭代器
	db        *DB
	options   IteratorOptions
}

func (db *DB) NewIterator(options IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(options.Reverse)
	return &Iterator{
		db:        db,
		indexIter: indexIter,
		options:   options,
	}
}
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}
func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}
func (it *Iterator) Value() ([]byte, error) { // 拿到对应的value
	logRecordPos := it.indexIter.Value()
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	return it.db.getValueByPosition(logRecordPos)
}
func (it *Iterator) Close() {
	it.indexIter.Close()
}

// 跳过所有不满足options中prefix的key
func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.Prefix)
	if prefixLen == 0 {
		return
	}
	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		if prefixLen <= len(key) && bytes.Compare(it.options.Prefix, key[:prefixLen]) == 0 {
			break
		}
	}
}
