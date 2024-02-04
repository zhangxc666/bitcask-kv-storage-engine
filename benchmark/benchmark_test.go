package benchmark

import (
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/rand"
	"os"
	"testing"
	"time"
	tiny_kvDB "tiny-kvDB"
	"tiny-kvDB/utils"
)

var db *tiny_kvDB.DB

func init() {
	// 初始化用户基准测试的存储引擎
	opt := tiny_kvDB.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-benchmark")
	opt.DirPath = dir

	var err error
	db, err = tiny_kvDB.Open(opt)
	if err != nil {
		panic(err)
	}
}

func Benchmark_Put(b *testing.B) {
	// 开始计时
	b.ResetTimer()
	// 打印内存分配
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(b, err)
	}
}

func Benchmark_Get(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(i))
		assert.Nil(b, err)
	}

	rand.Seed(uint64(time.Now().UnixNano()))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Get(utils.GetTestKey(i))
		if err != nil && err != tiny_kvDB.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func Benchmark_Del(b *testing.B) {
	rand.Seed(uint64(time.Now().UnixNano()))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := db.Delete(utils.GetTestKey(i))
		if err != nil && err != tiny_kvDB.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}
