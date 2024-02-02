package fio

import "golang.org/x/exp/mmap"

// MMap IO,内存文件映射
type MMap struct {
	readerAt *mmap.ReaderAt
}

// NewMMapIOManage 刚开始打开数据库时，可以使用mmap读入，加入文件启动速度
// 因为无需把内核缓冲区的数据拷贝到用户缓冲区，直接操作内核缓冲区即可，提高访问速度
func NewMMapIOManage(fileName string) (*MMap, error) {
	readerAt, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MMap{readerAt: readerAt}, nil
}

func (mmap *MMap) Read(b []byte, n int64) (int, error) {
	return mmap.readerAt.ReadAt(b, n)
}

// Write 写入字节数组到文件中
func (mmap *MMap) Write([]byte) (int, error) {
	panic("not implemented")
}

// Sync 将内存缓冲区数据持久化到磁盘中
func (mmap *MMap) Sync() error {
	panic("not implemented")
}

// Close 关闭文件
func (mmap *MMap) Close() error {
	return mmap.readerAt.Close()
}

// Size 获取到对应文件的大小
func (mmap *MMap) Size() (int64, error) {
	return int64(mmap.readerAt.Len()), nil
}
