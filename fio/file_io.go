package fio

import "os"

type FileIO struct {
	fd *os.File // 系统文件描述符
}

func NewFileIOManager(fileName string) (*FileIO, error) {
	// o_create 文件不存在创建,o_append 支持追加写
	// 0644打开权限
	fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, DataFilePerm)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: fd}, nil
}

// Read 从文件的给定位置读取对应的数据
func (fio *FileIO) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

// Write 写入字节数组到文件中
func (fio *FileIO) Write(b []byte) (int, error) {
	return fio.fd.Write(b)
}

// Sync 将内存缓冲区数据持久化到磁盘中
func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

// Close 关闭文件
func (fio *FileIO) Close() error {
	return fio.fd.Close()
}

func (fio *FileIO) Size() (int64, error) {
	stat, err := fio.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
