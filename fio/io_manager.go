package fio

const DataFilePerm = 0644

type IOManager interface {
	// Read 从文件的给定位置读取对应的数据
	Read([]byte, int64) (int, error)
	// Write 写入字节数组到文件中
	Write([]byte) (int, error)
	// Sync 将内存缓冲区数据持久化到磁盘中
	Sync() error
	// Close 关闭文件
	Close() error
}
