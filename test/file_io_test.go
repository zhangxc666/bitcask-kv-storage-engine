package test

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
	fio2 "tiny-kvDB/fio"
)

func deleteTempFile(fileName string) {
	if err := os.RemoveAll(fileName); err != nil {
		panic(err)
	}

}

func TestFileIO_Close(t *testing.T) {
	fileName := filepath.Join("/tmp", "0002.data")
	fio, err := fio2.NewFileIOManager(fileName)
	defer deleteTempFile(fileName)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Equal(t, nil, err)
}

func TestFileIO_Read(t *testing.T) {
	fileName := filepath.Join("/tmp", "0001.data")
	fio, err := fio2.NewFileIOManager(filepath.Join("/tmp", "0001.data"))
	defer deleteTempFile(fileName)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("key-a"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte("key-b"))
	assert.Nil(t, err)

	b1 := make([]byte, 5)
	n, err := fio.Read(b1, 0)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-a"), b1)

	b2 := make([]byte, 5)
	n, err = fio.Read(b2, 5)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-b"), b2)
	t.Log("b1: ", string(b1), " b2: ", string(b2))
}

func TestFileIO_Sync(t *testing.T) {
	fileName := filepath.Join("/tmp", "0002.data")
	fio, err := fio2.NewFileIOManager(fileName)
	defer deleteTempFile(fileName)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Equal(t, nil, err)

}

func TestFileIO_Write(t *testing.T) {
	fileName := filepath.Join("/tmp", "0002.data")
	fio, err := fio2.NewFileIOManager(fileName)
	defer deleteTempFile(fileName)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("bitcask kv"))
	assert.Equal(t, 10, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("zxc666"))
	assert.Equal(t, 6, n)
	assert.Nil(t, err)
}

func TestNewFileIOManager(t *testing.T) {
	fileName := filepath.Join("/tmp", "0002.data")
	fio, err := fio2.NewFileIOManager(fileName)
	defer deleteTempFile(fileName)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
}
