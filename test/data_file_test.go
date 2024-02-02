package test

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"tiny-kvDB/data"
	"tiny-kvDB/fio"
)

func TestOpenDataFile(t *testing.T) {
	datafile1, err := data.OpenDataFile(os.TempDir(), 0, fio.StandardFileIO)
	assert.Nil(t, err)
	assert.NotNil(t, datafile1)

	datafile2, err := data.OpenDataFile(os.TempDir(), 1, fio.StandardFileIO)
	assert.Nil(t, err)
	assert.NotNil(t, datafile2)

	datafile3, err := data.OpenDataFile(os.TempDir(), 0, fio.StandardFileIO)
	assert.Nil(t, err)
	assert.NotNil(t, datafile3)
}

func TestDataFile_Write(t *testing.T) {
	datafile1, err := data.OpenDataFile(os.TempDir(), 0, fio.StandardFileIO)
	assert.Nil(t, err)
	assert.NotNil(t, datafile1)
	err = datafile1.Write([]byte("aaa"))
	assert.Nil(t, err)
	err = datafile1.Write([]byte("aaa"))
	assert.Nil(t, err)

	assert.Equal(t, datafile1.WriteOff, int64(6))
}

func TestDataFile_Close(t *testing.T) {
	datafile1, err := data.OpenDataFile(os.TempDir(), 0, fio.StandardFileIO)
	assert.Nil(t, err)
	assert.NotNil(t, datafile1)

	err = datafile1.Close()
	assert.Nil(t, err)
}

func TestDataFile_Sync(t *testing.T) {
	datafile1, err := data.OpenDataFile(os.TempDir(), 0, fio.StandardFileIO)
	assert.Nil(t, err)
	assert.NotNil(t, datafile1)

	err = datafile1.Sync()
	assert.Nil(t, err)
}
