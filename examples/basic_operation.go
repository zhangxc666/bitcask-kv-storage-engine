package main

import (
	"fmt"
	tiny_kvDB "tiny-kvDB"
)

func main() {
	opts := tiny_kvDB.DefaultOptions
	opts.DirPath = "/tmp/bitcask-go"
	db, err := tiny_kvDB.Open(opts)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("name"), []byte("bitcask"))
	if err != nil {
		panic(err)
	}

	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}

	fmt.Println(string(val))

	db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}

	val, err = db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}

	fmt.Println(string(val))
}
