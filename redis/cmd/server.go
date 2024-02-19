package main

import (
	"github.com/tidwall/redcon"
	"log"
	"sync"
	tiny_kvDB "tiny-kvDB"
	tiny_kvDB_redis "tiny-kvDB/redis"
)

const addr = "localhost:6380"

type BitcaskServer struct {
	// 可以连接多个db
	dbs    map[int]*tiny_kvDB_redis.RedisDataStructure
	server *redcon.Server
	mu     sync.RWMutex
}

func main() {
	// 打开redis数据结构的服务
	rds, err := tiny_kvDB_redis.NewRedisDataStructure(tiny_kvDB.DefaultOptions)
	if err != nil {
		panic(err)
	}

	// 初始化Bitcaskserver
	bitcaskServer := &BitcaskServer{
		dbs: make(map[int]*tiny_kvDB_redis.RedisDataStructure),
	}
	bitcaskServer.dbs[0] = rds

	// 初始化一个Redis服务端
	bitcaskServer.server = redcon.NewServer(addr, execClientCommand, bitcaskServer.accept, bitcaskServer.close)
	bitcaskServer.listen()
}

func (svr *BitcaskServer) listen() {
	log.Println("bitcask server running, ready to accept connections.")
	_ = svr.server.ListenAndServe()
}

func (svr *BitcaskServer) accept(conn redcon.Conn) bool {
	client := new(BitcaskClient)
	svr.mu.Lock()
	defer svr.mu.Unlock()
	client.server = svr
	client.db = svr.dbs[0]
	// 通过context传递client
	conn.SetContext(client)
	return true
}

func (svr *BitcaskServer) close(conn redcon.Conn, err error) {
	for _, db := range svr.dbs {
		_ = db.Close()
	}
	_ = svr.server.Close()
	_ = conn.Close()
}
