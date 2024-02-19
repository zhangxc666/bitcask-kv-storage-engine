package main

import (
	"errors"
	"fmt"
	"github.com/tidwall/redcon"
	"strings"
	tiny_kvDB "tiny-kvDB"
	tiny_kvDB_redis "tiny-kvDB/redis"
	"tiny-kvDB/utils"
)

type cmdHandler func(client *BitcaskClient, args [][]byte) (any, error)

var supportedCommand = map[string]cmdHandler{
	"set":    set,
	"get":    get,
	"sadd":   sadd,
	"hset":   hset,
	"lpush":  lpush,
	"zadd":   zadd,
	"config": config,
}

type BitcaskClient struct {
	// 用户连接数据的实例
	db *tiny_kvDB_redis.RedisDataStructure
	// 连接的server
	server *BitcaskServer
}

func newWrongNumberOfArgsError(cmd string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command.\n", cmd)
}

func execClientCommand(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(string(cmd.Args[0]))
	cmdFunc, ok := supportedCommand[command]
	if !ok {
		conn.WriteError("Err unsupported command : '" + command + "'")
		return
	}
	//
	client, _ := conn.Context().(*BitcaskClient)
	switch command {
	case "quit":
		_ = conn.Close()
	case "ping":
		conn.WriteString("PONG")
	default:
		res, err := cmdFunc(client, cmd.Args[1:])
		if err != nil {
			if errors.Is(err, tiny_kvDB.ErrKeyNotFound) {
				conn.WriteNull()
			} else {
				conn.WriteError(err.Error())
			}
			return
		}
		conn.WriteAny(res)
	}
}

func set(client *BitcaskClient, args [][]byte) (any, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("set")
	}
	key, value := args[0], args[1]
	if err := client.db.Set(key, value, 0); err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

func get(client *BitcaskClient, args [][]byte) (any, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("get")
	}
	key := args[0]
	var value []byte
	var err error
	if value, err = client.db.Get(key); err != nil {
		return nil, err
	}
	return value, nil
}

func hset(client *BitcaskClient, args [][]byte) (any, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("hset")
	}
	var ok = 0
	key, field, value := args[0], args[1], args[2]
	res, err := client.db.HSet(key, field, value)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func sadd(client *BitcaskClient, args [][]byte) (any, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("sadd")
	}
	var ok = 0
	key, member := args[0], args[1]
	res, err := client.db.SAdd(key, member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func lpush(client *BitcaskClient, args [][]byte) (any, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("sadd")
	}
	key, value := args[0], args[1]
	res, err := client.db.LPush(key, value)
	if err != nil {
		return nil, err
	}
	return redcon.SimpleInt(res), nil
}

func zadd(client *BitcaskClient, args [][]byte) (any, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("zadd")
	}
	var ok = 0
	key, score, member := args[0], args[1], args[2]
	res, err := client.db.ZAdd(key, utils.FloatFromBytes(score), member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func config(client *BitcaskClient, args [][]byte) (any, error) {
	// 返回一个空数组响应
	return redcon.Array, nil
}
