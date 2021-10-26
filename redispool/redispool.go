// Copyright 2014 The sutil Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// modifyed by jiajia --2017.4
// 实现方式改了，改成更nb的方式了，更符合go语言的share memory by comunication思想，没有了阻塞锁，通过令牌桶的方式限容量，随用随建连接

package redispool

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"sync/atomic"

	"sync"
	"time"

	"github.com/fzzy/radix/redis"

	"strings"

	"github.com/shawnfeng/sutil/slog"
)

const (
	TIMEOUT_INTV = time.Second * 200
)

type RedisEntry struct {
	client *redis.Client
	addr   string
	stamp  time.Time
}

func (self *RedisEntry) String() string {
	return fmt.Sprintf("%p@%s@%v", self.client, self.addr, self.stamp)

}

func (self *RedisEntry) Cmd(args []interface{}) *redis.Reply {
	value := args[0].(string)

	return self.client.Cmd(value, args[1:]...)

}

func (self *RedisEntry) close() {
	fun := "RedisEntry.close"
	slog.Infof("%s re:%s", fun, self)

	err := self.client.Close()
	if err != nil {
		slog.Infof("%s err re:%s err:%s", fun, self, err)
	}
	self.client = nil // 指示器，当为nil，tokenpool可以回收一个占位
}

type luaScript struct {
	sha1 string
	data []byte
}

type atomicvar struct {
	val atomic.Value
	mu  sync.Mutex
}

func (av *atomicvar) read(addr string) int {
	v := av.val.Load()
	if v != nil {
		m := v.(map[string]int)
		if i, ok := m[addr]; ok {
			return i
		}
	}
	return av.insert(addr)
}

func (av *atomicvar) insert(addr string) int {
	av.mu.Lock()
	defer av.mu.Unlock()

	var mold map[string]int
	v := av.val.Load()
	if v != nil {
		mold = v.(map[string]int)
	}
	if i, ok := mold[addr]; ok {
		return i
	}

	mnew := map[string]int{}
	for k, v := range mold {
		mnew[k] = v
	}
	i := len(mnew)
	mnew[addr] = i
	av.val.Store(mnew)

	return i
}

type RedisPool struct {
	clipool   []chan *RedisEntry
	tokenpool []chan struct{} // 令牌桶，控制容量
	capconn   int             // 最大连接数
	capidle   int             // 最大空闲连接数
	capaddr   int             // 最大addr数
	av        *atomicvar      // 记录addr和index映射关系

	muLua sync.Mutex
	luas  map[string]*luaScript
}

func (self *RedisPool) getindex(addr string) (int, error) {
	i := self.av.read(addr)
	if i >= self.capaddr {
		return -1, fmt.Errorf("redispool.RedisPool.getindex exceed! addr: %s, i: %d", addr, i)
	}
	return i, nil
}

func (self *RedisPool) filteraddr(origaddr string) (addr, pwd string) {
	addr = origaddr
	if pos := strings.Index(addr, "/"); pos != -1 {
		pwd = addr[pos+1:]
		addr = addr[:pos]
	}
	return
}

func (self *RedisPool) add(addr string) (*RedisEntry, error) {
	fun := "RedisPool.add"
	slog.Tracef("%s addr:%s", fun, addr)

	i, err := self.getindex(addr)
	if err != nil {
		return nil, err
	}

	select {
	case <-self.tokenpool[i]:
	default:
		return nil, fmt.Errorf("%s exceed! addr: %s, i: %d", fun, addr, i)
	}

	addr, pwd := self.filteraddr(addr)
	c, err := redis.DialTimeout("tcp", addr, time.Second*3)
	if err != nil {
		self.tokenpool[i] <- struct{}{}
		return nil, err
	}
	if pwd != "" {
		rp := c.Cmd("auth", pwd)
		if rp.Type == redis.ErrorReply {
			c.Close()
			return nil, fmt.Errorf("%s auth fail! addr: %s, pwd: %s", fun, addr, pwd)
		}
	}

	en := &RedisEntry{
		client: c,
		addr:   addr,
		stamp:  time.Now(),
	}

	return en, nil
}

func (self *RedisPool) getCache(addr string) (re *RedisEntry) {
	fun := "RedisPool.getCache"
	//slog.Traceln(fun, "call", addr, self)

	i, err := self.getindex(addr)
	if err != nil {
		return nil
	}

	select {
	case re = <-self.clipool[i]:
	default:
	}

	slog.Tracef("%s addr:%s re:%s", fun, addr, re)

	//slog.Traceln(fun, "call", addr, self)
	return
}

func (self *RedisPool) payback(addr string, re *RedisEntry) {
	fun := "RedisPool.payback"
	//slog.Traceln(fun, "call", addr, self)

	i, err := self.getindex(addr)
	if err != nil {
		return
	}

	if re.client != nil {
		re.stamp = time.Now()
		self.clipool[i] <- re
	} else {
		self.tokenpool[i] <- struct{}{}
	}

	slog.Tracef("%s addr:%s re:%s len:%d", fun, addr, re, len(self.clipool[i]))

	//slog.Traceln(fun, "end", addr, self)

}

func (self *RedisPool) get(addr string) (*RedisEntry, error) {
	if r := self.getCache(addr); r != nil {
		return r, nil
	} else {
		return self.add(addr)
	}
}

// 收割逻辑，淘汰掉长期没用到的client
func (self *RedisPool) reap() {
	for addrpos, clich := range self.clipool {
		go func(addrpos int, clich chan *RedisEntry) {
			ticker1 := time.NewTicker(TIMEOUT_INTV / 4)
			ticker2 := time.NewTicker(time.Second)
			for {
				select {
				case <-ticker1.C:
					loopnum := len(clich)
					if loopnum == 0 {
						break
					}
					for i := loopnum; i > 0; i-- {
						select {
						case client := <-clich:
							if time.Since(client.stamp) < TIMEOUT_INTV {
								clich <- client
							} else {
								client.close()
								self.tokenpool[addrpos] <- struct{}{}
							}
						default:
							i = 0
						}
					}
				case <-ticker2.C:
					if self.capidle == 0 {
						break
					}
					loopnum := len(clich) - self.capidle
					if loopnum <= 0 {
						break
					}
					for i := loopnum; i > 0; i-- {
						select {
						case client := <-clich:
							client.close()
							self.tokenpool[addrpos] <- struct{}{}
						default:
							i = 0
						}
					}
				}
			}
		}(addrpos, clich)
	}
}

// 只对一个redis执行命令
func (self *RedisPool) CmdSingleRetry(addr string, cmd []interface{}, retrytimes int) *redis.Reply {
	fun := "RedisPool.CmdSingleRetry"
	c, err := self.get(addr)
	if err != nil {
		es := fmt.Sprintf("get conn retrytimes:%d addr:%s err:%s", retrytimes, addr, err)
		slog.Infoln(fun, es)
		return &redis.Reply{Type: redis.ErrorReply, Err: errors.New(es)}
	}
	defer self.payback(addr, c)

	rp := c.Cmd(cmd)
	if rp.Type == redis.ErrorReply {
		slog.Errorf("%s redis Cmd try:%d err:%s", fun, retrytimes, rp)
		c.close()

		// TODO
		// 这个判断应该有问题，timeout也不是返回"EOF"
		if rp.String() == "EOF" {
			if retrytimes > 0 {
				return rp
			}
			// redis 连接timeout，重试一次
			return self.CmdSingleRetry(addr, cmd, retrytimes+1)
		}
	}

	return rp

}

func (self *RedisPool) CmdSingle(addr string, cmd []interface{}) *redis.Reply {
	return self.CmdSingleRetry(addr, cmd, 0)

}

func (self *RedisPool) sha1Lua(key string) (string, error) {
	self.muLua.Lock()
	defer self.muLua.Unlock()
	if v, ok := self.luas[key]; ok {
		return v.sha1, nil
	} else {
		return "", errors.New("lua not find")
	}

}

func (self *RedisPool) dataLua(key string) ([]byte, error) {
	self.muLua.Lock()
	defer self.muLua.Unlock()

	if v, ok := self.luas[key]; ok {
		return v.data, nil
	} else {
		return []byte{}, errors.New("lua not find")
	}

}

func (self *RedisPool) LoadLuaFile(key, file string) error {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}

	h := sha1.Sum(data)
	hex := fmt.Sprintf("%x", h)

	slog.Infof("RedisPool.loadLuaFile key:%s sha1:%s file:%s", key, hex, file)

	self.muLua.Lock()
	defer self.muLua.Unlock()

	self.luas[key] = &luaScript{
		sha1: hex,
		data: data,
	}

	return nil

}

// lua 脚本执行的快捷命令
func (self *RedisPool) EvalSingle(addr string, key string, cmd_args []interface{}) *redis.Reply {
	fun := "RedisPool.EvalSingle"
	sha1, err := self.sha1Lua(key)
	if err != nil {
		es := fmt.Sprintf("get lua sha1 add:%s key:%s err:%s", addr, key, err)
		return &redis.Reply{Type: redis.ErrorReply, Err: errors.New(es)}
	}

	cmd := append([]interface{}{"evalsha", sha1}, cmd_args...)
	rp := self.CmdSingle(addr, cmd)
	if rp.Type == redis.ErrorReply && rp.String() == "NOSCRIPT No matching script. Please use EVAL." {
		slog.Infoln(fun, "load lua", addr)
		cmd[0] = "eval"
		cmd[1], _ = self.dataLua(key)
		rp = self.CmdSingle(addr, cmd)
	}

	return rp
}

func (self *RedisPool) Cmd(multi_args map[string][]interface{}) map[string]*redis.Reply {
	rv := make(map[string]*redis.Reply)
	for k, v := range multi_args {
		rv[k] = self.CmdSingle(k, v)
	}

	return rv

}

func HashRedis(addrs []string, key string) string {
	h := fnv.New32a()
	h.Write([]byte(key))
	hv := h.Sum32()

	return addrs[hv%uint32(len(addrs))]

}

func NewRedisPool() *RedisPool {
	return NewRedisPoolWithCap(30000, 200, 5)
}

func NewRedisPoolWithCap(capconn, capidle, capaddr int) *RedisPool {
	if capconn < 0 || capconn > 0xFFFF {
		panic("redispool.NewRedisPoolWithCap() need a valid capconn")
	}
	if capconn == 0 {
		capconn = 0xFFFF
	}
	if capidle < 0 || capidle > 0xFFFF {
		panic("redispool.NewRedisPoolWithCap() need a valid capidle")
	}
	if capidle >= capconn {
		capidle = 0
	}
	if capaddr < 1 || capaddr > 100 {
		panic("redispool.NewRedisPoolWithCap() need a valid capaddr")
	}

	clipool := make([]chan *RedisEntry, capaddr)
	for i := range clipool {
		clipool[i] = make(chan *RedisEntry, capconn)
	}
	tokenpool := make([]chan struct{}, capaddr)
	for i := range tokenpool {
		tokench := make(chan struct{}, capconn)
		for j := 0; j < capconn; j++ {
			tokench <- struct{}{}
		}
		tokenpool[i] = tokench
	}

	rp := &RedisPool{
		clipool:   clipool,
		tokenpool: tokenpool,
		capconn:   capconn, // NOTICE: /proc/sys/net/ipv4/ip_local_port_range
		capidle:   capidle,
		capaddr:   capaddr,
		av:        &atomicvar{},
		luas:      make(map[string]*luaScript),
	}
	rp.reap() // 启动清理协程
	return rp
}

//////////
//TODO
// OK 1. timeout remove
// 2. multi addr channel get
// 3. single addr multi cmd
// 4. pool conn ceil controll
