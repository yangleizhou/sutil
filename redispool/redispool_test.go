// Copyright 2014 The sutil Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package redispool

import (
	"testing"

	"github.com/fzzy/radix/redis"
	"github.com/shawnfeng/sutil/slog"
)

func TestLuaLoad(t *testing.T) {
	pool := NewRedisPool()

	err := pool.LoadLuaFile("Test", "./test.luad")
	slog.Infoln(err)
	if err == nil || err.Error() != "open ./test.luad: no such file or directory" {
		t.Errorf("error here")
	}

	err = pool.LoadLuaFile("Test", "./test.lua")
	if err != nil {
		t.Errorf("error here")
	}

	addr := "localhost:9600"
	args := []interface{}{
		2,
		"key1",
		"key2",
		"argv1",
		"argv2",
	}

	rp := pool.EvalSingle(addr, "Nothave", args)

	slog.Infoln(rp)

	if "get lua sha1 add:localhost:9600 key:Nothave err:lua not find" != rp.String() {
		t.Errorf("error here")
	}

	rp = pool.EvalSingle(addr, "Test", args)

	slog.Infoln(rp)
	if rp.Type == redis.ErrorReply {
		t.Errorf("error here")
	}

	if rp.String() != "key1key2argv1argv222" {
		t.Errorf("error here")
	}

}

func TestPool(t *testing.T) {
	pool := NewRedisPool()

	addr := "localhost:6379"
	key := "key-test-pool"
	args := []interface{}{
		"del",
		key,
	}
	rp := pool.CmdSingle(addr, args)
	t.Logf("rp: %v", rp)
	if rp.Type == redis.ErrorReply {
		t.Fatalf("rp: %v", rp)
	}

	value := "value"
	args = []interface{}{
		"set",
		key,
		value,
	}

	rp = pool.CmdSingle(addr, args)
	t.Logf("rp: %v", rp)
	if rp.Type == redis.ErrorReply {
		t.Fatalf("rp: %v", rp)
	}

	args = []interface{}{
		"get",
		key,
	}

	rp = pool.CmdSingle(addr, args)
	t.Logf("rp: %v", rp)
	if rp.Type == redis.ErrorReply {
		t.Fatalf("rp: %v", rp)
	}
}
