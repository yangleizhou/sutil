// Copyright 2014 The sutil Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sutil

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"code.google.com/p/go-uuid/uuid"
)

func HashV(addrs []string, key string) string {
	if len(addrs) == 0 {
		return ""
	}
	h := fnv.New32a()
	h.Write([]byte(key))
	hv := h.Sum32()

	return addrs[hv%uint32(len(addrs))]

}

func IsJSON(s []byte) bool {
	//var js map[string]interface{}
	var js interface{}
	return json.Unmarshal(s, &js) == nil
}

func JsonBigInt64Decode(s []byte, v interface{}) (err error) {
	d := json.NewDecoder(bytes.NewReader(s))
	d.UseNumber()
	return d.Decode(&v)
}

func ComputeFileMd5(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	var result []byte
	h := hash.Sum(result)

	return fmt.Sprintf("%x", h), nil
}

// 截取获取合法 num 个unicode字符 的utf8字符串
// num 为0，全部截取
// 返回截取的unicode字符个数，以及字符串
func GetInvalidUtf8String(s string, num int) (string, int) {
	rv := ""
	count := 0
	for i := 0; len(s) > 0; i++ {
		ru, size := utf8.DecodeRuneInString(s)
		if ru != utf8.RuneError {
			rv += s[:size]
			count++

			if num > 0 && count >= num {
				break
			}
		}
		s = s[size:]
	}

	return rv, count
}

func GetUtf8Chars(s string, num int) string {
	rv := ""
	for i := 0; len(s) > 0 && i < num; i++ {
		_, size := utf8.DecodeRuneInString(s)
		rv += s[:size]
		s = s[size:]
	}

	return rv
}

func GetUtf8Chars_old(s string, num int) string {
	b := []byte(s)
	rv := ""
	for i := 0; len(b) > 0 && i < num; i++ {
		_, size := utf8.DecodeRune(b)
		rv += string(b[:size])
		b = b[size:]
	}

	return rv
}

var uuidMu sync.Mutex

func GetUUID() string {
	uuidMu.Lock()
	defer uuidMu.Unlock()

	uuidgen := uuid.NewUUID()
	return uuidgen.String()
}

func GetUniqueMd5() string {
	u := GetUUID()
	h := md5.Sum([]byte(u))
	return fmt.Sprintf("%x", h)
}

// 文件输出，目录不存在自动创建
func WriteFile(path string, data []byte, perm os.FileMode) error {

	idx := strings.LastIndex(path, "/")
	if idx != -1 {
		logdir := path[:idx]
		err := os.MkdirAll(logdir, 0777)
		if err != nil {
			return err
		}
	}

	return ioutil.WriteFile(path, data, perm)
}

// 四舍五入
func Round(val float64, deci int) float64 {
	format := fmt.Sprintf("%%0.%df", deci)
	sval := fmt.Sprintf(format, val)

	nv, _ := strconv.ParseFloat(sval, 64)

	return nv
}

type VersionCmp struct {
	ver string
}

func NewVersionCmp(ver string) *VersionCmp {
	v := &VersionCmp{}

	v.ver = v.fmtver(ver)
	return v
}

func (m *VersionCmp) fmtver(ver string) string {
	var buf bytes.Buffer
	p := 0
	for i, c := range ver {
		if c != '.' {
			continue
		}

		count := 20 - i + p
		if count > 0 {
			buf.WriteString(strings.Repeat("0", count))
		}

		buf.WriteString(ver[p:i])
		p = i + 1
	}

	i := len(ver)
	if p <= i {
		count := 20 - i + p
		if count > 0 {
			buf.WriteString(strings.Repeat("0", count))
		}

		buf.WriteString(ver[p:i])
	}

	return buf.String()
}

func (m *VersionCmp) Min() string {
	return m.fmtver("0")
}

func (m *VersionCmp) Max() string {
	return m.fmtver("99999999999999999999")
}

func (m *VersionCmp) Lt(ver string) bool {
	return m.ver < m.fmtver(ver)
}

func (m *VersionCmp) Lte(ver string) bool {
	return m.ver <= m.fmtver(ver)
}

func (m *VersionCmp) Gt(ver string) bool {
	return m.ver > m.fmtver(ver)
}

func (m *VersionCmp) Gte(ver string) bool {
	return m.ver >= m.fmtver(ver)
}

func (m *VersionCmp) Eq(ver string) bool {
	return m.ver == m.fmtver(ver)
}

func (m *VersionCmp) Ne(ver string) bool {
	return m.ver != m.fmtver(ver)
}
