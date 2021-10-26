// Copyright 2014 The sutil Author. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package snetutil

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"time"
)

func IpBetween(from, to, test net.IP) (bool, error) {
	if from == nil || to == nil || test == nil {
		return false, fmt.Errorf("An ip input is nil")
	}

	from16 := from.To16()
	to16 := to.To16()
	test16 := test.To16()
	if from16 == nil || to16 == nil || test16 == nil {
		return false, fmt.Errorf("An ip did not convert to a 16 byte")
	}

	if bytes.Compare(test16, from16) >= 0 && bytes.Compare(test16, to16) <= 0 {
		return true, nil
	}
	return false, nil
}

func IpBetweenStr(from, to, test string) (bool, error) {
	return IpBetween(net.ParseIP(from), net.ParseIP(to), net.ParseIP(test))
}

//10.0.0.0/8：10.0.0.0～10.255.255.255
//172.16.0.0/12：172.16.0.0～172.31.255.255
//192.168.0.0/16：192.168.0.0～192.168.255.255
func IsInterIp(ip string) (bool, error) {
	ok, err := IpBetweenStr("10.0.0.0", "10.255.255.255", ip)
	if err != nil {
		return false, err
	}

	if !ok {
		ok, err = IpBetweenStr("172.16.0.0", "172.31.255.255", ip)
		if err != nil {
			return false, err
		}

		if !ok {
			ok, err = IpBetweenStr("192.168.0.0", "192.168.255.255", ip)
			if err != nil {
				return false, err
			}
		}
	}

	return ok, nil

}

func GetInterIp() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				//fmt.Println(ipnet.IP.String())
				return ipnet.IP.String(), nil
			}
		}
	}

	/*
		for _, addr := range addrs {
			//fmt.Printf("Inter %v\n", addr)
			ip := addr.String()
			if "10." == ip[:3] {
				return strings.Split(ip, "/")[0], nil
			} else if "172." == ip[:4] {
				return strings.Split(ip, "/")[0], nil
			} else if "196." == ip[:4] {
				return strings.Split(ip, "/")[0], nil
			} else if "192." == ip[:4] {
				return strings.Split(ip, "/")[0], nil
			}

		}
	*/

	return "", errors.New("no inter ip")
}

// 获取首个外网ip v4
func GetExterIp() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	for _, addr := range addrs {
		//fmt.Printf("Inter %v\n", addr)
		ips := addr.String()
		idx := strings.LastIndex(ips, "/")
		if idx == -1 {
			continue
		}
		ipv := net.ParseIP(ips[:idx])
		if ipv == nil {
			continue
		}

		ipv4 := ipv.To4()
		if ipv4 == nil {
			// ipv6
			continue
		}
		ip := ipv4.String()

		//if "10." != ip[:3] && "172." != ip[:4] && "196." != ip[:4] && "127." != ip[:4] {
		//	return ip, nil
		//}
		ok, _ := IsInterIp(ip)
		if !ok && !ipv.IsLoopback() {
			return ip, nil
		}

	}

	return "", errors.New("no exter ip")
}

// 不指定host使用内网host
// 指定了就使用指定的，不管指定的是0.0.0.0还是内网或者外网
func GetListenAddr(a string) (string, error) {

	addrTcp, err := net.ResolveTCPAddr("tcp", a)
	if err != nil {
		return "", err
	}

	addr := addrTcp.String()
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}

	if len(host) == 0 {
		return GetServAddr(addrTcp)
	}

	return addr, nil

}

func GetServAddr(a net.Addr) (string, error) {
	addr := a.String()
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}
	if len(host) == 0 {
		host = "0.0.0.0"
	}

	ip := net.ParseIP(host)

	if ip == nil {
		return "", fmt.Errorf("ParseIP error:%s", host)
	}
	/*
		fmt.Println("ADDR TYPE", ip,
			"IsGlobalUnicast",
			ip.IsGlobalUnicast(),
			"IsInterfaceLocalMulticast",
			ip.IsInterfaceLocalMulticast(),
			"IsLinkLocalMulticast",
			ip.IsLinkLocalMulticast(),
			"IsLinkLocalUnicast",
			ip.IsLinkLocalUnicast(),
			"IsLoopback",
			ip.IsLoopback(),
			"IsMulticast",
			ip.IsMulticast(),
			"IsUnspecified",
			ip.IsUnspecified(),
		)
	*/

	raddr := addr
	if ip.IsUnspecified() {
		// 没有指定ip的情况下，使用内网地址
		inerip, err := GetInterIp()
		if err != nil {
			return "", err
		}

		raddr = net.JoinHostPort(inerip, port)
	}

	//slog.Tracef("ServAddr --> addr:[%s] ip:[%s] host:[%s] port:[%s] raddr[%s]", addr, ip, host, port, raddr)

	return raddr, nil
}

// Request.RemoteAddress contains port, which we want to remove i.e.:
// "[::1]:58292" => "[::1]"
func IpAddrFromRemoteAddr(s string) string {
	idx := strings.LastIndex(s, ":")
	if idx == -1 {
		return s
	}
	return s[:idx]
}

func IpAddrPort(s string) string {
	idx := strings.LastIndex(s, ":")
	if idx == -1 {
		return ""
	}
	return s[idx+1:]
}

// 获取http请求的client的地址
func IpAddressHttpClient(r *http.Request) string {
	hdr := r.Header
	hdrRealIp := hdr.Get("X-Real-Ip")
	hdrForwardedFor := hdr.Get("X-Forwarded-For")

	if hdrRealIp == "" && hdrForwardedFor == "" {
		return IpAddrFromRemoteAddr(r.RemoteAddr)
	}

	if hdrForwardedFor != "" {
		// X-Forwarded-For is potentially a list of addresses separated with ","
		parts := strings.Split(hdrForwardedFor, ",")
		for i, p := range parts {
			parts[i] = strings.TrimSpace(p)
		}
		// TODO: should return first non-local address
		for _, ip := range parts {
			ok, _ := IsInterIp(ip)
			if !ok && len(ip) > 5 && "127." != ip[:4] {
				return ip
			}
		}

	}

	return hdrRealIp
}

func PackdataPad(data []byte, pad byte) []byte {
	sendbuff := make([]byte, 0)
	// no pad
	var pacLen uint64 = uint64(len(data))
	buff := make([]byte, 20)
	rv := binary.PutUvarint(buff, pacLen)

	sendbuff = append(sendbuff, buff[:rv]...) // len
	sendbuff = append(sendbuff, data...)      //data
	sendbuff = append(sendbuff, pad)          //pad

	return sendbuff

}

func Packdata(data []byte) []byte {
	return PackdataPad(data, 0)
}

// 最小的消息长度、最大消息长度，数据流，包回调
// 正常返回解析剩余的数据，nil
// 否则返回错误
func UnPackdata(lenmin uint, lenmax uint, packBuff []byte, readCall func([]byte)) ([]byte, error) {
	for {

		// n == 0: buf too small
		// n  < 0: value larger than 64 bits (overflow)
		//     and -n is the number of bytes read
		pacLen, sz := binary.Uvarint(packBuff)
		if sz < 0 {
			return packBuff, errors.New("package head error")
		} else if sz == 0 {
			return packBuff, nil
		}

		// sz > 0

		// must < lenmax
		if pacLen > uint64(lenmax) {
			return packBuff, errors.New("package too long")
		} else if pacLen < uint64(lenmin) {
			return packBuff, errors.New("package too short")
		}

		apacLen := uint64(sz) + pacLen + 1
		if uint64(len(packBuff)) >= apacLen {
			pad := packBuff[apacLen-1]
			if pad != 0 {
				return packBuff, errors.New("package pad error")
			}

			readCall(packBuff[sz : apacLen-1])
			packBuff = packBuff[apacLen:]
		} else {
			return packBuff, nil
		}

	}

	return nil, errors.New("unknown err")

}

func HttpReqGetOk(url string, timeout time.Duration) ([]byte, error) {

	client := &http.Client{Timeout: timeout}
	response, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	if response.StatusCode != 200 {
		return nil, fmt.Errorf("statuscode:%d body:%s", response.StatusCode, body)

	} else {
		return body, nil
	}

}

func HttpReqPostOk(url string, data []byte, timeout time.Duration) ([]byte, error) {
	return HttpReqOk(url, "POST", data, timeout)
}

func HttpReqOk(url, method string, data []byte, timeout time.Duration) ([]byte, error) {
	body, status, err := HttpReq(url, method, data, timeout)
	if err != nil {
		return nil, err
	}
	if status != 200 {
		return nil, errors.New(fmt.Sprintf("status:%d err:%s", status, body))

	} else {
		return body, nil
	}

}

func HttpReqPost(url string, data []byte, timeout time.Duration) ([]byte, int, error) {
	return HttpReq(url, "POST", data, timeout)
}

func HttpReq(url, method string, data []byte, timeout time.Duration) ([]byte, int, error) {
	client := &http.Client{Timeout: timeout}

	reqest, err := http.NewRequest(method, url, bytes.NewReader(data))
	if err != nil {
		return nil, 0, err
	}
	reqest.Header.Set("Connection", "Keep-Alive")

	response, err := client.Do(reqest)
	if err != nil {
		return nil, 0, err
	}

	defer response.Body.Close()

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, 0, err
	}

	return body, response.StatusCode, nil

}

func HttpReqWithHeadOk(url, method string, heads map[string]string, data []byte, timeout time.Duration) ([]byte, error) {
	body, status, err := HttpReqWithHead(url, method, heads, data, timeout)
	if err != nil {
		return nil, err
	}

	if status < 200 || status > 299 {
		return nil, errors.New(fmt.Sprintf("status:%d err:%s", status, body))

	} else {
		return body, nil
	}

}

func HttpReqWithHead(url, method string, heads map[string]string, data []byte, timeout time.Duration) ([]byte, int, error) {
	client := &http.Client{Timeout: timeout}

	reqest, err := http.NewRequest(method, url, bytes.NewReader(data))
	if err != nil {
		return nil, 0, err
	}

	for key, val := range heads {
		reqest.Header.Set(key, val)
	}

	response, err := client.Do(reqest)
	if err != nil {
		return nil, 0, err
	}

	defer response.Body.Close()

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, 0, err
	}

	return body, response.StatusCode, nil

}

func PackageSplit(conn net.Conn, readtimeout time.Duration, readCall func([]byte)) (bool, []byte, error) {
	buffer := make([]byte, 2048)
	packBuff := make([]byte, 0)

	for {
		conn.SetReadDeadline(time.Now().Add(readtimeout))
		bytesRead, err := conn.Read(buffer)
		if err != nil {
			return true, nil, err
		}

		packBuff = append(packBuff, buffer[:bytesRead]...)

		packBuff, err = UnPackdata(1, 1024*5, packBuff, readCall)

		if err != nil {
			return false, packBuff, err
		}

	}

	return false, nil, errors.New("fuck err")

}
