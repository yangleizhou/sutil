/*
 * Copyright (C) Chad Sang
 * Copyright (C) ixiaochuan.cn
 */
package sutil

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

var cases = [...]string{"", ".", "..", ".0.", ".0.2", "0", "0.1", "0.1.3", "1.0.0", "4.1.3.2"}

// BenchmarkPrintf-8   	  200000	      6294 ns/op	    2304 B/op	      66 allocs/op
// BenchmarkRepeat-8    	  300000	      5409 ns/op	    3360 B/op	      91 allocs/op
// BenchmarkBuffer-8    	  500000	      3769 ns/op	    3440 B/op	      71 allocs/op
// BenchmarkParser-8    	  300000	      4087 ns/op	    3232 B/op	      81 allocs/op

func BenchmarkPrintf(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, k := range cases {
			fmtver(k)
		}
	}
}

func BenchmarkRepeat(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, k := range cases {
			fmtver2(k)
		}
	}
}

func BenchmarkBuffer(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, k := range cases {
			fmtver3(k)
		}
	}
}

func BenchmarkParser(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, k := range cases {
			fmtver4(k)
		}
	}
}

func TestFmtver(t *testing.T) {
	for _, k := range cases {
		answer := fmtver(k)

		v2 := fmtver2(k)
		if v2 != answer {
			t.Errorf("k: %s, v2: %s (expected: %s)", k, v2, answer)
		}

		v3 := fmtver3(k)
		if v3 != answer {
			t.Errorf("k: %s, v3: %s (expected: %s)", k, v3, answer)
		}

		v4 := fmtver4(k)
		if v4 != answer {
			t.Errorf("k: %s, v4: %s (expected: %s)", k, v4, answer)
		}
	}
}

func fmtver(ver string) string {
	pvs := strings.Split(ver, ".")

	rv := ""
	for _, pv := range pvs {
		rv += fmt.Sprintf("%020s", pv)
	}

	return rv
}

func fmtver2(ver string) string {
	pvs := strings.Split(ver, ".")

	rv := ""
	for _, pv := range pvs {
		count := 20 - len(pv)
		if count > 0 {
			pv = strings.Repeat("0", count) + pv
		}
		rv += pv
	}

	return rv
}

func fmtver3(ver string) string {
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

func fmtver4(ver string) string {
	var result string
	p := 0
	for i, c := range ver {
		if c != '.' {
			continue
		}

		count := 20 - i + p
		if count > 0 {
			result += strings.Repeat("0", count)
		}

		result += ver[p:i]
		p = i + 1
	}

	i := len(ver)
	if p <= i {
		count := 20 - i + p
		if count > 0 {
			result += strings.Repeat("0", count)
		}

		result += ver[p:i]
	}

	return result
}
