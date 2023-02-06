package utils

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

var CHARS = []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
	"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
	"1", "2", "3", "4", "5", "6", "7", "8", "9", "0"}

func randString(lenNum int) string {
	str := strings.Builder{}
	length := 52
	lenNum %= 27
	for i := 0; i < lenNum; i++ {
		str.WriteString(CHARS[rand.Intn(length)])
	}
	return str.String()
}

func TestBloomBasic(t *testing.T) {
	n := 1000
	b := NewFilter(n, 0.01)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%d", i)
		b.Insert(key)
	}
	c := 0
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%d", i)
		err := b.Check(key)
		if !err {
			c++
		}
	}
	if float64(c)/float64(n) > 0.01 {
		t.Errorf("length: %d and false positive: %f", n, float64(c)/float64(n))
	}
}

func Benchmark_BloomBasic(b *testing.B) {
	n := 1000
	f := NewFilter(n, 0.01)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%d", i)
		f.Insert(key)
	}
	c := 0
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key%d", i)
		err := f.Check(key)
		if !err {
			c++
		}
	}
	if float64(c)/float64(n) > 0.01 {
		b.Errorf("length: %d and false positive: %f", n, float64(c)/float64(n))
	}
}

func TestBloomSearchNull(t *testing.T) {
	b := NewFilter(10000, 0.01)
	strs := map[string]bool{}
	for k := 1; k <= 10000; k++ {
		str := randString(k)
		b.Insert(str)
		strs[str] = true
	}
	cnt := 0
	for i := 13456; i < 23456; i++ {
		key := randString(i)
		err := b.Check(key)
		if !strs[key] && err {
			cnt++
		}
	}
	c := 0
	for str, _ := range strs {
		err := b.Check(str)
		if !err {
			c++
		}
	}
	if float64(c)/float64(10000) > 0.01 || float64(cnt)/float64(10000) > 0.01 {
		t.Errorf("false positive too high and Search InsertKey: %d, Search NoInsertKey: %d", c, cnt)
	}
}

func Benchmark_BloomSearchNull(b *testing.B) {
	f := NewFilter(10000, 0.01)
	strs := map[string]bool{}
	for k := 1; k <= 10000; k++ {
		str := randString(k)
		f.Insert(str)
		strs[str] = true
	}
	cnt := 0
	for i := 13456; i < 23456; i++ {
		key := randString(i)
		err := f.Check(key)
		if !strs[key] && err {
			cnt++
		}
	}
	c := 0
	for str, _ := range strs {
		err := f.Check(str)
		if !err {
			c++
		}
	}
	if float64(c)/float64(10000) > 0.01 || float64(cnt)/float64(10000) > 0.01 {
		b.Errorf("false positive too high and Search InsertKey: %d, Search NoInsertKey: %d", c, cnt)
	}
}
