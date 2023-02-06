package utils

import (
	"math"
)

const (
	seed = 0xbc9f1d34
	m    = 0xc6a4a793
)

type Filter []byte

type BloomFilter struct {
	f Filter
	k uint8
}

// m, n, fp, k: 位数组大小，插入元素个数，误报率，哈希函数个数
func NewFilter(n int, fp float64) *BloomFilter {
	b := &BloomFilter{}
	bitsPerKey := calBitsPerKey(n, fp)
	k := calK(bitsPerKey)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	b.k = uint8(k)
	nBits := bitsPerKey * n
	if nBits < 64 {
		nBits = 64
	}
	nBytes := (nBits + 7) / 8
	nBits = nBytes * 8
	filter := make([]byte, nBytes+1)
	filter[nBytes] = uint8(k) // 最后一位存k
	b.f = filter
	return b
}

// 插入key
func (b *BloomFilter) Insert(key string) bool {
	k := b.k
	if k > 30 {
		return true
	}
	h := hash([]byte(key))
	nBits := uint32((b.Len() - 1) * 8)
	delta := h>>17 | h<<15
	for i := uint8(0); i < k; i++ {
		pos := h % uint32(nBits)
		b.f[pos/8] |= 1 << (pos % 8)
		h += delta
	}
	return true
}

// 检查key
func (b *BloomFilter) Check(key string) bool {
	if b.Len() < 2 {
		return false
	}
	k := b.k
	if k > 30 {
		return true
	}
	h := hash([]byte(key))
	nBits := uint32((b.Len() - 1) * 8)
	delta := h>>17 | h<<15
	for i := uint8(0); i < k; i++ {
		pos := h % uint32(nBits)
		if b.f[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
		h += delta
	}
	return true
}

func (b *BloomFilter) reset() {
	if b == nil {
		return
	}
	for i := range b.f {
		b.f[i] = 0
	}
}

func (b *BloomFilter) Len() int32 {
	return int32(len(b.f))
}

// m := -1*n*lnp/(ln2)^2
// 一个key占多少位
func calBitsPerKey(n int, fp float64) int {
	m := -1 * float64(n) * math.Log(fp) / (math.Pow(math.Log(float64(2)), 2))
	locs := math.Ceil(m / float64(n))
	return int(locs)
}

// k := ln2*m/n
func calK(bitsPerKey int) uint32 {
	return uint32(math.Log(2) * float64(bitsPerKey))
}

func hash(b []byte) uint32 {
	h := uint32(seed) ^ uint32(len(b))*m
	for ; len(b) >= 4; b = b[4:] {
		h += uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		h *= m
		h ^= h >> 16
	}
	switch len(b) {
	case 3:
		h += uint32(b[2]) << 16
		fallthrough
	case 2:
		h += uint32(b[1]) << 8
		fallthrough
	case 1:
		h += uint32(b[0])
		h *= m
		h ^= h >> 24
	}
	return h
}
