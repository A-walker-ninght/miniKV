package lsm

import (
	"fmt"
	"github.com/A-walker-ninght/miniKV/codec"
	"github.com/stretchr/testify/assert"

	"testing"
)

func TestMemtableBasicAcid(t *testing.T) {
	m := NewMemTable(1000, "test_wal.log")
	for i := 0; i < 100; i++ {
		key, value := fmt.Sprintf("key%d", i), []byte(fmt.Sprintf("key%d", i))
		e := codec.NewEntry(key, value, int64(i))
		m.Add(e)
		v := m.Search(key)
		assert.Equal(t, v, value)
	}

	// 删除
	e := codec.NewEntry("key1", []byte("key1"), 2)
	m.Delete(e)
	v := m.Search("key1")
	assert.Equal(t, []byte{}, v)

	e = codec.NewEntry("key3", []byte("key3"), 2)

	m.Delete(e)
	v = m.Search("key3")
	assert.Equal(t, []byte("key3"), v)

	e = codec.NewEntry("key5", []byte("key5"), 4)
	m.Delete(e)
	v = m.Search("key5")

	assert.Equal(t, []byte("key5"), v)

}
