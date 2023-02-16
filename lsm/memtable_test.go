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
		e := codec.NewEntry(key, value)
		m.Add(&e)
		_, status := m.Search(key)
		assert.Equal(t, status, codec.Found)
	}

	// 删除
	e := codec.NewEntry("key1", []byte("key1"))
	e.Deleted = true
	m.Delete(&e)
	_, status := m.Search("key1")
	assert.Equal(t, status, codec.Deleted)

	e = codec.NewEntry("key3", []byte("key3"))
	e.Deleted = true
	m.Delete(&e)
	_, status = m.Search("key3")
	assert.Equal(t, status, codec.Deleted)

	e = codec.NewEntry("key5", []byte("key5"))
	e.Deleted = true
	m.Delete(&e)
	_, status = m.Search("key5")
	assert.Equal(t, status, codec.Deleted)

}
