package miniKV

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func InitDB() {
	Init()
}

func TestDBAcid(t *testing.T) {
	InitDB()
	for i := 0; i < 10000; i++ {
		key, value := fmt.Sprintf("key%d", i), []byte(fmt.Sprintf("key%d", i))
		db.Set(key, value)
		// fmt.Println(err)
	}
	for i := 0; i < 10000; i++ {
		key, value := fmt.Sprintf("key%d", i), []byte(fmt.Sprintf("key%d", i))
		v := db.Get(key)
		assert.Equal(t, v, value)
	}

	for i := 0; i < 100; i++ {
		key, value := fmt.Sprintf("key%d", i), []byte(fmt.Sprintf("key%d", i))
		db.Del(key)

		v := db.Get(key)
		assert.Equal(t, v, value)
	}

}
