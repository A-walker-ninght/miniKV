package miniKV

import (
	"fmt"
	"github.com/A-walker-ninght/miniKV/config"
	"github.com/A-walker-ninght/miniKV/lsm"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func InitDB() {
	con := config.Config{
		DataDir: "./logFile/sst/",
		WalDir:  "./logFile/wal/",
		LevelSize: config.LevelSize{
			LSizes: []int{4, 8, 16, 32, 64, 128, 256},
		},
		PartSize:      15,
		Threshold:     2000,
		CheckInterval: 3 * time.Microsecond,
		MaxLevelNum:   7,
	}
	db = &DB{
		lock:    &sync.RWMutex{},
		opt:     &con,
		writeCh: make(chan *request, 20),
		checkCh: make(chan struct{}, 5),
		close:   make(chan struct{}, 0),
	}
	db.lsm = lsm.NewLSM(con)
	go db.schedule()
}

func TestDBAcid(t *testing.T) {
	InitDB()
	for i := 0; i < 10000; i++ {
		key, value := fmt.Sprintf("key%d", i), []byte(fmt.Sprintf("key%d", i))
		db.Set(key, value)
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
