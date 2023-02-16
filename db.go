package miniKV

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/A-walker-ninght/miniKV/config"
	"github.com/A-walker-ninght/miniKV/lsm"
)

type DBAPI interface {
	Get(key string) []byte
	Set(key string, value interface{}) error
	Del(key string) error
	Close() error
	Options() config.Config
}

type request struct {
	key   string
	value interface{}
}

func (r *request) getKey() string {
	return r.key
}

func (r *request) getValue() []byte {
	v, _ := json.Marshal(r.value)
	return v
}

type DB struct {
	lock    *sync.RWMutex
	lsm     *lsm.LSM
	opt     *config.Config
	writeCh chan *request
	checkCh chan struct{}
	close   chan struct{}
}

var db *DB

func init() {
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

func (d *DB) schedule() {
	for {
		select {
		case <-d.close:
			return
		case r := <-d.writeCh:
			go d.lsm.Set(r.getKey(), r.getValue())
		default:
		}
	}
}

func (d *DB) Get(key string) interface{} {
	v := d.lsm.Search(key)
	if len(v) == 0 {
		return []byte{}
	}
	var value interface{}
	json.Unmarshal(v, &value)
	return value
}

func (d *DB) Set(key string, value interface{}) {
	r := &request{
		key:   key,
		value: value,
	}
	for {
		select {
		case d.writeCh <- r:
			return
		default:
		}
	}
}
func (d *DB) Del(key string) {
	d.lsm.Delete(key)
}

func (d *DB) Options() config.Config {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return *d.opt
}

func (d *DB) Close() error {
	for c := range d.writeCh {
		go d.lsm.Set(c.getKey(), c.getValue())
	}
	close(d.checkCh)
	d.lsm.Close()
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		d.close <- struct{}{}
	}()
	wg.Wait()
	return nil
}
