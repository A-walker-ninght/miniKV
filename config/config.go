package config

import (
	"time"
)

// Config 数据库启动配置
// dataDir: logFile/sst/fileName
// WalDir: logFile/wal/wal.log memtable
// WalDir: logFile/wal/wal_id.iog immumtable
// LevelSize
// 0: 16MB
// 1: 32MB
// 2: 100MB
// 3: 200MB
// 4: 300MB
// 5: 400MB
// 6: 500MB

// LevelDir: logFile/lv.log
// PartSize: 10
// Threshold: 1000
// CheckInterval: 1s
// MaxLevelNum: 7

type Config struct {
	DataDir       string        // 数据目录
	WalDir        string        // wal目录
	LevelSize     LevelSize     // 每层大小
	PartSize      int           // 每层中 SsTable 表数量的阈值，该层 SsTable 将会被压缩到下一层
	Threshold     int           // 内存表的 kv 最大数量，超出这个阈值，内存表将会被保存到 SsTable 中
	CheckInterval time.Duration // 压缩内存、文件的时间间隔，多久进行一次检查工作
	MaxLevelNum   int           // lsm最大层级
}

type LevelSize struct {
	LSizes []int
}
