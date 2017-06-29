package models

import gonats "github.com/nats-io/go-nats"

type TableStats struct {
	InsertCount int64
	UpdateCount int64
	DelCount    int64
}

type DelayCount struct {
	Num  uint64
	Time uint64
}

type ThroughputStat struct {
	Num  uint64
	Time uint64
}

type MsgStat struct {
	InMsgs   uint64
	OutMsgs  uint64
	InBytes  uint64
	OutBytes uint64
}

type Stats struct {
	TableStats     *TableStats
	DelayCount     *DelayCount
	ThroughputStat *ThroughputStat
	MsgStat        gonats.Statistics
	Status         string
}

type TaskStatistics struct {
	Stats     *Stats
	Timestamp int64
}

type AllocStatistics struct {
	Tasks map[string]*TaskStatistics
}
