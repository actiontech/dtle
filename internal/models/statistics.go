package models

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

type Stats struct {
	TableStats     *TableStats
	DelayCount     *DelayCount
	ThroughputStat *ThroughputStat
}

type TaskStatistics struct {
	Stats     *Stats
	Timestamp int64
}

type AllocStatistics struct {
	Tasks map[string]*TaskStatistics
}
