package kafka

// TODO: for ddl support
type databaseHistory interface {
	recode()
	recover()
}
