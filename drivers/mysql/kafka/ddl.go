package kafka

// See: debezium/debezium-core/src/main/java/io/debezium/relational/history

type DDLSource struct {
	Server string `json:"server"`
}
type DDLPosition struct {
	// See dbz `offsetUsingPosition`.
	TsSec         int64  `json:"ts_sec"`
	File          string  `json:"file"`
	Pos           int64   `json:"pos"`
	Gtids         string  `json:"gtids"`
	Snapshot      bool    `json:"snapshot,omitempty"`
}
type DDLTableChange struct {
	// create/alter/drop
	Type  string `json:"type"`
	ID    string `json:"id"`
	Table struct {
		DefaultCharsetName    string   `json:"defaultCharsetName"`
		PrimaryKeyColumnNames []string `json:"primaryKeyColumnNames"`
		Columns               []struct {
			// See dbz `JsonTableChangeSerializer.toDocument(Column column)`.
			Name            string  `json:"name"`
			JdbcType        int     `json:"jdbcType"`
			NativeType      *int    `json:"nativeType,omitempty"`
			TypeName        string  `json:"typeName"`
			TypeExpression  string  `json:"typeExpression"`
			CharsetName     *string `json:"charsetName"`
			Length          *int    `json:"length,omitempty"`
			Scale           *int    `json:"scale,omitempty"`
			Position        int     `json:"position"`
			Optional        bool    `json:"optional"`
			AutoIncremented bool    `json:"autoIncremented"`
			Generated       bool    `json:"generated"`
		} `json:"columns"`
	} `json:"table"`
	Comment string `json:"comment"`
}

type DDLPayload struct {
	// See dbz `class HistoryRecord`.
	Source DDLSource `json:"source,omitempty"`
	Position DDLPosition `json:"position,omitempty"`
	DatabaseName string `json:"databaseName,omitempty"`
	// unused for MySQL
	SchemaName   string `json:"schemaName,omitempty"`
	DDL          string `json:"ddl"`
	TableChanges []DDLTableChange `json:"tableChanges"`
}
