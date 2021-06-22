package models

type ListDatabaseSchemasReqV2 struct {
	MysqlHost                string `query:"mysql_host" validate:"required"`
	MysqlPort                uint32 `query:"mysql_port" validate:"required"`
	MysqlUser                string `query:"mysql_user" validate:"required"`
	MysqlPassword            string `query:"mysql_password" validate:"required"`
	MysqlCharacterSet        string `query:"mysql_character_set"`
	IsMysqlPasswordEncrypted bool   `query:"is_mysql_password_encrypted"`
}

type ListMysqlSchemasRespV2 struct {
	Schemas []*SchemaItem `json:"schemas"`
	BaseResp
}

type SchemaItem struct {
	SchemaName string `json:"schema_name"`
	Tables     []*TableItem
}

type TableItem struct {
	TableName string `json:"table_name"`
}

type ListColumnsReqV2 struct {
	MysqlHost                string `query:"mysql_host" validate:"required"`
	MysqlPort                uint32 `query:"mysql_port" validate:"required"`
	MysqlUser                string `query:"mysql_user" validate:"required"`
	MysqlPassword            string `query:"mysql_password" validate:"required"`
	MysqlCharacterSet        string `query:"mysql_character_set"`
	MysqlSchema              string `query:"mysql_schema" validate:"required"`
	MysqlTable               string `query:"mysql_table" validate:"required"`
	IsMysqlPasswordEncrypted bool   `query:"is_mysql_password_encrypted"`
}

type ListColumnsRespV2 struct {
	Columns []string `json:"columns"`
	BaseResp
}
