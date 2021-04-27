package models

type ListDatabaseSchemasReqV2 struct {
	MysqlHost         string `query:"mysql_host" validate:"required"`
	MysqlPort         int    `query:"mysql_port" validate:"required"`
	MysqlUser         string `query:"mysql_user" validate:"required"`
	MysqlPassword     string `query:"mysql_password" validate:"required"`
	MysqlCharacterSet *string `query:"mysql_character_set"`
}

type ListDatabaseSchemasRespV2 struct {
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
