package models

type ListDatabaseSchemasReqV2 struct {
	Host                string `query:"host" validate:"required"`
	Port                int    `query:"port" validate:"required"`
	User                string `query:"user" validate:"required"`
	Password            string `query:"password" validate:"required"`
	DatabaseType        string `query:"database_type" validate:"required"`
	ServiceName         string `query:"service_name"`
	CharacterSet        string `query:"character_set"`
	IsPasswordEncrypted bool   `query:"is_password_encrypted"`
}

type ListSchemasRespV2 struct {
	Schemas []*SchemaItem `json:"schemas"`
	BaseResp
}

type SchemaItem struct {
	SchemaName string       `json:"schema_name"`
	Tables     []*TableItem `json:"tables"`
}

type TableItem struct {
	TableName string `json:"table_name"`
}

type ListColumnsReqV2 struct {
	Host                string `query:"host" validate:"required"`
	Port                int    `query:"port" validate:"required"`
	User                string `query:"user" validate:"required"`
	Password            string `query:"password" validate:"required"`
	CharacterSet        string `query:"character_set"`
	DatabaseType        string `query:"database_type" validate:"required"`
	ServiceName         string `query:"service_name"`
	Schema              string `query:"schema" validate:"required"`
	Table               string `query:"table" validate:"required"`
	IsPasswordEncrypted bool   `query:"is_password_encrypted"`
}

type ListColumnsRespV2 struct {
	Columns []string `json:"columns"`
	BaseResp
}

type ConnectionReqV2 struct {
	DatabaseType        string `query:"database_type" validate:"required"`
	ServiceName         string `query:"service_name"`
	Host                string `query:"host" validate:"required"`
	Port                int    `query:"port" validate:"required"`
	User                string `query:"user" validate:"required"`
	Password            string `query:"password" validate:"required"`
	IsPasswordEncrypted bool   `query:"is_password_encrypted"`
}

type ConnectionRespV2 struct {
	BaseResp
}
