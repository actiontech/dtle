package v2

import (
	"net/http"

	"github.com/actiontech/dtle/api/handler"

	"github.com/actiontech/dtle/api/models"
	"github.com/labstack/echo/v4"
)

// This file Deprecated, use database instead

// @Id ListMysqlSchemasV2
// @Description list schemas of mysql source instance.
// @Tags mysql
// @Security ApiKeyAuth
// @Param mysql_host query string true "mysql host"
// @Param mysql_port query string true "mysql port"
// @Param mysql_user query string true "mysql user"
// @Param mysql_password query string true "mysql password"
// @Param mysql_character_set query string false "mysql character set"
// @Param is_mysql_password_encrypted query bool false "indecate that mysql password is encrypted or not"
// @Success 200 {object} models.ListSchemasRespV2
// @Router /v2/mysql/schemas [get]
func ListMysqlSchemasV2(c echo.Context) error {
	logger := handler.NewLogger().Named("ListMysqlSchemasV2")
	reqParam := new(models.ListMySQLSchemasReqV2)
	if err := handler.BindAndValidate(logger, c, reqParam); err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	newReqParam := &models.ListDatabaseSchemasReqV2{
		Host:                reqParam.MysqlHost,
		Port:                int(reqParam.MysqlPort),
		User:                reqParam.MysqlUser,
		Password:            reqParam.MysqlPassword,
		DatabaseType:        DB_TYPE_MYSQL,
		CharacterSet:        reqParam.MysqlCharacterSet,
		IsPasswordEncrypted: reqParam.IsMysqlPasswordEncrypted,
	}
	replicateDoDb, err := listMySQLSchema(logger, newReqParam)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	return c.JSON(http.StatusOK, &models.ListSchemasRespV2{
		Schemas:  replicateDoDb,
		BaseResp: models.BuildBaseResp(nil),
	})
}

// @Id ListMysqlColumnsV2
// @Description list columns of mysql source instance.
// @Tags mysql
// @Security ApiKeyAuth
// @Param mysql_host query string true "mysql host"
// @Param mysql_port query string true "mysql port"
// @Param mysql_user query string true "mysql user"
// @Param mysql_password query string true "mysql password"
// @Param mysql_schema query string true "mysql schema"
// @Param mysql_table query string true "mysql table"
// @Param mysql_character_set query string false "mysql character set"
// @Param is_mysql_password_encrypted query bool false "indecate that mysql password is encrypted or not"
// @Success 200 {object} models.ListColumnsRespV2
// @Router /v2/mysql/columns [get]
func ListMysqlColumnsV2(c echo.Context) error {
	logger := handler.NewLogger().Named("ListMysqlColumnsV2")
	reqParam := new(models.ListMySQLColumnsReqV2)
	if err := handler.BindAndValidate(logger, c, reqParam); err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	newReqParam := &models.ListColumnsReqV2{
		Host:                reqParam.MysqlHost,
		Port:                int(reqParam.MysqlPort),
		User:                reqParam.MysqlUser,
		Password:            reqParam.MysqlPassword,
		CharacterSet:        reqParam.MysqlCharacterSet,
		DatabaseType:        DB_TYPE_MYSQL,
		Schema:              reqParam.MysqlSchema,
		Table:               reqParam.MysqlTable,
		IsPasswordEncrypted: reqParam.IsMysqlPasswordEncrypted,
	}
	columns, err := listMySQLColumns(logger, newReqParam)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}

	return c.JSON(http.StatusOK, &models.ListColumnsRespV2{
		Columns:  columns,
		BaseResp: models.BuildBaseResp(nil),
	})
}

// @Id MySQLConnectionV2
// @Description connect to  mysql instance.
// @Tags mysql
// @Security ApiKeyAuth
// @Param mysql_host query string true "mysql host"
// @Param mysql_port query string true "mysql port"
// @Param mysql_user query string true "mysql user"
// @Param mysql_password query string true "mysql password"
// @Param is_mysql_password_encrypted query bool false "indecate that mysql password is encrypted or not"
// @Success 200 {object} models.ConnectionRespV2
// @Router /v2/mysql/instance_connection [get]
func MySQLConnectionV2(c echo.Context) error {
	logger := handler.NewLogger().Named("MySQLConnectionReqV2")
	reqParam := new(models.MySQLConnectionReqV2)
	if err := handler.BindAndValidate(logger, c, reqParam); err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	newReqParam := &models.ConnectionReqV2{
		DatabaseType:        DB_TYPE_MYSQL,
		Host:                reqParam.MysqlHost,
		Port:                int(reqParam.MysqlPort),
		User:                reqParam.MysqlUser,
		Password:            reqParam.MysqlPassword,
		IsPasswordEncrypted: reqParam.IsMysqlPasswordEncrypted,
	}
	err := connectDatabase(newReqParam)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	return c.JSON(http.StatusOK, &models.ConnectionRespV2{
		BaseResp: models.BuildBaseResp(nil),
	})
}
