package v2

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/actiontech/dtle/g"
	"github.com/labstack/echo/v4"

	"github.com/actiontech/dtle/drivers/api/handler"

	"github.com/actiontech/dtle/drivers/api/models"
	"github.com/actiontech/dtle/drivers/mysql/mysql/mysqlconfig"
	"github.com/actiontech/dtle/drivers/mysql/mysql/sql"
)

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
// @Success 200 {object} models.ListMysqlSchemasRespV2
// @Router /v2/mysql/schemas [get]
func ListMysqlSchemasV2(c echo.Context) error {
	logger := handler.NewLogger().Named("ListMysqlSchemasV2")
	logger.Info("validate params")
	reqParam := new(models.ListDatabaseSchemasReqV2)
	if err := c.Bind(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("convert request param failed. you should check the param format, error: %v", err)))
	}
	if err := c.Validate(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invalid params:\n%v", err)))
	}

	uri, err := buildMysqlUri(reqParam.MysqlHost, reqParam.MysqlUser, reqParam.MysqlPassword,
		reqParam.MysqlCharacterSet, int(reqParam.MysqlPort), reqParam.IsMysqlPasswordEncrypted)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("build mysql Uri failed: %v", err)))
	}

	db, err := sql.CreateDB(uri)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("create db failed: %v", err)))
	}
	defer db.Close()

	logger.Info("get schemas and tables from mysql")

	dbs, err := sql.ShowDatabases(db)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("showdatabases failed: %v", err)))
	}

	replicateDoDb := []*models.SchemaItem{}
	for _, dbName := range dbs {
		tbs, err := sql.ShowTables(db, dbName, true)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("showtables failed: %v", err)))
		}

		tables := []*models.TableItem{}
		for _, t := range tbs {
			if strings.ToLower(t.TableType) == "view" {
				continue
			}
			tb := &models.TableItem{
				TableName: t.TableName,
			}
			tables = append(tables, tb)
		}

		schema := &models.SchemaItem{
			SchemaName: dbName,
			Tables:     tables,
		}
		replicateDoDb = append(replicateDoDb, schema)
	}

	return c.JSON(http.StatusOK, &models.ListMysqlSchemasRespV2{
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
	logger.Info("validate params")
	reqParam := new(models.ListColumnsReqV2)
	if err := c.Bind(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("convert request param failed. you should check the param format, error: %v", err)))
	}
	if err := c.Validate(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invalid params:\n%v", err)))
	}
	uri, err := buildMysqlUri(reqParam.MysqlHost, reqParam.MysqlUser, reqParam.MysqlPassword,
		reqParam.MysqlCharacterSet, int(reqParam.MysqlPort), reqParam.IsMysqlPasswordEncrypted)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("build mysql Uri failed: %v", err)))
	}

	db, err := sql.CreateDB(uri)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("create db failed: %v", err)))
	}
	defer db.Close()

	logger.Info("get columns")

	columns, err := sql.ListColumns(db, reqParam.MysqlSchema, reqParam.MysqlTable)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("find db columns failed: %v", err)))

	}
	return c.JSON(http.StatusOK, &models.ListColumnsRespV2{
		Columns:  columns,
		BaseResp: models.BuildBaseResp(nil),
	})
}

func buildMysqlUri(host, user, pwd, characterSet string, port int, isMysqlPasswordEncrypted bool) (string, error) {
	mysqlConnectionConfig := mysqlconfig.ConnectionConfig{
		Host:     host,
		Port:     port,
		User:     user,
		Password: pwd,
		Charset:  characterSet,
	}

	if "" == mysqlConnectionConfig.Charset {
		mysqlConnectionConfig.Charset = "utf8"
	}
	if "" != mysqlConnectionConfig.Password && isMysqlPasswordEncrypted {
		realPwd, err := handler.DecryptPassword(mysqlConnectionConfig.Password, g.RsaPrivateKey)
		if nil != err {
			return "", fmt.Errorf("DecryptPassword failed: %v", err)
		}
		mysqlConnectionConfig.Password = realPwd
	}

	uri := mysqlConnectionConfig.GetDBUri()
	return uri, nil
}

// @Id Connection
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
func Connection(c echo.Context) error {
	logger := handler.NewLogger().Named("Connection")
	logger.Info("validate params")
	reqParam := new(models.ConnectionReqV2)
	if err := c.Bind(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("convert request param failed. you should check the param format, error: %v", err)))
	}
	if err := c.Validate(reqParam); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invalid params:\n%v", err)))
	}

	uri, err := buildMysqlUri(reqParam.MysqlHost, reqParam.MysqlUser, reqParam.MysqlPassword,
		"", int(reqParam.MysqlPort), reqParam.IsMysqlPasswordEncrypted)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("build mysql Uri failed: %v", err)))
	}

	db, err := sql.CreateDB(uri)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("open db failed: %v", err)))
	}
	defer db.Close()

	err = db.Ping()
	return c.JSON(http.StatusOK, &models.ConnectionRespV2{
		BaseResp: models.BuildBaseResp(err),
	})
}
