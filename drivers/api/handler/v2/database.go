package v2

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/actiontech/dtle/drivers/driver/mysql/mysqlconfig"
	"github.com/actiontech/dtle/drivers/driver/oracle/config"

	"github.com/hashicorp/go-hclog"

	"github.com/actiontech/dtle/g"
	"github.com/labstack/echo/v4"

	"github.com/actiontech/dtle/drivers/api/handler"

	"github.com/actiontech/dtle/drivers/api/models"
	"github.com/actiontech/dtle/drivers/driver/mysql/sql"
)

const (
	DB_TYPE_MYSQL  = "MySQL"
	DB_TYPE_ORACLE = "Oracle"
)

// @Id ListDatabaseSchemasV2
// @Description list schemas of database source instance.
// @Tags database
// @Security ApiKeyAuth
// @Param database_type query string true "database_type"
// @Param host query string true "database host"
// @Param port query int true "database port"
// @Param user query string true "database user"
// @Param password query string true "database password"
// @Param service_name query string false "database service_name"
// @Param character_set query string false "database character set"
// @Param is_password_encrypted query bool false "indecate that database password is encrypted or not"
// @Success 200 {object} models.ListSchemasRespV2
// @Router /v2/database/schemas [get]
func ListDatabaseSchemasV2(c echo.Context) error {
	logger := handler.NewLogger().Named("ListDatabaseSchemasV2")
	reqParam := new(models.ListDatabaseSchemasReqV2)
	err := handler.BindAndValidate(logger, c, reqParam)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}

	replicateDoDb := make([]*models.SchemaItem, 0)
	switch reqParam.DatabaseType {
	case DB_TYPE_MYSQL:
		replicateDoDb, err = listMySQLSchema(logger, reqParam)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
		}
	case DB_TYPE_ORACLE:
		replicateDoDb, err = listOracleSchema(logger, reqParam)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
		}
	}

	return c.JSON(http.StatusOK, &models.ListSchemasRespV2{
		Schemas:  replicateDoDb,
		BaseResp: models.BuildBaseResp(nil),
	})
}

func listMySQLSchema(logger hclog.Logger, reqParam *models.ListDatabaseSchemasReqV2) ([]*models.SchemaItem, error) {
	uri, err := buildMysqlUri(reqParam.Host, reqParam.User, reqParam.Password,
		reqParam.CharacterSet, reqParam.Port, reqParam.IsPasswordEncrypted)
	if err != nil {
		return nil, fmt.Errorf("build database Uri failed: %v", err)
	}

	db, err := sql.CreateDB(uri)
	if err != nil {
		return nil, fmt.Errorf("create db failed: %v", err)
	}
	defer db.Close()

	logger.Info("get schemas and tables from mysql")

	dbs, err := sql.ShowDatabases(db)
	if err != nil {
		return nil, fmt.Errorf("showdatabases failed: %v", err)
	}

	replicateDoDb := make([]*models.SchemaItem, 0)
	for _, dbName := range dbs {
		tbs, err := sql.ShowTables(db, dbName, true)
		if err != nil {
			return nil, fmt.Errorf("showtables failed: %v", err)
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
	return replicateDoDb, nil
}

func listOracleSchema(logger hclog.Logger, reqParam *models.ListDatabaseSchemasReqV2) ([]*models.SchemaItem, error) {
	if reqParam.IsPasswordEncrypted && reqParam.Password != "" {
		realPwd, err := handler.DecryptPassword(reqParam.Password, g.RsaPrivateKey)
		if nil != err {
			return nil, err
		}
		reqParam.Password = realPwd
	}
	oracleDb, err := config.NewDB(&config.OracleConfig{
		User:        reqParam.User,
		Password:    reqParam.Password,
		Host:        reqParam.Host,
		Port:        reqParam.Port,
		ServiceName: reqParam.ServiceName,
	})
	if err != nil {
		return nil, fmt.Errorf("new oracle db err %v", err)
	}
	defer oracleDb.Close()

	logger.Info("get schemas and tables from oracle")

	schemas, err := oracleDb.GetSchemas()
	if err != nil {
		return nil, fmt.Errorf("get oracle schemas err : %v", err)
	}
	replicateDoDb := make([]*models.SchemaItem, 0)
	for _, schema := range schemas {
		tableNames, err := oracleDb.GetTables(schema)
		if err != nil {
			return nil, fmt.Errorf("get oracle schema %v tables failed: %v", schema, err)
		}
		tables := []*models.TableItem{}
		for _, tableName := range tableNames {
			tb := &models.TableItem{
				TableName: tableName,
			}
			tables = append(tables, tb)
		}

		schema := &models.SchemaItem{
			SchemaName: schema,
			Tables:     tables,
		}
		replicateDoDb = append(replicateDoDb, schema)
	}
	return replicateDoDb, nil

}

// @Id ListDatabaseColumnsV2
// @Description list columns of database source instance.
// @Tags database
// @Security ApiKeyAuth
// @Param host query string true "database host"
// @Param port query int true "database port"
// @Param user query string true "database user"
// @Param password query string true "database password"
// @Param database_type query string true "database_type"
// @Param service_name query string false "database service_name"
// @Param schema query string true "database schema"
// @Param table query string true "database table"
// @Param character_set query string false "database character set"
// @Param is_password_encrypted query bool false "indecate that database password is encrypted or not"
// @Success 200 {object} models.ListColumnsRespV2
// @Router /v2/database/columns [get]
func ListDatabaseColumnsV2(c echo.Context) error {
	logger := handler.NewLogger().Named("ListDatabaseColumnsV2")
	reqParam := new(models.ListColumnsReqV2)
	err := handler.BindAndValidate(logger, c, reqParam)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}

	columns := make([]string, 0)
	switch reqParam.DatabaseType {
	case DB_TYPE_MYSQL:
		columns, err = listMySQLColumns(logger, reqParam)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
		}
	case DB_TYPE_ORACLE:
		if reqParam.IsPasswordEncrypted && reqParam.Password != "" {
			realPwd, err := handler.DecryptPassword(reqParam.Password, g.RsaPrivateKey)
			if nil != err {
				return c.JSON(http.StatusOK, &models.ConnectionRespV2{
					BaseResp: models.BuildBaseResp(err),
				})
			}
			reqParam.Password = realPwd
		}
		oracleDb, err := config.NewDB(&config.OracleConfig{
			User:        reqParam.User,
			Password:    reqParam.Password,
			Host:        reqParam.Host,
			Port:        reqParam.Port,
			ServiceName: reqParam.ServiceName,
		})
		if err != nil {
			return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("new oracle db err %v", err)))
		}
		defer oracleDb.Close()
		columns, err = oracleDb.GetColumns(reqParam.Schema, reqParam.Table)
		if err != nil {
			return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("Get schema %v table %v columns  err : %v", reqParam.Schema, reqParam.Table, err)))
		}
	}

	return c.JSON(http.StatusOK, &models.ListColumnsRespV2{
		Columns:  columns,
		BaseResp: models.BuildBaseResp(nil),
	})
}

func listMySQLColumns(logger hclog.Logger, reqParam *models.ListColumnsReqV2) ([]string, error) {
	uri, err := buildMysqlUri(reqParam.Host, reqParam.User, reqParam.Password,
		reqParam.CharacterSet, int(reqParam.Port), reqParam.IsPasswordEncrypted)
	if err != nil {
		return nil, fmt.Errorf("build database Uri failed: %v", err)
	}

	db, err := sql.CreateDB(uri)
	if err != nil {
		return nil, fmt.Errorf("create db failed: %v", err)
	}
	defer db.Close()

	logger.Info("get columns")

	columns, err := sql.ListColumns(db, reqParam.Schema, reqParam.Table)
	if err != nil {
		return nil, fmt.Errorf("find db columns failed: %v", err)

	}
	return columns, nil
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

// @Id ConnectionV2
// @Description connect to  database instance.
// @Tags database
// @Security ApiKeyAuth
// @Param host query string true "database host"
// @Param port query int true "database port"
// @Param user query string true "database user"
// @Param password query string true "database password"
// @Param database_type query string true "database_type"
// @Param service_name query string false "database service_name"
// @Param is_password_encrypted query bool false "indecate that database password is encrypted or not"
// @Success 200 {object} models.ConnectionRespV2
// @Router /v2/database/instance_connection [get]
func ConnectionV2(c echo.Context) error {
	logger := handler.NewLogger().Named("ConnectionV2")
	reqParam := new(models.ConnectionReqV2)
	if err := handler.BindAndValidate(logger, c, reqParam); err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	err := connectDatabase(reqParam)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}
	return c.JSON(http.StatusOK, &models.ConnectionRespV2{
		BaseResp: models.BuildBaseResp(nil),
	})
}

func connectDatabase(reqParam *models.ConnectionReqV2) error {
	switch reqParam.DatabaseType {
	case DB_TYPE_MYSQL:
		uri, err := buildMysqlUri(reqParam.Host, reqParam.User, reqParam.Password,
			"", int(reqParam.Port), reqParam.IsPasswordEncrypted)
		if err != nil {
			return fmt.Errorf("build database Uri failed: %v", err)
		}
		db, err := sql.CreateDB(uri)
		if err != nil {
			return fmt.Errorf("open db failed: %v", err)
		}
		defer db.Close()

		err = db.Ping()
		if err != nil {
			return err
		}
	case DB_TYPE_ORACLE:
		if reqParam.IsPasswordEncrypted && reqParam.Password != "" {
			realPwd, err := handler.DecryptPassword(reqParam.Password, g.RsaPrivateKey)
			if nil != err {
				return err
			}
			reqParam.Password = realPwd
		}
		oracleDb, err := config.NewDB(&config.OracleConfig{
			User:        reqParam.User,
			Password:    reqParam.Password,
			Host:        reqParam.Host,
			Port:        reqParam.Port,
			ServiceName: reqParam.ServiceName,
		})
		if err != nil {
			return err
		}
		defer oracleDb.Close()
	default:
		return fmt.Errorf("data type %v is unsupport", reqParam.DatabaseType)
	}

	return nil
}
