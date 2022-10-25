package v2

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	mysql "github.com/actiontech/dtle/driver/mysql"

	"github.com/actiontech/dtle/api/handler"
	"github.com/actiontech/dtle/api/models"
	"github.com/actiontech/dtle/driver/common"
	"github.com/actiontech/dtle/g"
	nomadApi "github.com/hashicorp/nomad/api"
	"github.com/labstack/echo/v4"
	"github.com/mitchellh/mapstructure"
)

// @Id ValidateJobV2
// @Description validate job config.
// @Tags validation
// @Accept application/json
// @Security ApiKeyAuth
// @Param job_config body models.ValidateJobReqV2 true "validate job config"
// @Success 200 {object} models.ValidateJobRespV2
// @Router /v2/validation/job [post]
func ValidateJobV2(c echo.Context) error {
	logger := handler.NewLogger().Named("ValidateJobV2")
	reqParam := new(models.ValidateJobReqV2)
	if err := handler.BindAndValidate(logger, c, reqParam); err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}

	reqJson, err := apiJobConfigToNomadJobJson(reqParam)
	if nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("convert param failed: %v", err)))
	}

	url := handler.BuildUrl("/v1/validate/job")
	logger.Info("invoke nomad api begin", "url", url)
	nomadValidateResp := nomadApi.JobValidateResponse{}
	if err := handler.InvokePostApiWithJson(url, reqJson, &nomadValidateResp); nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("invoke nomad api %v failed: %v", url, err)))
	}
	logger.Info("invoke nomad api finished")
	logger.Info("validate task config")
	// decrypt mysql password
	if reqParam.IsPasswordEncrypted {
		err := decryptMySQLPwd(reqParam.SrcTaskConfig, reqParam.DestTaskConfig)
		if nil != err {
			return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
		}
	}

	validationTasks, err := validateTaskConfig(reqParam.SrcTaskConfig, reqParam.DestTaskConfig)
	if nil != err {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("validate task config failed: %v", err)))
	}

	return c.JSON(http.StatusOK, &models.ValidateJobRespV2{
		DriverConfigValidated: nomadValidateResp.DriverConfigValidated,
		MysqlValidationTasks:  validationTasks,
		JobValidationError:    nomadValidateResp.Error,
		JobValidationWarning:  nomadValidateResp.Warnings,
		BaseResp:              models.BuildBaseResp(nil),
	})
}

func apiJobConfigToNomadJobJson(apiJobConfig *models.ValidateJobReqV2) (resJson []byte, err error) {
	nomadJob, err := convertJobToNomadJob(true, &models.CreateOrUpdateMysqlToMysqlJobParamV2{JobId: apiJobConfig.JobId, SrcTask: apiJobConfig.SrcTaskConfig, DestTask: apiJobConfig.DestTaskConfig})
	if nil != err {
		return nil, fmt.Errorf("convert mysql-to-mysql job to nomad job struct faild: %v", err)
	}

	m := map[string]interface{}{
		"job": nomadJob,
	}
	resJson, err = json.Marshal(m)
	if nil != err {
		return nil, fmt.Errorf("marshal nomad job struct faild: %v", err)
	}

	return resJson, nil
}

func decryptMySQLPwd(apiSrcTask *models.SrcTaskConfig, apiDestTask *models.DestTaskConfig) (err error) {
	// decrypt mysql password
	apiSrcTask.ConnectionConfig.Password, err = handler.DecryptPassword(apiSrcTask.ConnectionConfig.Password, g.RsaPrivateKey)
	if nil != err {
		return fmt.Errorf("decrypt src mysql password failed: %v", err)
	}
	apiDestTask.ConnectionConfig.Password, err = handler.DecryptPassword(apiDestTask.ConnectionConfig.Password, g.RsaPrivateKey)
	if nil != err {
		return fmt.Errorf("decrypt src mysql password failed: %v", err)
	}
	return
}

func validateTaskConfig(apiSrcTask *models.SrcTaskConfig, apiDestTask *models.DestTaskConfig) ([]*models.MysqlTaskValidationReport, error) {
	taskValidationRes := []*models.MysqlTaskValidationReport{}
	srcTaskConfig := common.DtleTaskConfig{}
	// validate src task
	if apiSrcTask.MysqlSrcTaskConfig != nil {
		srcTaskMap := buildDatabaseSrcTaskConfigMap(apiSrcTask, apiDestTask, nil)
		if err := mapstructure.WeakDecode(srcTaskMap, &srcTaskConfig); err != nil {
			return nil, fmt.Errorf("convert src task config failed: %v", err)
		}

		validationRes := &models.MysqlTaskValidationReport{
			TaskName: apiSrcTask.TaskName,
		}

		srcTaskInspector := mysql.NewInspector(&common.MySQLDriverConfig{
			DtleTaskConfig: srcTaskConfig,
		}, g.Logger.Named("http api: validateTaskConfig"))
		defer srcTaskInspector.Close()

		if err := srcTaskInspector.InitDB(); nil != err {
			return nil, fmt.Errorf("init src task inspector failed: %v", err)
		}

		validationRes.ConnectionValidation = &models.ConnectionValidation{Validated: true}
		if err := srcTaskInspector.ValidateConnection(); nil != err {
			validationRes.ConnectionValidation.Error = err.Error()
			goto endSrcTaskValidation
		}

		validationRes.GtidModeValidation = &models.GtidModeValidation{Validated: true}
		if err := srcTaskInspector.ValidateGTIDMode(); nil != err {
			validationRes.GtidModeValidation.Error = err.Error()
		}

		validationRes.ServerIdValidation = &models.ServerIDValidation{Validated: true}
		if err := srcTaskInspector.ValidateServerId(); nil != err {
			validationRes.ServerIdValidation.Error = err.Error()
		}

		validationRes.BinlogValidation = &models.BinlogValidation{Validated: true}
		if err := srcTaskInspector.ValidateBinlogs(); nil != err {
			validationRes.BinlogValidation.Error = err.Error()
		}

		validationRes.PrivilegesValidation = &models.PrivilegesValidation{Validated: true}
		if err := srcTaskInspector.ValidateGrants(); nil != err {
			validationRes.PrivilegesValidation.Error = err.Error()
		}

	endSrcTaskValidation:
		taskValidationRes = append(taskValidationRes, validationRes)
	}
	// validate dest task
	{
		validationRes := &models.MysqlTaskValidationReport{
			TaskName: apiDestTask.TaskName,
		}
		destTaskInspector, err := mysql.NewApplier(
			&common.ExecContext{},
			&common.MySQLDriverConfig{
				DtleTaskConfig: srcTaskConfig, // #592: src sends the config to dest.
			},
			g.Logger.Named("http api: validateTaskConfig"),
			nil, "", nil, nil, nil, context.Background())
		if nil != err {
			return nil, fmt.Errorf("create dest task inspector failed: %v", err)
		}

		if err := destTaskInspector.InitDB(); nil != err {
			return nil, fmt.Errorf("init dest task inspector failed: %v", err)
		}
		defer destTaskInspector.Shutdown()

		validationRes.ConnectionValidation = &models.ConnectionValidation{Validated: true}
		if err := destTaskInspector.ValidateConnection(); nil != err {
			validationRes.ConnectionValidation.Error = err.Error()
			goto endDestTaskValidation
		}

		validationRes.PrivilegesValidation = &models.PrivilegesValidation{Validated: true}
		if err := destTaskInspector.ValidateGrants(); nil != err {
			validationRes.PrivilegesValidation.Error = err.Error()
		}

	endDestTaskValidation:
		taskValidationRes = append(taskValidationRes, validationRes)
	}

	return taskValidationRes, nil
}
