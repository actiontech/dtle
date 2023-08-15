package v2

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/actiontech/dtle/api/handler"
	"github.com/actiontech/dtle/api/models"
	"github.com/labstack/echo/v4"
)

// @Id UpdateLogLevelV2
// @Description reload log level dynamically.
// @Tags log
// @accept application/x-www-form-urlencoded
// @Security ApiKeyAuth
// @Param dtle_log_level formData string true "dtle log level" Enums(TRACE, DEBUG, INFO, WARN, ERROR)
// @Param dtle_debug_job formData string false "dtle debug job"
// @Success 200 {object} models.UpdataLogLevelRespV2
// @router /v2/log/level [post]
func UpdateLogLevelV2(c echo.Context) error {
	logger := handler.NewLogger().Named("UpdateLogLevelV2")

	reqParam := new(models.UpdataLogLevelReqV2)
	if err := handler.BindAndValidate(logger, c, reqParam); err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}

	err := handler.DtleDriver.SetLogLevel(reqParam.DtleLogLevel)
	if err != nil {
		return c.String(http.StatusInternalServerError, fmt.Sprintf("failed to set log level to %v. err: %v",
			reqParam.DtleLogLevel, err))
	}

	debugJob := handler.DtleDriver.ResetDebugJob(reqParam.DebugJob)

	return c.JSON(http.StatusOK, &models.UpdataLogLevelRespV2{
		DtleLogLevel: strings.ToUpper(reqParam.DtleLogLevel),
		DebugJob:     debugJob,
		BaseResp:     models.BuildBaseResp(nil),
	})
}
