package v2

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/actiontech/dtle/drivers/api/handler"

	"github.com/actiontech/dtle/drivers/api/models"

	"github.com/actiontech/dtle/g"
	hclog "github.com/hashicorp/go-hclog"
	"github.com/labstack/echo/v4"
	"github.com/shirou/gopsutil/v3/process"
)

// @Id UpdateLogLevelV2
// @Description reload log level dynamically.
// @Tags log
// @accept application/x-www-form-urlencoded
// @Security ApiKeyAuth
// @Param dtle_log_level formData string true "dtle log level" Enums(TRACE, DEBUG, INFO, WARN, ERROR)
// @Success 200 {object} models.UpdataLogLevelRespV2
// @router /v2/log/level [post]
func UpdateLogLevelV2(c echo.Context) error {
	logger := handler.NewLogger().Named("UpdateLogLevelV2")

	reqParam := new(models.UpdataLogLevelReqV2)
	if err := handler.BindAndValidate(logger, c, reqParam); err != nil {
		return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(err))
	}

	// verify
	logLevelStr := reqParam.DtleLogLevel
	logLevel := hclog.LevelFromString(logLevelStr)
	if logLevel == hclog.NoLevel {
		return c.String(http.StatusInternalServerError, fmt.Sprintf("dtle log level should be one of these value[\"TRACE\",\"DEBUG\",\"INFO\",\"WARN\",\"ERROR\"], got %v", logLevelStr))
	}

	// reload nomad log level
	logger.Info("reload nomad log level")
	ppid := os.Getppid()
	p, err := process.NewProcess(int32(ppid))
	if nil != err {
		return c.String(http.StatusInternalServerError, fmt.Sprintf("failed to get parent(pid=%v) process info: %v", ppid, err))
	}
	pn, err := p.Name()
	if nil != err {
		return c.String(http.StatusInternalServerError, fmt.Sprintf("failed to get parent(pid=%v) process name: %v", ppid, err))
	}

	if pn != "nomad" {
		return c.String(http.StatusInternalServerError, fmt.Sprintf("canot reload log level because the parent(pid=%v) process is not nomad", ppid))
	}

	cmd := exec.Command("kill", "-SIGHUP", strconv.Itoa(ppid))
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); nil != err {
		return c.String(http.StatusInternalServerError, fmt.Sprintf("failed to reload log level of nomad(pid=%v): %v stderr: %v", ppid, err, stderr.String()))
	}

	// reload dtle log level
	logger.Info("reload dtle log level")
	g.Logger.SetLevel(logLevel)

	logger.Info("update log level", "dtle log_level", logLevelStr)

	return c.JSON(http.StatusOK, &models.UpdataLogLevelRespV2{
		DtleLogLevel: strings.ToUpper(logLevelStr),
		BaseResp:     models.BuildBaseResp(nil),
	})
}
