package v2

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/actiontech/dtle/g"
	hclog "github.com/hashicorp/go-hclog"
	"github.com/labstack/echo/v4"
	"github.com/shirou/gopsutil/process"
)

type UpdataLogLevelRespV2 struct {
	Message      string `json:"message"`
	DtleLogLevel string `json:"dtle_log_level"`
}

// @Description reload log level dynamically.
// @Tags log
// @accept application/x-www-form-urlencoded
// @Param dtle_log_level formData string false "dtle log level" Enums(TRACE, DEBUG, INFO, WARN, ERROR)
// @Success 200 {object} UpdataLogLevelRespV2
// @router /v2/log_level [post]
func UpdateLogLevelV2(c echo.Context) error {
	// verify
	logLevelStr := c.FormValue("dtle_log_level")
	logLevel := hclog.LevelFromString(logLevelStr)
	if logLevel == hclog.NoLevel {
		return c.String(http.StatusInternalServerError, fmt.Sprintf("dtle log level should be one of these value[\"TRACE\",\"DEBUG\",\"INFO\",\"WARN\",\"ERROR\"], got %v", logLevelStr))
	}

	// reload nomad log level
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
	g.Logger.SetLevel(logLevel)
	g.Logger.Info("update log level", "dtle log_level", logLevelStr)

	return c.JSON(http.StatusOK, &UpdataLogLevelRespV2{
		Message:      "reload log level successfully",
		DtleLogLevel: strings.ToUpper(logLevelStr),
	})
}
