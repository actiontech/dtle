package v2

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strconv"

	"github.com/actiontech/dtle/g"
	hclog "github.com/hashicorp/go-hclog"
	"github.com/labstack/echo/v4"
	"github.com/shirou/gopsutil/process"
)

type UpdataLogLevelResp struct {
	Message      string `json:"message"`
	DtleLogLevel string `json:"dtle_log_level"`
}

func UpdateLogLevelV2(c echo.Context) error {
	// verify
	logLevelStr := c.QueryParam("dtle_log_level")
	logLevel := hclog.LevelFromString(logLevelStr)
	if logLevel == hclog.NoLevel {
		return c.String(http.StatusInternalServerError, fmt.Sprintf("dtle log level should be one of these value[\"trace\",\"debug\",\"info\",\"warn\",\"error\"], got %v", logLevelStr))
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
	logger.Info("update log level", "dtle log_level", logLevelStr)

	return c.JSON(http.StatusOK, &UpdataLogLevelResp{
		Message:      "reload log level successfully",
		DtleLogLevel: logLevelStr,
	})
}
