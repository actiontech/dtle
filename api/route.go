package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"

	_ "github.com/actiontech/dtle/api/docs"
	"github.com/actiontech/dtle/api/handler"
	v1 "github.com/actiontech/dtle/api/handler/v1"
	v2 "github.com/actiontech/dtle/api/handler/v2"
	"github.com/actiontech/dtle/api/models"
	dtle "github.com/actiontech/dtle/driver"
	"github.com/actiontech/dtle/driver/common"
	"github.com/actiontech/dtle/g"
	report "github.com/actiontech/golang-live-coverage-report/pkg"

	metrics "github.com/armon/go-metrics"
	"github.com/armon/go-metrics/prometheus"
	hclog "github.com/hashicorp/go-hclog"
	"github.com/labstack/echo/v4"
	middleware "github.com/labstack/echo/v4/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	echoSwagger "github.com/swaggo/echo-swagger"
)

// @title dtle API Docs
// @version 2.0
// @description This is a sample server for dev.
// @securityDefinitions.apikey ApiKeyAuth
// @in header
// @name Authorization
// @BasePath /

func SetupApiServer(logger g.LoggerType, driverConfig *dtle.DriverConfig) (err error) {
	logger.Debug("Begin Setup api server", "addr", driverConfig.ApiAddr)
	e := echo.New()

	// adapt to stdout
	e.StdLogger = handler.NewLogger().StandardLogger(&hclog.StandardLoggerOptions{
		InferLevels: false,
		ForceLevel:  hclog.Debug,
	})

	handler.NomadHost = driverConfig.NomadAddr
	handler.ApiAddr = driverConfig.ApiAddr
	handler.ConsulAddr = driverConfig.Consul

	e.GET("/swagger/*", echoSwagger.WrapHandler)

	// api v1
	v1Router := e.Group("/v1")
	v1Router.Use(JWTTokenAdapter(), middleware.JWT([]byte(common.JWTSecret)), AuthFilter())
	v1Router.GET("/job/:jobId", v1.JobDetailRequest)
	v1Router.DELETE("/job/:jobId", v1.JobDeleteRequest)
	v1Router.GET("/job/:jobId/:path", v1.JobRequest)
	v1Router.POST("/jobs", v1.UpdupJob)
	v1Router.GET("/jobs", v1.JobListRequest)
	v1Router.GET("/allocations", v1.AllocsRequest)
	v1Router.GET("/allocation/:allocID", v1.AllocSpecificRequest)
	v1Router.GET("/evaluations", v1.EvalsRequest)
	v1Router.GET("/evaluation/:evalID/:type", v1.EvalRequest)
	v1Router.GET("/agent/allocation/:tokens", v1.ClientAllocRequest)
	v1Router.GET("/self", v1.AgentSelfRequest)
	v1Router.POST("/join", v1.AgentJoinRequest)
	v1Router.POST("/agent/force-leave", v1.AgentForceLeaveRequest)
	v1Router.GET("/members", v1.AgentMembersRequest)
	v1Router.POST("/managers", v1.UpdateServers)
	v1Router.GET("/managers", v1.ListServers)
	v1Router.GET("/regions", v1.RegionListRequest)
	v1Router.GET("/leader", v1.StatusLeaderRequest)
	v1Router.GET("/peers", v1.StatusPeersRequest)
	v1Router.POST("/validate/job", v1.ValidateJobRequest)
	v1Router.GET("/nodes", v1.NodesRequest)
	v1Router.GET("/node/:nodeName/:type", v1.NodeRequest)

	// api v2
	v2Router := e.Group("/v2")
	v2Router.Use(JWTTokenAdapter(), middleware.JWT([]byte(common.JWTSecret)), AuthFilter())
	e.POST("/v2/login", v2.LoginV2)
	e.POST("/v2/loginWithoutVerifyCode", v2.LoginWithoutVerifyCodeV2)
	e.POST("/v2/login/captcha", v2.CaptchaV2)
	e.GET("/v2/monitor/task", v2.GetTaskProgressV2)
	e.POST("/v2/log/level", v2.UpdateLogLevelV2)
	v2Router.GET("/jobs/migration", v2.MigrationJobListV2)
	v2Router.GET("/job/migration/detail", v2.GetMigrationJobDetailV2)
	v2Router.POST("/job/migration/create", v2.CreateMigrationJobV2)
	v2Router.POST("/job/migration/update", v2.UpdateMigrationJobV2)
	v2Router.POST("/job/migration/reverse", v2.ReverseMigrationJobV2)
	v2Router.POST("/job/migration/pause", v2.PauseMigrationJobV2)
	v2Router.POST("/job/migration/resume", v2.ResumeMigrationJobV2)
	v2Router.POST("/job/migration/delete", v2.DeleteMigrationJobV2)
	v2Router.POST("/job/migration/reverse_start", v2.ReverseStartMigrationJobV2)

	v2Router.GET("/jobs/sync", v2.SyncJobListV2)
	v2Router.GET("/job/sync/detail", v2.GetSyncJobDetailV2)
	v2Router.POST("/job/sync/create", v2.CreateSyncJobV2)
	v2Router.POST("/job/sync/update", v2.UpdateSyncJobV2)
	v2Router.POST("/job/sync/reverse", v2.ReverseSyncJobV2)
	v2Router.POST("/job/sync/pause", v2.PauseSyncJobV2)
	v2Router.POST("/job/sync/resume", v2.ResumeSyncJobV2)
	v2Router.POST("/job/sync/delete", v2.DeleteSyncJobV2)
	v2Router.POST("/job/sync/reverse_start", v2.ReverseStartSyncJobV2)

	v2Router.GET("/jobs/subscription", v2.SubscriptionJobListV2)
	v2Router.GET("/job/subscription/detail", v2.GetSubscriptionJobDetailV2)
	v2Router.POST("/job/subscription/create", v2.CreateSubscriptionJobV2)
	v2Router.POST("/job/subscription/update", v2.UpdateSubscriptionJobV2)
	v2Router.POST("/job/subscription/pause", v2.PauseSubscriptionJobV2)
	v2Router.POST("/job/subscription/resume", v2.ResumeSubscriptionJobV2)
	v2Router.POST("/job/subscription/delete", v2.DeleteSubscriptionJobV2)

	v2Router.GET("/nodes", v2.NodeListV2)
	v2Router.POST("/validation/job", v2.ValidateJobV2)
	v2Router.GET("/database/schemas", v2.ListDatabaseSchemasV2)
	v2Router.GET("/database/columns", v2.ListDatabaseColumnsV2)
	v2Router.GET("/database/instance_connection", v2.ConnectionV2)
	v2Router.GET("/job/gtid", v2.GetJobGtidV2)
	v2Router.GET("/user/list", v2.UserListV2)
	v2Router.POST("/user/create", v2.CreateUserV2)
	v2Router.POST("/user/update", v2.UpdateUserV2)
	v2Router.POST("/user/reset_password", v2.ResetPasswordV2)
	v2Router.POST("/user/delete", v2.DeleteUserV2)
	v2Router.GET("/tenant/list", v2.TenantListV2)
	v2Router.GET("/user/current_user", v2.GetCurrentUserV2)
	v2Router.GET("/user/list_action", v2.ListActionV2)
	v2Router.GET("/role/list", v2.RoleListV2)
	v2Router.POST("/role/create", v2.CreateRoleV2)
	v2Router.POST("/role/delete", v2.DeleteRoleV2)
	v2Router.POST("/role/update", v2.UpdateRoleV2)

	// Deprecated
	v2Router.GET("/mysql/schemas", v2.ListMysqlSchemasV2)
	v2Router.GET("/mysql/columns", v2.ListMysqlColumnsV2)
	v2Router.GET("/mysql/instance_connection", v2.MySQLConnectionV2)

	// for coverage report
	e.GET("/coverage_report", echo.WrapHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
		if err != nil {
			logger.Error("get current path err", "err :", err)
		}
		coverageReportRawCodeDir := strings.Replace(dir, "\\", "/", -1)
		err = report.GenerateHtmlReport2(w, coverageReportRawCodeDir)
		if nil != err {
			logger.Error("generate failed ", "err : ", err)
		}
	})))
	// for pprof
	e.GET("/debug/pprof/*", echo.WrapHandler(http.DefaultServeMux))
	e.Validator = handler.NewValidator()

	uiDir := driverConfig.UiDir
	if uiDir != "" {
		logger.Info("found ui_dir", "dir", uiDir)
		e.File("/", path.Join(uiDir, "index.html"))
		e.Static("/static", path.Join(uiDir, "static"))
		e.File("/favicon.png", path.Join(uiDir, "favicon.png"))
		e.GET("/*", func(c echo.Context) error {
			return c.File(path.Join(uiDir, "index.html"))
		})
	}

	apiAddr := driverConfig.ApiAddr
	go func() {
		var err error
		if driverConfig.CertFilePath != "" && driverConfig.KeyFilePath != "" {
			err = e.StartTLS(apiAddr, driverConfig.CertFilePath, driverConfig.KeyFilePath)
		} else {
			err = e.Start(apiAddr)
		}
		if err != nil {
			logger.Error("in SetupApiServer ListenAndServe", "err", err)
			// TODO mark plugin unhealthy
		}
	}()
	logger.Info("Setup api server succeeded", "addr", apiAddr)

	//d.apiServer = router
	e.GET("/metrics", echo.WrapHandler(promhttp.Handler()))
	sink, err := prometheus.NewPrometheusSink()
	if err != nil {
		return err
	}

	metricsConfig := metrics.DefaultConfig("dtle")

	// adding hostname to labels, and don't set it as a prefix to gauge values.
	metricsConfig.EnableHostname = false
	metricsConfig.EnableHostnameLabel = true

	_, err = metrics.NewGlobal(metricsConfig, sink)
	if err != nil {
		return err
	}

	return nil
}

// JWTTokenAdapter is a `echo` middleware,　by rewriting the header, the jwt token support header
// "Authorization: {token}" and "Authorization: Bearer {token}".
func JWTTokenAdapter() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			auth := c.Request().Header.Get(echo.HeaderAuthorization)
			if auth != "" && !strings.HasPrefix(auth, middleware.DefaultJWTConfig.AuthScheme) {
				c.Request().Header.Set(echo.HeaderAuthorization,
					fmt.Sprintf("%s %s", middleware.DefaultJWTConfig.AuthScheme, auth))
			}
			return next(c)
		}
	}
}

func AuthFilter() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			if inWhiteList(c.Request().URL.Path) {
				return next(c)
			}
			logger := handler.NewLogger().Named("AuthFilter")
			storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
			if err != nil {
				return echo.NewHTTPError(http.StatusForbidden)
			}
			user, exists, err := storeManager.GetUser(v2.GetUserName(c))
			if err != nil || !exists {
				return echo.NewHTTPError(http.StatusForbidden)
			}

			// admin  has the maximum authority
			if user.Role == common.DefaultRole {
				return next(c)
			}
			role, exists, err := storeManager.GetRole(user.Tenant, user.Role)
			if err != nil || !exists {
				return echo.NewHTTPError(http.StatusForbidden)
			}
			authority := make([]models.MenuItem, 0)
			err = json.Unmarshal([]byte(role.Authority), &authority)
			if err != nil {
				return echo.NewHTTPError(http.StatusForbidden, fmt.Sprintf("check your authority fail : %v", err))
			}
			for _, menuItem := range authority {
				for _, buttonItem := range menuItem.Operations {
					if c.Request().URL.Path == buttonItem.Uri {
						return next(c)
					}
				}
			}
			return echo.NewHTTPError(http.StatusForbidden)
		}
	}
}

var whiteList = []string{
	"/v2/nodes",
	"/v2/validation/job",
	"/v2/database/schemas",
	"/v2/database/columns",
	"/v2/database/instance_connection",
	"/v2/mysql/schemas",
	"/v2/mysql/columns",
	"/v2/mysql/instance_connection",
	"/v2/job/position",
	"/v2/user/list_action",
	"/v2/user/current_user",
	"/v2/user/reset_password",
}

func inWhiteList(uri string) bool {
	for _, whiteUri := range whiteList {
		if uri == whiteUri {
			return true
		}
	}
	return false
}
func init() {
	middleware.ErrJWTMissing.Code = http.StatusUnauthorized
	middleware.ErrJWTMissing.Message = "permission denied,please login again!"
	middleware.ErrJWTInvalid.Message = "permission denied,please login again!"
}
