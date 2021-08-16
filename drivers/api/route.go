package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"strings"

	"github.com/actiontech/dtle/drivers/mysql"

	"github.com/actiontech/dtle/drivers/api/models"

	middleware "github.com/labstack/echo/v4/middleware"

	"github.com/actiontech/dtle/drivers/mysql/common"

	_ "github.com/actiontech/dtle/drivers/api/docs"
	"github.com/actiontech/dtle/drivers/api/handler"
	v1 "github.com/actiontech/dtle/drivers/api/handler/v1"
	v2 "github.com/actiontech/dtle/drivers/api/handler/v2"
	metrics "github.com/armon/go-metrics"
	"github.com/armon/go-metrics/prometheus"
	hclog "github.com/hashicorp/go-hclog"
	"github.com/labstack/echo/v4"
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

func SetupApiServer(logger hclog.Logger, driverConfig *mysql.DriverConfig) (err error) {
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
	e.POST("/v2/login", v2.Login)
	e.POST("/v2/login/captcha", v2.CaptchaV2)
	v2Router.POST("/log/level", v2.UpdateLogLevelV2, AdminUserAllowed())
	v2Router.GET("/jobs", v2.JobListV2)
	v2Router.GET("/job/migration/detail", v2.GetMigrationJobDetailV2)
	v2Router.POST("/job/migration", v2.CreateOrUpdateMigrationJobV2)
	v2Router.GET("/job/sync/detail", v2.GetSyncJobDetailV2)
	v2Router.POST("/job/sync", v2.CreateOrUpdateSyncJobV2)
	v2Router.GET("/job/subscription/detail", v2.GetSubscriptionJobDetailV2)
	v2Router.POST("/job/subscription", v2.CreateOrUpdateSubscriptionJobV2)
	v2Router.POST("/job/pause", v2.PauseJobV2)
	v2Router.POST("/job/resume", v2.ResumeJobV2)
	v2Router.POST("/job/delete", v2.DeleteJobV2)
	v2Router.GET("/nodes", v2.NodeListV2)
	v2Router.POST("/validation/job", v2.ValidateJobV2)
	v2Router.GET("/mysql/schemas", v2.ListMysqlSchemasV2)
	v2Router.GET("/mysql/columns", v2.ListMysqlColumnsV2)
	v2Router.GET("/mysql/instance_connection", v2.Connection)
	v2Router.GET("/monitor/task", v2.GetTaskProgressV2)
	v2Router.GET("/job/gtid", v2.GetJobGtid)
	v2Router.POST("/job/reverse_start", v2.ReverseStartJob)
	v2Router.POST("/job/reverse", v2.ReverseJob)
	v2Router.GET("/user/list", v2.UserList)
	v2Router.POST("/user/create", v2.CreateUser)
	v2Router.POST("/user/update", v2.UpdateUser)
	v2Router.POST("/user/reset_password", v2.ResetPassword)
	v2Router.POST("/user/delete", v2.DeleteUser)
	v2Router.GET("/tenant/list", v2.TenantList)
	v2Router.GET("/user/current_user", v2.GetCurrentUser)
	v2Router.GET("/user/list_action", v2.ListAction)
	v2Router.GET("/role/list", v2.RoleList)
	v2Router.POST("/role/create", v2.CreateRole, AdminUserAllowed())
	v2Router.POST("/role/delete", v2.DeleteRole, AdminUserAllowed())
	v2Router.POST("/role/update", v2.UpdateRole, AdminUserAllowed())

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
			authority := make(map[string][]models.ActionItem, 0)
			err = json.Unmarshal([]byte(role.Authority), &authority)
			if err != nil {
				return echo.NewHTTPError(http.StatusForbidden, "please check your authority")
			}
			for _, actionItems := range authority {
				for _, item := range actionItems {
					if c.Request().URL.Path == item.Uri {
						return next(c)
					}
				}
			}
			return echo.NewHTTPError(http.StatusForbidden)
		}
	}
}

//AdminUserAllowed is a `echo` middleware, only allow admin user to access next.
func AdminUserAllowed() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			logger := handler.NewLogger().Named("ResumeJobV2")
			tenant, user := v2.GetUserName(c)
			storeManager, err := common.NewStoreManager([]string{handler.ConsulAddr}, logger)
			if err != nil {
				return c.JSON(http.StatusInternalServerError, models.BuildBaseResp(fmt.Errorf("consul_addr=%v; connect to consul failed: %v", handler.ConsulAddr, err)))
			}
			role, _, _ := storeManager.GetRole(tenant, user)
			if role != nil && role.Name == common.DefaultAdminUser {
				return next(c)
			}
			return echo.NewHTTPError(http.StatusForbidden)
		}
	}
}

func init() {
	middleware.ErrJWTMissing.Code = http.StatusUnauthorized
	middleware.ErrJWTMissing.Message = "permission denied,please login again!"
	middleware.ErrJWTInvalid.Message = "permission denied,please login again!"
}
