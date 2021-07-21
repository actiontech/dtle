package api

import (
	"fmt"
	"net/http"
	"path"
	"strings"

	middleware "github.com/labstack/echo/v4/middleware"

	"github.com/actiontech/dtle/drivers/mysql/common"

	"github.com/dgrijalva/jwt-go"

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

func SetupApiServer(logger hclog.Logger, apiAddr, nomadAddr, consulAddr, uiDir string) (err error) {
	logger.Debug("Begin Setup api server", "addr", apiAddr)
	e := echo.New()

	// adapt to stdout
	e.StdLogger = handler.NewLogger().StandardLogger(&hclog.StandardLoggerOptions{
		InferLevels: false,
		ForceLevel:  hclog.Debug,
	})

	handler.NomadHost = nomadAddr
	handler.ApiAddr = apiAddr
	handler.ConsulAddr = consulAddr

	e.GET("/swagger/*", echoSwagger.WrapHandler)

	// api v1
	e.GET("/v1/job/:jobId", v1.JobDetailRequest)
	e.DELETE("/v1/job/:jobId", v1.JobDeleteRequest)
	e.GET("/v1/job/:jobId/:path", v1.JobRequest)
	e.POST("/v1/jobs", v1.UpdupJob)
	e.GET("/v1/jobs", v1.JobListRequest)
	e.GET("/v1/allocations", v1.AllocsRequest)
	e.GET("/v1/allocation/:allocID", v1.AllocSpecificRequest)
	e.GET("/v1/evaluations", v1.EvalsRequest)
	e.GET("/v1/evaluation/:evalID/:type", v1.EvalRequest)
	e.GET("/v1/agent/allocation/:tokens", v1.ClientAllocRequest)
	e.GET("/v1/self", v1.AgentSelfRequest)
	e.POST("/v1/join", v1.AgentJoinRequest)
	e.POST("/v1/agent/force-leave", v1.AgentForceLeaveRequest)
	e.GET("/v1/members", v1.AgentMembersRequest)
	e.POST("/v1/managers", v1.UpdateServers)
	e.GET("/v1/managers", v1.ListServers)
	e.GET("/v1/regions", v1.RegionListRequest)
	e.GET("/v1/leader", v1.StatusLeaderRequest)
	e.GET("/v1/peers", v1.StatusPeersRequest)
	e.POST("/v1/validate/job", v1.ValidateJobRequest)
	e.GET("/v1/nodes", v1.NodesRequest)
	e.GET("/v1/node/:nodeName/:type", v1.NodeRequest)

	// api v2
	v2Router := e.Group("/v2")
	v2Router.Use(JWTTokenAdapter(), middleware.JWT([]byte(common.JWTSecret)))
	e.POST("/v2/login", v2.Login)
	v2Router.POST("/log/level", v2.UpdateLogLevelV2)
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
	v2Router.GET("/nodes", v2.NodeListV2, AdminUserAllowed())
	v2Router.POST("/validation/job", v2.ValidateJobV2)
	v2Router.GET("/mysql/schemas", v2.ListMysqlSchemasV2)
	v2Router.GET("/mysql/columns", v2.ListMysqlColumnsV2)
	v2Router.GET("/monitor/task", v2.GetTaskProgressV2)
	v2Router.GET("/job/gtid", v2.GetJobGtid)
	v2Router.POST("/job/finish", v2.FinishJob)
	v2Router.POST("/job/reverse", v2.ReverseJob)
	v2Router.GET("/user/list", v2.UserList)
	v2Router.POST("/user/update", v2.CreateOrUpdateUser)
	v2Router.POST("/user/delete", v2.DeleteUser)

	// for pprof
	e.GET("/debug/pprof/*", echo.WrapHandler(http.DefaultServeMux))
	e.Validator = handler.NewValidator()

	if uiDir != "" {
		logger.Info("found ui_dir", "dir", uiDir)
		e.File("/", path.Join(uiDir, "index.html"))
		e.Static("/static", path.Join(uiDir, "static"))
		e.File("/favicon.png", path.Join(uiDir, "favicon.png"))
		e.GET("/*", func(c echo.Context) error {
			return c.File(path.Join(uiDir, "index.html"))
		})
	}
	go func() {
		err := e.Start(apiAddr)
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
	_, err = metrics.NewGlobal(metricsConfig, sink)
	if err != nil {
		return err
	}

	return nil
}

func GetUserName(c echo.Context) (string, string) {
	user := c.Get("user").(*jwt.Token)
	claims := user.Claims.(jwt.MapClaims)
	return claims["group"].(string), claims["name"].(string)
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

//AdminUserAllowed is a `echo` middleware, only allow admin user to access next.
func AdminUserAllowed() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			userGroup, user := GetUserName(c)
			if userGroup == common.DefaultAdminGroup && user == common.DefaultAdminUser {
				return next(c)
			}
			return echo.NewHTTPError(http.StatusForbidden)
		}
	}
}
