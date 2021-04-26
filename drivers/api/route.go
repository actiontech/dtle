package api

import (
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

func SetupApiServer(logger hclog.Logger, apiAddr, nomadAddr, uiDir string) (err error) {
	e := echo.New()
	handler.NomadHost = nomadAddr
	logger.Debug("Begin Setup api server", "addr", apiAddr)

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
	e.POST("/v2/log_level", v2.UpdateLogLevelV2)
	e.GET("/v2/jobs", v2.JobListV2)
	e.GET("/v2/job/detail", v2.GetJobDetailV2)
	e.POST("/v2/job/migration", v2.CreateOrUpdateMigrationJobV2)
	e.GET("/v2/nodes", v2.NodeListV2)
	e.POST("/v2/validation/job", v2.ValidateJobV2)
	e.GET("/v2/database/schemas", v2.ListDatabaseSchemasV2)

	if uiDir != "" {
		logger.Info("found ui_dir", "dir", uiDir)
		e.Static("/", uiDir)
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
