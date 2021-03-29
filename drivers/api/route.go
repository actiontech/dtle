package api

import (
	"github.com/actiontech/dtle/drivers/api/handler"
	"github.com/actiontech/dtle/drivers/api/handler/v1"
	v2 "github.com/actiontech/dtle/drivers/api/handler/v2"
	metrics "github.com/armon/go-metrics"
	"github.com/armon/go-metrics/prometheus"
	hclog "github.com/hashicorp/go-hclog"
	"github.com/labstack/echo/v4"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func SetupApiServer(logger hclog.Logger, apiAddr, nomadAddr, uiDir string) (err error) {
	e := echo.New()
	handler.NomadHost = nomadAddr
	logger.Debug("Begin Setup api server", "addr", apiAddr)
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
	e.GET("/metrics", echo.WrapHandler(promhttp.Handler()))
	//router.POST("/v1/operator/",updupJob)
	/*router.POST("/v1/job/renewal",updupJob)
	router.POST("/v1/job/info",updupJob)
	*/

	e.POST("/v2/log_level", v2.UpdateLogLevelV2)

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
