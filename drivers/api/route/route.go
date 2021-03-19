package route

import (
	metrics "github.com/armon/go-metrics"
	"github.com/armon/go-metrics/prometheus"
	hclog "github.com/hashicorp/go-hclog"
	"github.com/labstack/echo/v4"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func SetupApiServer(logger hclog.Logger, apiAddr, nomadAddr, uiDir string) (err error) {
	e := echo.New()
	NomadHost = nomadAddr
	logger.Debug("Begin Setup api server", "addr", apiAddr)
	e.GET("/v1/job/:jobId", JobDetailRequest)
	e.DELETE("/v1/job/:jobId", JobDeleteRequest)
	e.GET("/v1/job/:jobId/:path", JobRequest)
	e.POST("/v1/jobs", UpdupJob)
	e.GET("/v1/jobs", JobListRequest)
	e.GET("/v1/allocations", AllocsRequest)
	e.GET("/v1/allocation/:allocID", AllocSpecificRequest)
	e.GET("/v1/evaluations", EvalsRequest)
	e.GET("/v1/evaluation/:evalID/:type", EvalRequest)
	e.GET("/v1/agent/allocation/:tokens", ClientAllocRequest)
	e.GET("/v1/self", AgentSelfRequest)
	e.POST("/v1/join", AgentJoinRequest)
	e.POST("/v1/agent/force-leave", AgentForceLeaveRequest)
	e.GET("/v1/members", AgentMembersRequest)
	e.POST("/v1/managers", UpdateServers)
	e.GET("/v1/managers", ListServers)
	e.GET("/v1/regions", RegionListRequest)
	e.GET("/v1/leader", StatusLeaderRequest)
	e.GET("/v1/peers", StatusPeersRequest)
	e.POST("/v1/validate/job", ValidateJobRequest)
	e.GET("/v1/nodes", NodesRequest)
	e.GET("/v1/node/:nodeName/:type", NodeRequest)
	e.GET("/metrics", echo.WrapHandler(promhttp.Handler()))
	//router.POST("/v1/operator/",updupJob)
	/*router.POST("/v1/job/renewal",updupJob)
	router.POST("/v1/job/info",updupJob)
	*/

	e.POST("/v2/log_level", UpdateLogLevelV2)

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
