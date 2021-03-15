package route

import (
	"net/http"

	"github.com/NYTimes/gziphandler"
	metrics "github.com/armon/go-metrics"
	"github.com/armon/go-metrics/prometheus"
	hclog "github.com/hashicorp/go-hclog"
	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func SetupApiServer(logger hclog.Logger, apiAddr, nomadAddr, uiDir string) (err error) {
	NomadHost = nomadAddr
	logger.Debug("Begin Setup api server", "addr", apiAddr)
	router := httprouter.New()
	router.GET("/v1/job/:jobId", JobDetailRequest)
	router.DELETE("/v1/job/:jobId", JobDeleteRequest)
	router.GET("/v1/job/:jobId/:path", JobRequest)
	router.POST("/v1/jobs", UpdupJob)
	router.GET("/v1/jobs", JobListRequest)
	router.GET("/v1/allocations", AllocsRequest)
	router.GET("/v1/allocation/:allocID", AllocSpecificRequest)
	router.GET("/v1/evaluations", EvalsRequest)
	router.GET("/v1/evaluation/:evalID/:type", EvalRequest)
	router.GET("/v1/agent/allocation/:tokens", ClientAllocRequest)
	router.GET("/v1/self", AgentSelfRequest)
	router.POST("/v1/join", AgentJoinRequest)
	router.POST("/v1/agent/force-leave", AgentForceLeaveRequest)
	router.GET("/v1/members", AgentMembersRequest)
	router.POST("/v1/managers", UpdateServers)
	router.GET("/v1/managers", ListServers)
	router.GET("/v1/regions", RegionListRequest)
	router.GET("/v1/leader", StatusLeaderRequest)
	router.GET("/v1/peers", StatusPeersRequest)
	router.POST("/v1/validate/job", ValidateJobRequest)
	router.GET("/v1/nodes", NodesRequest)
	router.GET("/v1/node/:nodeName/:type", NodeRequest)
	//router.POST("/v1/operator/",updupJob)
	/*router.POST("/v1/job/renewal",updupJob)
	router.POST("/v1/job/info",updupJob)
	*/

	mux := http.NewServeMux()
	mux.Handle("/v1/", router)

	if uiDir != "" {
		logger.Info("found ui_dir", "dir", uiDir)
		mux.Handle("/", http.StripPrefix("/", http.FileServer(http.Dir(uiDir))))
	}
	go func() {
		err := http.ListenAndServe(apiAddr, gziphandler.GzipHandler(mux))
		if err != nil {
			logger.Error("in SetupApiServer ListenAndServe", "err", err)
			// TODO mark plugin unhealthy
		}
	}()
	logger.Info("Setup api server succeeded", "addr", apiAddr)

	router.Handler("GET", "/metrics", promhttp.Handler())

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
