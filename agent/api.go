package agent

import (
	"net"
	"net/http"

	"github.com/NYTimes/gziphandler"
	"github.com/ngaut/log"
	"github.com/gorilla/mux"
	"encoding/json"
	"fmt"
)

func (a *Agent) ServeHTTP() {
	// Start the listener
	lnAddr, err := net.ResolveTCPAddr("tcp", a.config.HTTPAddr)
	if err != nil {
		log.Infof("failed to start HTTP listener: %v", err)
	}
	ln, err := a.config.Listener("tcp", lnAddr.IP.String(), lnAddr.Port)
	if err != nil {
		log.Infof("failed to start HTTP listener: %v", err)
	}

	// Create the mux
	mux := http.NewServeMux()
	// Start the server
	go http.Serve(ln, gziphandler.GzipHandler(mux))
}

func (a *Agent) apiRoutes(r *mux.Router) {
	subver := r.PathPrefix("/v1").Subrouter()
	subver.HandleFunc("/", a.indexHandler)
	subver.HandleFunc("/members", a.membersHandler)
	subver.HandleFunc("/leader", a.leaderHandler)
	subver.HandleFunc("/leave", a.leaveHandler).Methods(http.MethodGet, http.MethodPost)
}

func printJson(w http.ResponseWriter, r *http.Request, v interface{}) error {
	if _, ok := r.URL.Query()["pretty"]; ok {
		j, _ := json.MarshalIndent(v, "", "\t")
		if _, err := fmt.Fprintf(w, string(j)); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return err
		}
	} else {
		if err := json.NewEncoder(w).Encode(v); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return err
		}
	}

	return nil
}

func (a *Agent) indexHandler(w http.ResponseWriter, r *http.Request) {
	local := a.serf.LocalMember()
	stats := map[string]map[string]string{
		"agent": {
			"name":    local.Name,
			"version": a.config.Version,
			"backend": backend,
		},
		"serf": a.serf.Stats(),
		"tags": local.Tags,
	}

	if err := printJson(w, r, stats); err != nil {
		log.Fatal(err)
	}
}

func (a *Agent) membersHandler(w http.ResponseWriter, r *http.Request) {
	if err := printJson(w, r, a.serf.Members()); err != nil {
		log.Fatal(err)
	}
}

func (a *Agent) leaderHandler(w http.ResponseWriter, r *http.Request) {
	member, err := a.leaderMember()
	if err == nil {
		if err := printJson(w, r, member); err != nil {
			log.Fatal(err)
		}
	}
}

func (a *Agent) leaveHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		log.Warn("/leave GET is deprecated and will be removed, use POST")
	}
	if err := a.serf.Leave(); err != nil {
		if err := printJson(w, r, a.listServers()); err != nil {
			log.Fatal(err)
		}
	}
}