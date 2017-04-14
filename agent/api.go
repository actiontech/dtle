package agent

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/carbocation/interpose"
	"github.com/docker/libkv/store"
	"github.com/gorilla/mux"

	"udup/internal/client/plugins"
	ulog "udup/logger"
)

const apiPathPrefix = "v1"

func (a *Agent) NewHTTPServer() {
	r := mux.NewRouter().StrictSlash(true)
	a.apiRoutes(r)

	middle := interpose.New()
	middle.Use(metaMiddleware(a.config.NodeName))
	middle.UseHandler(r)

	srv := &http.Server{Addr: a.config.Server.HTTPAddr, Handler: middle}

	ulog.Logger.WithFields(logrus.Fields{
		"address": a.config.Server.HTTPAddr,
	}).Info("api: Running HTTP server")

	go srv.ListenAndServe()
}

func (a *Agent) apiRoutes(r *mux.Router) {
	subver := r.PathPrefix("/v1").Subrouter()
	subver.HandleFunc("/", a.indexHandler)
	subver.HandleFunc("/members", a.membersHandler)
	subver.HandleFunc("/leader", a.leaderHandler)
	subver.HandleFunc("/leave", a.leaveHandler).Methods(http.MethodGet, http.MethodPost)

	subver.Path("/jobs").HandlerFunc(a.jobUpsertHandler).Methods(http.MethodPost, http.MethodPatch)
	// Place fallback routes last
	subver.Path("/jobs").HandlerFunc(a.jobsHandler)

	sub := subver.PathPrefix("/jobs").Subrouter()
	sub.HandleFunc("/{job}", a.jobDeleteHandler).Methods(http.MethodDelete)
	sub.HandleFunc("/{job}/start", a.jobStartHandler).Methods(http.MethodPost)
	sub.HandleFunc("/{job}/stop", a.jobStopHandler).Methods(http.MethodPost)
	// Place fallback routes last
	sub.HandleFunc("/{job}", a.jobGetHandler)
}

func metaMiddleware(nodeName string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.URL.Path, "/"+apiPathPrefix) {
				w.Header().Set("Content-Type", "application/json; charset=UTF-8")
				w.Header().Set("X-Whom", nodeName)
			}
			next.ServeHTTP(w, r)
		})
	}
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
		ulog.Logger.Fatal(err)
	}
}

func (a *Agent) jobsHandler(w http.ResponseWriter, r *http.Request) {
	jobs, err := a.store.GetJobs()
	if err != nil {
		ulog.Logger.Fatal(err)
	}

	if err := printJson(w, r, jobs); err != nil {
		ulog.Logger.Fatal(err)
	}
}

func (a *Agent) jobGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobName := vars["job"]

	job, err := a.store.GetJob(jobName)
	if err != nil {
		ulog.Logger.Error(err)
	}
	if job == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if err := printJson(w, r, job); err != nil {
		ulog.Logger.Fatal(err)
	}
}

func (a *Agent) jobUpsertHandler(w http.ResponseWriter, r *http.Request) {
	// Init the Job object with defaults
	job := Job{
		Status: Stopped,
	}

	body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1048576))
	if err != nil {
		ulog.Logger.Fatal(err)
	}

	if err := json.Unmarshal(body, &job); err != nil {
		w.WriteHeader(422) // unprocessable entity
		if err := json.NewEncoder(w).Encode(err); err != nil {
			ulog.Logger.Fatal(err)
		}
		return
	}

	if err := r.Body.Close(); err != nil {
		ulog.Logger.Fatal(err)
	}

	// Get if the requested job already exist
	ej, err := a.store.GetJob(job.Name)
	if err != nil && err != store.ErrKeyNotFound {
		w.WriteHeader(422) // unprocessable entity
		if err := json.NewEncoder(w).Encode(err.Error()); err != nil {
			ulog.Logger.Fatal(err)
		}
		return
	}

	// If it's an existing job, lock it
	if ej != nil {
		ej.Lock()
		defer ej.Unlock()
	}
	for k, v := range job.Processors {
		if k == plugins.DataSrc && v.ServerID == 0 {
			job.Processors[plugins.DataSrc].ServerID, err = a.genServerId(string(a.store.GetLeader()))
			if err != nil {
				ulog.Logger.Fatal(err)
			}
		}
	}

	// Save the job to the store
	if err = a.store.UpsertJob(&job); err != nil {
		w.WriteHeader(422) // unprocessable entity
		if err := json.NewEncoder(w).Encode(err.Error()); err != nil {
			ulog.Logger.Fatal(err)
		}
		return
	}

	w.Header().Set("Location", fmt.Sprintf("%s/%s", r.RequestURI, job.Name))
	w.WriteHeader(http.StatusCreated)
	if err := printJson(w, r, &job); err != nil {
		ulog.Logger.Fatal(err)
	}
}

func (a *Agent) jobDeleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobName := vars["job"]

	j, err := a.store.GetJob(jobName)
	if err != nil && err != store.ErrKeyNotFound {
		w.WriteHeader(422) // unprocessable entity
		if err := json.NewEncoder(w).Encode(err.Error()); err != nil {
			ulog.Logger.Fatal(err)
		}
		return
	}

	if j.Status == Running {
		w.WriteHeader(422) // unprocessable entity
		if err := json.NewEncoder(w).Encode("Status cannot be deleted"); err != nil {
			ulog.Logger.Fatal(err)
		}
		return
	}

	job, err := a.store.DeleteJob(jobName)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		if err := json.NewEncoder(w).Encode(err); err != nil {
			ulog.Logger.Fatal(err)
		}
		return
	}

	if err := printJson(w, r, job); err != nil {
		ulog.Logger.Fatal(err)
	}
}

func (a *Agent) jobStartHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobName := vars["job"]

	job, err := a.store.GetJob(jobName)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		if err := json.NewEncoder(w).Encode(err); err != nil {
			ulog.Logger.Fatal(err)
		}
		return
	}
	job.Start()

	w.Header().Set("Location", r.RequestURI)
	w.WriteHeader(http.StatusAccepted)
	if err := printJson(w, r, job); err != nil {
		ulog.Logger.Fatal(err)
	}
}

func (a *Agent) jobStopHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobName := vars["job"]

	job, err := a.store.GetJob(jobName)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		if err := json.NewEncoder(w).Encode(err); err != nil {
			ulog.Logger.Fatal(err)
		}
		return
	}

	job.Stop()

	w.Header().Set("Location", r.RequestURI)
	w.WriteHeader(http.StatusAccepted)
	if err := printJson(w, r, job); err != nil {
		ulog.Logger.Fatal(err)
	}
}

func (a *Agent) membersHandler(w http.ResponseWriter, r *http.Request) {
	if err := printJson(w, r, a.serf.Members()); err != nil {
		ulog.Logger.Fatal(err)
	}
}

func (a *Agent) leaderHandler(w http.ResponseWriter, r *http.Request) {
	member, err := a.leaderMember()
	if err == nil {
		if err := printJson(w, r, member); err != nil {
			ulog.Logger.Fatal(err)
		}
	}
}

func (a *Agent) leaveHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		ulog.Logger.Warn("/leave GET is deprecated and will be removed, use POST")
	}
	if err := a.serf.Leave(); err != nil {
		if err := printJson(w, r, a.listServers()); err != nil {
			ulog.Logger.Fatal(err)
		}
	}
}
