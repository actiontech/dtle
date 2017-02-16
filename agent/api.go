package agent

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/carbocation/interpose"
	"github.com/docker/libkv/store"
	"github.com/gorilla/mux"
	"github.com/ngaut/log"
)

const apiPathPrefix = "v1"

func (a *Agent) ServeHTTP() {
	r := mux.NewRouter().StrictSlash(true)
	a.apiRoutes(r)

	middle := interpose.New()
	middle.Use(metaMiddleware(a.config.NodeName))
	middle.UseHandler(r)

	srv := &http.Server{Addr: a.config.HTTPAddr, Handler: middle}

	log.Infof("address:%v,api: Running HTTP server", a.config.HTTPAddr)

	go srv.ListenAndServe()
}

func (a *Agent) apiRoutes(r *mux.Router) {
	subver := r.PathPrefix("/v1").Subrouter()
	subver.HandleFunc("/", a.indexHandler)
	subver.HandleFunc("/members", a.membersHandler)
	subver.HandleFunc("/leader", a.leaderHandler)
	subver.HandleFunc("/leave", a.leaveHandler).Methods(http.MethodGet, http.MethodPost)

	subver.Path("/jobs").HandlerFunc(a.jobCreateOrUpdateHandler).Methods(http.MethodPost, http.MethodPatch)
	// Place fallback routes last
	subver.Path("/jobs").HandlerFunc(a.jobsHandler)

	sub := subver.PathPrefix("/jobs").Subrouter()
	sub.HandleFunc("/{job}", a.jobDeleteHandler).Methods(http.MethodDelete)
	sub.HandleFunc("/{job}", a.jobRunHandler).Methods(http.MethodPost)
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
		log.Fatal(err)
	}
}

func (a *Agent) jobsHandler(w http.ResponseWriter, r *http.Request) {
	jobs, err := a.store.GetJobs()
	if err != nil {
		log.Fatal(err)
	}

	if err := printJson(w, r, jobs); err != nil {
		log.Fatal(err)
	}
}

func (a *Agent) jobGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobName := vars["job"]

	job, err := a.store.GetJob(jobName)
	if err != nil {
		log.Error(err)
	}
	if job == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if err := printJson(w, r, job); err != nil {
		log.Fatal(err)
	}
}

func (a *Agent) jobCreateOrUpdateHandler(w http.ResponseWriter, r *http.Request) {
	// Init the Job object with defaults
	job := Job{
		Concurrency: ConcurrencyAllow,
	}

	body, err := ioutil.ReadAll(io.LimitReader(r.Body, 1048576))
	if err != nil {
		log.Fatal(err)
	}

	if err := json.Unmarshal(body, &job); err != nil {
		w.WriteHeader(422) // unprocessable entity
		if err := json.NewEncoder(w).Encode(err); err != nil {
			log.Fatal(err)
		}
		log.Infof("err:%v", err)
		return
	}

	if err := r.Body.Close(); err != nil {
		log.Fatal(err)
	}

	// Get if the requested job already exist
	ej, err := a.store.GetJob(job.Name)
	if err != nil && err != store.ErrKeyNotFound {
		w.WriteHeader(422) // unprocessable entity
		if err := json.NewEncoder(w).Encode(err.Error()); err != nil {
			log.Fatal(err)
		}
		return
	}

	// If it's an existing job, lock it
	if ej != nil {
		ej.Lock()
		defer ej.Unlock()
	}

	// Save the job to the store
	if err = a.store.UpsertJob(&job); err != nil {
		w.WriteHeader(422) // unprocessable entity
		if err := json.NewEncoder(w).Encode(err.Error()); err != nil {
			log.Fatal(err)
		}
		return
	}

	// Save the job parent
	if err = a.store.UpsertJobDependencyTree(&job, ej); err != nil {
		w.WriteHeader(422) // unprocessable entity
		if err := json.NewEncoder(w).Encode(err.Error()); err != nil {
			log.Fatal(err)
		}
		return
	}

	a.schedulerRestartQuery(string(a.store.GetLeader()))

	w.Header().Set("Location", fmt.Sprintf("%s/%s", r.RequestURI, job.Name))
	w.WriteHeader(http.StatusCreated)
	if err := printJson(w, r, &job); err != nil {
		log.Fatal(err)
	}
	log.Infof("job:%v", job)
}

func (a *Agent) jobDeleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobName := vars["job"]

	job, err := a.store.DeleteJob(jobName)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		if err := json.NewEncoder(w).Encode(err); err != nil {
			log.Fatal(err)
		}
		return
	}

	a.schedulerRestartQuery(string(a.store.GetLeader()))

	if err := printJson(w, r, job); err != nil {
		log.Fatal(err)
	}
}

func (a *Agent) jobRunHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobName := vars["job"]

	job, err := a.store.GetJob(jobName)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		if err := json.NewEncoder(w).Encode(err); err != nil {
			log.Fatal(err)
		}
		return
	}
	a.RunQuery(job)

	w.Header().Set("Location", r.RequestURI)
	w.WriteHeader(http.StatusAccepted)
	if err := printJson(w, r, job); err != nil {
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
