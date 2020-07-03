package route

import (
	"encoding/json"
	"fmt"
	"github.com/actiontech/dtle/g"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/api"
	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

var logger = hclog.NewNullLogger()
func SetLogger(theLogger hclog.Logger) {
	logger = theLogger
}

var Host string

// decodeBody is used to decode a JSON request body
func decodeBody(req *http.Request, out interface{}) error {
	dec := json.NewDecoder(req.Body)
	return dec.Decode(&out)
}

// TODO use this instead of + everywhere.
func buildUrl(path string) string {
	return "http://" + Host + path
}
func UpdupJob(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	err := func() (err error) {
		var oldJob Job
		if err := decodeBody(r, &oldJob); err != nil {
			return errors.Wrap(err, "decodeBody")
		}

		var nomadJobreq NomadJobRegisterRequest
		nomadJobreq.Job, err = convertJob(&oldJob)
		if err != nil {
			return errors.Wrap(err, "convertJob")
		}

		param, err := json.Marshal(nomadJobreq)
		if err != nil {
			return errors.Wrap(err, "json.Marshal")
		}

		//logger.Debug("*** json", "json", string(param))

		url := "http://" + Host + "/v1/jobs"
		resp, err := http.Post(url, "application/x-www-form-urlencoded",
			strings.NewReader(string(param)))
		if err != nil {
			return errors.Wrap(err, "forwarding")
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return errors.Wrap(err, "reading forwarded resp")
		}
		_, err = fmt.Fprintf(w, string(body))
		if err != nil {
			return errors.Wrap(err, "writing forwarded resp")
		}

		return nil
	}()

	if err != nil {
		logger.Error("UpdupJob error", "err", err)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(err.Error()))
	}

}

func JobListRequest(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	url := "http://" + Host + "/v1/jobs"
	resp, err := http.Get(url)
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}
func JobRequest(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	//	path := strings.TrimPrefix(r.URL.Path, "/v1/node/")
	nodeName := ps.ByName("NodeId")
	path := ps.ByName("path")
	if path == "allocations" {
		url := "http://" + Host + "/v1/job/" + nodeName + "allocations"
		resp, err := http.Get(url)
		if err != nil {
			w.Write([]byte(err.Error()))
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		fmt.Fprintf(w, string(body))
	} else if path == "evaluate" {
		url := "http://" + Host + "/v1/job/" + nodeName + "evaluate"
		resp, err := http.Post(url, "application/x-www-form-urlencoded",
			strings.NewReader(""))
		if err != nil {
			w.Write([]byte(err.Error()))
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		fmt.Fprintf(w, string(body))
	}

}

func AllocsRequest(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	url := "http://" + Host + "/v1/allocations"
	resp, err := http.Get(url)
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}
func AllocSpecificRequest(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	allocID := ps.ByName("allocID")
	url := "http://" + Host + "/v1/allocation/" + allocID
	resp, err := http.Get(url)
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}

func EvalsRequest(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	url := "http://" + Host + "/v1/evaluations"
	resp, err := http.Get(url)
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}

func EvalRequest(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	url := "http://" + Host + "/v1/evaluation/"
	evalID := ps.ByName("evalID")
	changeType := ps.ByName("type")
	if changeType == "evaluation" {
		url = "http://" + Host + "/v1/evaluation/"
	}
	resp, err := http.Get(url + evalID + "/allocations")
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}

func AgentSelfRequest(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	url := "http://" + Host + "/v1/agent/self"
	resp, err := http.Get(url)
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}

func ClientAllocRequest(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	url := "http://" + Host + "/v1/agent/allocation/"
	tokens := ps.ByName("tokens")
	resp, err := http.Get(url + tokens)
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}

func AgentJoinRequest(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	url := "http://" + Host + "/v1/agent/join"
	address := ps.ByName("address")
	resp, err := http.Post(url, "application/x-www-form-urlencoded",
		strings.NewReader(address))
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}

func AgentForceLeaveRequest(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	url := "http://" + Host + "/v1/agent/force-leave"
	node := ps.ByName("node")
	resp, err := http.Post(url, "application/x-www-form-urlencoded",
		strings.NewReader(node))
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}

func AgentMembersRequest(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	url := "http://" + Host + "/v1/agent/members"
	resp, err := http.Get(url)
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}

func UpdateServers(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	url := "http://" + Host + "/v1/agent/servers"
	address := ps.ByName("address")
	resp, err := http.Post(url, "application/x-www-form-urlencoded",
		strings.NewReader(address))
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}

func ListServers(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	url := "http://" + Host + "/v1/agent/servers"
	resp, err := http.Get(url)
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}
func RegionListRequest(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	url := "http://" + Host + "/v1/regions"
	resp, err := http.Get(url)
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}

func StatusLeaderRequest(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	url := "http://" + Host + "/v1/status/leader"
	resp, err := http.Get(url)
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}

func StatusPeersRequest(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	url := "http://" + Host + "/v1/status/peers"
	resp, err := http.Get(url)
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}

func convertJob(oldJob *Job) (*api.Job, error) {
	// oldJob: Name is optional (can be empty. repeatable). ID is optional (autogen if empty)
	// newJob: Name is mandatory and unique
	var jobName string
	if !g.StringPtrEmpty(oldJob.Name) {
		jobName = *oldJob.Name
	} else if !g.StringPtrEmpty(oldJob.ID) {
		jobName = *oldJob.ID
	} else {
		jobName = time.Now().Format("2006-01-02_15:04:05.000000")
	}

	nomadJob := api.NewServiceJob(jobName, jobName, "", structs.JobDefaultPriority)
	nomadJob.Datacenters = []string{"dc1"}
	for _, oldTask := range oldJob.Tasks {
		taskGroup := api.NewTaskGroup(oldTask.Type, 1)
		newTask := api.NewTask(oldTask.Type, g.PluginName)

		logger.Debug("*** task config", "config", oldTask.Config)

		switch strings.ToUpper(oldTask.Driver) {
		case "MYSQL", "":
			newTask.Config = oldTask.Config
		case "KAFKA":
			newTask.Config = make(map[string]interface{})
			newTask.Config["KafkaConfig"] = oldTask.Config
		default:
			return nil, fmt.Errorf("unknown driver %v", oldTask.Driver)
		}

		taskGroup.Tasks = append(taskGroup.Tasks, newTask)
		nomadJob.TaskGroups = append(nomadJob.TaskGroups, taskGroup)
	}
	return nomadJob, nil
}

func ValidateJobRequest(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	var err error
	var oldJob Job
	if err := decodeBody(r, &oldJob); err != nil {
		hclog.Fmt("err ", err)
	}

	var nomadJobreq NomadJobRegisterRequest
	nomadJobreq.Job, err = convertJob(&oldJob)
	if err != nil {
		//return errors.Wrap(err, "convertJob")
		// TODO
	}

	param, err := json.Marshal(nomadJobreq)

	if err != nil {
		fmt.Println("json.marshal failed, err:", err)
		return
	}
	url := "http://" + Host + "/v1/validate/job"
	resp, err := http.Post(url, "application/x-www-form-urlencoded",
		strings.NewReader(string(param)))
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}

func NodesRequest(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	url := "http://" + Host + "/v1/nodes"
	resp, err := http.Get(url)
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	result := string(body)
	result = strings.Replace(result, "Address", "HTTPAddr", -1)
	fmt.Fprintf(w, result)

}

func NodeRequest(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	nodeName := ps.ByName("nodeName")
	changeType := ps.ByName("type")
	url := "http://" + Host + "/v1/node/" + nodeName + "/evaluate"
	if changeType == "evaluate" {
		url = "http://" + Host + "/v1/node/" + nodeName + "/evaluate"
		resp, err := http.Post(url, "application/x-www-form-urlencoded",
			strings.NewReader(""))
		if err != nil {
			w.Write([]byte(err.Error()))
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		fmt.Fprintf(w, string(body))

	} else if changeType == "allocations" {
		url = "http://" + Host + "/v1/node/" + nodeName + "/allocations"
		resp, err := http.Get(url)
		if err != nil {
			w.Write([]byte(err.Error()))
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		fmt.Fprintf(w, string(body))
	}

}
