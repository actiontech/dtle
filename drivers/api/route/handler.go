package route

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/actiontech/dtle/g"
	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/api"
	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
)

var logger = hclog.NewNullLogger()

func SetLogger(theLogger hclog.Logger) {
	logger = theLogger
}

var NomadHost string

// decodeBody is used to decode a JSON request body
func decodeBody(req *http.Request, out interface{}) error {
	dec := json.NewDecoder(req.Body)
	return dec.Decode(&out)
}

func buildUrl(path string) string {
	return "http://" + NomadHost + path
}
func UpdupJob(c echo.Context) error {
	var oldJob OldJob
	err := decodeBody(c.Request(), &oldJob)
	if err != nil {
		return errors.Wrap(err, "decodeBody")
	}

	var nomadJobreq api.JobRegisterRequest
	nomadJobreq.Job, err = convertJob(&oldJob)
	if err != nil {
		return errors.Wrap(err, "convertJob")
	}

	param, err := json.Marshal(nomadJobreq)
	if err != nil {
		return errors.Wrap(err, "json.Marshal")
	}

	//logger.Debug("*** json", "json", string(param))

	url := buildUrl("/v1/jobs")
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
	return c.Blob(http.StatusOK, "text/html; charset=utf-8", body)
}

func JobListRequest(c echo.Context) error {

	url := buildUrl("/v1/jobs")
	resp, err := http.Get(url)
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrap(err, "reading forwarded resp")
	}

	return c.Blob(http.StatusOK, "text/html; charset=utf-8", body)
}

func JobRequest(c echo.Context) error {

	//	path := strings.TrimPrefix(r.URL.Path, "/v1/node/")
	jobId := c.Param("jobId")
	path := c.Param("path")
	if path == "allocations" {
		url := buildUrl("/v1/job/" + jobId + "/allocations")
		resp, err := http.Get(url)
		if err != nil {
			return c.String(http.StatusInternalServerError, err.Error())
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if nil != err {
			return c.String(http.StatusInternalServerError, err.Error())
		}

		return c.Blob(http.StatusOK, "text/html; charset=utf-8", body)
	} else if path == "evaluate" {
		url := buildUrl("/v1/job/" + jobId + "/evaluate")
		resp, err := http.Post(url, "application/x-www-form-urlencoded",
			strings.NewReader(""))
		if err != nil {
			return c.String(http.StatusInternalServerError, err.Error())
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if nil != err {
			return c.String(http.StatusInternalServerError, err.Error())
		}

		return c.Blob(http.StatusOK, "text/html; charset=utf-8", body)
	} else if path == "resume" {
		url := buildUrl("/v1/job/" + jobId + "/resume")
		resp, err := http.Get(url)
		if err != nil {
			return c.String(http.StatusInternalServerError, err.Error())
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if nil != err {
			return c.String(http.StatusInternalServerError, err.Error())
		}

		return c.Blob(http.StatusOK, "text/html; charset=utf-8", body)
	} else if path == "pause" {
		url := buildUrl("/v1/job/" + jobId + "/pause")
		resp, err := http.Get(url)
		if err != nil {
			return c.String(http.StatusInternalServerError, err.Error())
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if nil != err {
			return c.String(http.StatusInternalServerError, err.Error())
		}

		return c.Blob(http.StatusOK, "text/html; charset=utf-8", body)
	}
	return c.String(http.StatusInternalServerError, fmt.Sprintf("unknown url"))
}
func JobDetailRequest(c echo.Context) error {
	jobId := c.Param("jobId")
	url := buildUrl("/v1/job/" + jobId)
	resp, err := http.Get(url)
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if nil != err {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.Blob(http.StatusOK, "text/html; charset=utf-8", body)
}
func JobDeleteRequest(c echo.Context) error {
	jobId := c.Param("jobId")
	url := buildUrl("/v1/job/" + jobId + "?purge=true")
	req, _ := http.NewRequest("DELETE", url, nil)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if nil != err {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.Blob(http.StatusOK, "text/html; charset=utf-8", body)
}

func AllocsRequest(c echo.Context) error {

	url := buildUrl("/v1/allocations")
	resp, err := http.Get(url)
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if nil != err {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.Blob(http.StatusOK, "text/html; charset=utf-8", body)
}
func AllocSpecificRequest(c echo.Context) error {
	allocID := c.Param("allocID")
	url := buildUrl("/v1/allocation/" + allocID)
	resp, err := http.Get(url)
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if nil != err {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.Blob(http.StatusOK, "text/html; charset=utf-8", body)
}

func EvalsRequest(c echo.Context) error {
	url := buildUrl("/v1/evaluations")
	resp, err := http.Get(url)
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if nil != err {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.Blob(http.StatusOK, "text/html; charset=utf-8", body)
}

func EvalRequest(c echo.Context) error {
	url := buildUrl("/v1/evaluation/")
	evalID := c.Param("evalID")
	changeType := c.Param("type")
	if changeType == "evaluation" {
		url = "http://" + NomadHost + "/v1/evaluation/"
	}
	resp, err := http.Get(url + evalID + "/allocations")
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if nil != err {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.Blob(http.StatusOK, "text/html; charset=utf-8", body)
}

func AgentSelfRequest(c echo.Context) error {
	url := buildUrl("/v1/agent/self")
	resp, err := http.Get(url)
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if nil != err {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.Blob(http.StatusOK, "text/html; charset=utf-8", body)
}

func ClientAllocRequest(c echo.Context) error {
	url := buildUrl("/v1/client/allocation/" + c.Param("tokens") + "/stats")
	resp, err := http.Get(url)
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if nil != err {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.Blob(http.StatusOK, "text/html; charset=utf-8", body)
}

func AgentJoinRequest(c echo.Context) error {
	url := buildUrl("/v1/agent/join")
	address := c.Param("address")
	resp, err := http.Post(url, "application/x-www-form-urlencoded",
		strings.NewReader(address))
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if nil != err {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.Blob(http.StatusOK, "text/html; charset=utf-8", body)
}

func AgentForceLeaveRequest(c echo.Context) error {
	url := buildUrl("/v1/agent/force-leave")
	node := c.Param("node")
	resp, err := http.Post(url, "application/x-www-form-urlencoded",
		strings.NewReader(node))
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if nil != err {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.Blob(http.StatusOK, "text/html; charset=utf-8", body)
}

func AgentMembersRequest(c echo.Context) error {
	url := buildUrl("/v1/agent/members")
	resp, err := http.Get(url)
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if nil != err {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.Blob(http.StatusOK, "text/html; charset=utf-8", body)
}

func UpdateServers(c echo.Context) error {
	url := buildUrl("/v1/agent/servers")
	address := c.Param("address")
	resp, err := http.Post(url, "application/x-www-form-urlencoded",
		strings.NewReader(address))
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if nil != err {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.Blob(http.StatusOK, "text/html; charset=utf-8", body)
}

func ListServers(c echo.Context) error {
	url := buildUrl("/v1/agent/servers")
	resp, err := http.Get(url)
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if nil != err {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.Blob(http.StatusOK, "text/html; charset=utf-8", body)
}
func RegionListRequest(c echo.Context) error {
	url := buildUrl("/v1/regions")
	resp, err := http.Get(url)
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if nil != err {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.Blob(http.StatusOK, "text/html; charset=utf-8", body)
}

func StatusLeaderRequest(c echo.Context) error {
	url := buildUrl("/v1/status/leader")
	resp, err := http.Get(url)
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if nil != err {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.Blob(http.StatusOK, "text/html; charset=utf-8", body)
}

func StatusPeersRequest(c echo.Context) error {
	url := buildUrl("/v1/status/peers")
	resp, err := http.Get(url)
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if nil != err {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.Blob(http.StatusOK, "text/html; charset=utf-8", body)
}

func convertJob(oldJob *OldJob) (*api.Job, error) {
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

		logger.Debug("task config", "config", oldTask.Config)

		switch strings.ToUpper(oldTask.Driver) {
		case "MYSQL", "":
			newTask.Config = oldTask.Config
			delete(newTask.Config, "BytesLimit")
			delete(newTask.Config, "NatsAddr")
			delete(newTask.Config, "MsgsLimit")
			delete(newTask.Config, "MsgBytesLimit")
			delete(newTask.Config, "ApproveHeterogeneous")
			delete(newTask.Config, "SqlMode")
			delete(newTask.Config, "SystemVariables")
			delete(newTask.Config, "HasSuperPrivilege")
			delete(newTask.Config, "MySQLVersion")
			delete(newTask.Config, "TimeZone")
			delete(newTask.Config, "MySQLServerUuid")
		case "KAFKA":
			newTask.Config = make(map[string]interface{})
			newTask.Config["KafkaConfig"] = oldTask.Config
		default:
			return nil, fmt.Errorf("unknown driver %v", oldTask.Driver)
		}
		if oldTask.NodeID != "" {
			newAff := api.NewAffinity("${node.unique.id}", "=",
				// https://www.nomadproject.io/docs/runtime/interpolation
				// This page uses lower ID. I don't know if it is necessary.
				strings.ToLower(oldTask.NodeID), 50)
			newTask.Affinities = append(newTask.Affinities, newAff)
		} else if oldTask.NodeName != "" {
			newAff := api.NewAffinity("${node.unique.name}", "=", oldTask.NodeName, 50)
			newTask.Affinities = append(newTask.Affinities, newAff)
		}

		taskGroup.Tasks = append(taskGroup.Tasks, newTask)
		nomadJob.TaskGroups = append(nomadJob.TaskGroups, taskGroup)
	}
	return nomadJob, nil
}

func ValidateJobRequest(c echo.Context) error {

	var err error
	var oldJob OldJob
	if err := decodeBody(c.Request(), &oldJob); err != nil {
		hclog.Fmt("err ", err)
	}

	var nomadJobreq api.JobRegisterRequest
	nomadJobreq.Job, err = convertJob(&oldJob)
	if err != nil {
		//return errors.Wrap(err, "convertJob")
		// TODO
	}

	param, err := json.Marshal(nomadJobreq)

	if err != nil {
		return c.String(http.StatusInternalServerError, fmt.Sprintf("json.marshal failed, err: %v", err))
	}
	url := buildUrl("/v1/validate/job")
	resp, err := http.Post(url, "application/x-www-form-urlencoded",
		strings.NewReader(string(param)))
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if nil != err {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.Blob(http.StatusOK, "text/html; charset=utf-8", body)
}

func NodesRequest(c echo.Context) error {
	url := buildUrl("/v1/nodes")
	resp, err := http.Get(url)
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	result := string(body)
	result = strings.Replace(result, "Address", "HTTPAddr", -1)
	if nil != err {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.Blob(http.StatusOK, "text/html; charset=utf-8", body)
}

func NodeRequest(c echo.Context) error {
	nodeName := c.Param("nodeName")
	changeType := c.Param("type")
	url := buildUrl("/v1/node/" + nodeName + "/evaluate")
	if changeType == "evaluate" {
		url = buildUrl("/v1/node/" + nodeName + "/evaluate")
		resp, err := http.Post(url, "application/x-www-form-urlencoded",
			strings.NewReader(""))
		if err != nil {
			return c.String(http.StatusInternalServerError, err.Error())
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if nil != err {
			return c.String(http.StatusInternalServerError, err.Error())
		}

		return c.Blob(http.StatusOK, "text/html; charset=utf-8", body)
	} else if changeType == "allocations" {
		url = buildUrl("/v1/node/" + nodeName + "/allocations")
		resp, err := http.Get(url)
		if err != nil {
			return c.String(http.StatusInternalServerError, err.Error())
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if nil != err {
			return c.String(http.StatusInternalServerError, err.Error())
		}

		return c.Blob(http.StatusOK, "text/html; charset=utf-8", body)
	}
	return c.String(http.StatusInternalServerError, fmt.Sprintf("unknown change type: %v", changeType))
}
