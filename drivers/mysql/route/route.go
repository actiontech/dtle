package route

import (
	"net/http"
	"strings"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"io/ioutil"
	"github.com/julienschmidt/httprouter"
	"encoding/json"
	)

var Host string
var Port string
// decodeBody is used to decode a JSON request body
func decodeBody(req *http.Request, out interface{}) error {
	dec := json.NewDecoder(req.Body)
	return dec.Decode(&out)
}

func UpdupJob(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	var args Job
	if err := decodeBody(r, &args); err != nil {
		hclog.Fmt("err ", err)
	}

	//var nomadJob  route.NomadJob
	nomadJob :=  &NomadJob{}
	nomadJob.Region = args.Region
	nomadJob.EnforceIndex = args.EnforceIndex
	nomadJob.JobModifyIndex = args.ModifyIndex
	nomadJob.ID = args.ID
	nomadJob.ModifyIndex = args.ModifyIndex
	nomadJob.JobModifyIndex = args.JobModifyIndex
	nomadJob.Type = args.Type
	nomadJob.Name = args.Name
	nomadJob.CreateIndex = args.CreateIndex
	nomadJob.Datacenters = args.Datacenters
	for _, task := range args.Tasks {
		taskGroup  :=&TaskGroup{}
		ta:=&NomadTask{}
		taskGroup.Name = task.Type
		if task.Driver =="MySQL"{
			ta.Driver = "mysql"
		}else if task.Driver =="Kafka"{
			ta.Driver = "kafka"
		}

		task.Config["Type"] = task.Type
		ta.Config = task.Config
		ta.Name = task.Type
		taskGroup.Tasks = append(taskGroup.Tasks, ta)
		nomadJob.TaskGroups = append(nomadJob.TaskGroups,taskGroup)
	}

	var nomadJobreq  NomadJobRegisterRequest
	nomadJobreq.Job = nomadJob

	param, err := json.Marshal(nomadJobreq)

	if err != nil {
		fmt.Println("json.marshal failed, err:", err)
		return
	}
	url:="http://"+Host+":"+Port+"/v1/jobs"
	resp, err := http.Post(url, "application/x-www-form-urlencoded",
		strings.NewReader(string(param)))
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}


func JobListRequest(w http.ResponseWriter, r *http.Request,ps httprouter.Params) {

	url:="http://"+Host+":"+Port+"/v1/jobs"
	resp, err := http.Get(url)
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}
func JobRequest(w http.ResponseWriter, r *http.Request,ps httprouter.Params) {

//	path := strings.TrimPrefix(r.URL.Path, "/v1/node/")
	nodeName :=  ps.ByName("NodeId")
	path :=  ps.ByName("path")
	if path=="allocations"{
		url:="http://"+Host+":"+Port+"/v1/job/"+nodeName+"allocations"
		resp, err := http.Get(url)
		if err != nil {
			w.Write([]byte(err.Error()))
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		fmt.Fprintf(w, string(body))
	}else if path=="evaluate"{
		url:="http://"+Host+":"+Port+"/v1/job/"+nodeName+"evaluate"
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

func AllocsRequest(w http.ResponseWriter, r *http.Request,ps httprouter.Params) {


	url:="http://"+Host+":"+Port+"/v1/allocations"
	resp, err := http.Get(url)
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}
func AllocSpecificRequest(w http.ResponseWriter, r *http.Request,ps httprouter.Params) {
	allocID :=  ps.ByName("allocID")
	url:="http://"+Host+":"+Port+"/v1/allocation/"+allocID
	resp, err := http.Get(url)
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}


func EvalsRequest(w http.ResponseWriter, r *http.Request,ps httprouter.Params) {
	url:="http://"+Host+":"+Port+"/v1/evaluations"
	resp, err := http.Get(url)
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}



func EvalRequest(w http.ResponseWriter, r *http.Request,ps httprouter.Params) {
	url:="http://"+Host+":"+Port+"/v1/evaluation/"
	evalID :=  ps.ByName("evalID")
	changeType :=  ps.ByName("type")
	if changeType =="evaluation"{
		url="http://"+Host+":"+Port+"/v1/evaluation/"
	}
	resp, err := http.Get(url+evalID+"/allocations")
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}





func AgentSelfRequest(w http.ResponseWriter, r *http.Request,ps httprouter.Params) {
	url:="http://"+Host+":"+Port+"/v1/agent/self"
	resp, err := http.Get(url)
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}

func ClientAllocRequest(w http.ResponseWriter, r *http.Request,ps httprouter.Params) {
	url:="http://"+Host+":"+Port+"/v1/agent/allocation/"
	tokens :=  ps.ByName("tokens")
	resp, err := http.Get(url+tokens)
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}


func AgentJoinRequest(w http.ResponseWriter, r *http.Request,ps httprouter.Params) {
	url:="http://"+Host+":"+Port+"/v1/agent/join"
	address :=  ps.ByName("address")
	resp, err := http.Post(url, "application/x-www-form-urlencoded",
		strings.NewReader(address))
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}



func AgentForceLeaveRequest(w http.ResponseWriter, r *http.Request,ps httprouter.Params) {
	url:="http://"+Host+":"+Port+"/v1/agent/force-leave"
	node :=  ps.ByName("node")
	resp, err := http.Post(url, "application/x-www-form-urlencoded",
		strings.NewReader(node))
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}




func AgentMembersRequest(w http.ResponseWriter, r *http.Request,ps httprouter.Params) {
	url:="http://"+Host+":"+Port+"/v1/agent/members"
	resp, err := http.Get(url)
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}



func UpdateServers(w http.ResponseWriter, r *http.Request,ps httprouter.Params) {
	url:="http://"+Host+":"+Port+"/v1/agent/servers"
	address :=  ps.ByName("address")
	resp, err := http.Post(url, "application/x-www-form-urlencoded",
		strings.NewReader(address))
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}


func ListServers(w http.ResponseWriter, r *http.Request,ps httprouter.Params) {
	url:="http://"+Host+":"+Port+"/v1/agent/servers"
	resp, err := http.Get(url)
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}
func RegionListRequest(w http.ResponseWriter, r *http.Request,ps httprouter.Params) {
	url:="http://"+Host+":"+Port+"/v1/regions"
	resp, err := http.Get(url)
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}

func StatusLeaderRequest(w http.ResponseWriter, r *http.Request,ps httprouter.Params) {
	url:="http://"+Host+":"+Port+"/v1/status/leader"
	resp, err := http.Get(url)
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}


func StatusPeersRequest(w http.ResponseWriter, r *http.Request,ps httprouter.Params) {
	url:="http://"+Host+":"+Port+"/v1/status/peers"
	resp, err := http.Get(url)
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}


func ValidateJobRequest(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	var args Job
	if err := decodeBody(r, &args); err != nil {
		hclog.Fmt("err ", err)
	}

	//var nomadJob  route.NomadJob
	nomadJob :=  &NomadJob{}
	nomadJob.Region = args.Region
	nomadJob.EnforceIndex = args.EnforceIndex
	nomadJob.JobModifyIndex = args.ModifyIndex
	nomadJob.ID = args.ID
	nomadJob.ModifyIndex = args.ModifyIndex
	nomadJob.JobModifyIndex = args.JobModifyIndex
	nomadJob.Type = args.Type
	nomadJob.Name = args.Name
	nomadJob.CreateIndex = args.CreateIndex
	nomadJob.Datacenters = args.Datacenters
	for _, task := range args.Tasks {
		taskGroup  :=&TaskGroup{}
		ta:=&NomadTask{}
		taskGroup.Name = task.Type
		ta.Driver = task.Driver
		ta.Config = task.Config
		ta.Name = task.Type
		taskGroup.Tasks = append(taskGroup.Tasks, ta)
		nomadJob.TaskGroups = append(nomadJob.TaskGroups,taskGroup)
	}

	var nomadJobreq  NomadJobRegisterRequest
	nomadJobreq.Job = nomadJob

	param, err := json.Marshal(nomadJobreq)

	if err != nil {
		fmt.Println("json.marshal failed, err:", err)
		return
	}
	url:="http://"+Host+":"+Port+"/v1/validate/job"
	resp, err := http.Post(url, "application/x-www-form-urlencoded",
		strings.NewReader(string(param)))
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	fmt.Fprintf(w, string(body))

}




func NodesRequest(w http.ResponseWriter, r *http.Request,ps httprouter.Params) {
	url:="http://"+Host+":"+Port+"/v1/nodes"
	resp, err := http.Get(url)
	if err != nil {
		w.Write([]byte(err.Error()))
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	result :=string(body)
	result = strings.Replace(result, "Address", "HTTPAddr", -1 )
	fmt.Fprintf(w, result)

}



func NodeRequest(w http.ResponseWriter, r *http.Request,ps httprouter.Params) {
	nodeName :=  ps.ByName("nodeName")
	changeType :=  ps.ByName("type")
	url := "http://"+Host+":"+Port+"/v1/node/"+nodeName+"/evaluate"
	if changeType=="evaluate"{
		url = "http://"+Host+":"+Port+"/v1/node/"+nodeName+"/evaluate"
		resp, err := http.Post(url, "application/x-www-form-urlencoded",
			strings.NewReader(""))
		if err != nil {
			w.Write([]byte(err.Error()))
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		fmt.Fprintf(w, string(body))

	}else if changeType=="allocations" {
		url  ="http://"+Host+":"+Port+"/v1/node/"+nodeName+"/allocations"
		resp, err := http.Get(url)
		if err != nil {
			w.Write([]byte(err.Error()))
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		fmt.Fprintf(w, string(body))
	}

}




