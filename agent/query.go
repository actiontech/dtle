package agent

import (
	"encoding/json"
	"math/rand"

	"github.com/docker/libkv/store"
	"github.com/hashicorp/serf/serf"
	"github.com/ngaut/log"
)

const (
	QuerySchedulerRestart = "scheduler:restart"
	QueryRunJob           = "run:job"
	QueryRPCConfig        = "rpc:config"
)

type RunQueryParam struct {
	Job     *Job   `json:"job"`
	RPCAddr string `json:"rpc_addr"`
}

// Send a serf run query to the cluster, this is used to ask a node or nodes
// to run a Job.
func (a *Agent) RunQuery(j *Job) {
	var params *serf.QueryParam

	job, err := a.store.GetJob(j.Name)

	//Job can be removed and the QuerySchedulerRestart not yet received.
	//In this case, the job will not be found in the store.
	if err == store.ErrKeyNotFound {
		log.Infof("Job not found, cancelling this execution")
		return
	}

	params = &serf.QueryParam{
		FilterNodes: []string{j.NodeName},
		RequestAck:  true,
	}

	rqp := &RunQueryParam{
		Job:     j,
		RPCAddr: a.getRPCAddr(),
	}
	rqpJson, _ := json.Marshal(rqp)

	log.Debug("query:%v,job_name:%v,json:%v,agent: Sending query", QueryRunJob, job.Name, string(rqpJson))

	qr, err := a.serf.Query(QueryRunJob, rqpJson, params)
	if err != nil {
		log.Infof("query:%v,err:%v,agent: Sending query error", QueryRunJob, err)
	}
	defer qr.Close()

	ackCh := qr.AckCh()
	respCh := qr.ResponseCh()

	for !qr.Finished() {
		select {
		case ack, ok := <-ackCh:
			if ok {
				log.Debug("query:%v,from:%v,agent: Received ack", QueryRunJob, ack)
			}
		case resp, ok := <-respCh:
			if ok {
				log.Debug("query:%v,from:%v,response:%v,agent: Received response", QueryRunJob, resp.From, string(resp.Payload))

				// Save execution to store
				a.upsertJob(resp.Payload)
			}
		}
	}
	log.Infof("query:%v,agent: Done receiving acks and responses", QueryRunJob)
}

func (a *Agent) upsertJob(payload []byte) *Job {
	var ex Job
	if err := json.Unmarshal(payload, &ex); err != nil {
		log.Fatal(err)
	}

	// Save the new execution to store
	if err := a.store.UpsertJob(&ex); err != nil {
		log.Fatal(err)
	}

	return &ex
}

// Broadcast a SchedulerRestartQuery to the cluster, only server members
// will attend to this. Forces a scheduler restart and reload all jobs.
func (a *Agent) schedulerRestartQuery(leaderName string) {
	params := &serf.QueryParam{
		FilterNodes: []string{leaderName},
		RequestAck:  true,
	}

	qr, err := a.serf.Query(QuerySchedulerRestart, []byte(""), params)
	if err != nil {
		log.Infof("err:%v,agent: Error sending the scheduler reload query", err)
	}
	defer qr.Close()

	ackCh := qr.AckCh()
	respCh := qr.ResponseCh()

	for !qr.Finished() {
		select {
		case ack, ok := <-ackCh:
			if ok {
				log.Infof("from:%v,agent: Received ack", ack)
			}
		case resp, ok := <-respCh:
			if ok {
				log.Infof("from:%v,payload:%v,agent: Received response", resp.From, string(resp.Payload))
			}
		}
	}
	log.Infof("query:%v,agent: Done receiving acks and responses", QuerySchedulerRestart)
}

// Broadcast a query to get the RPC config of one udup_server, any that could
// attend later RPC calls.
func (a *Agent) queryRPCConfig(nodeName string) ([]byte, error) {
	if nodeName == "" {
		nodeName = a.selectServer().Name
	}

	params := &serf.QueryParam{
		FilterNodes: []string{nodeName},
		FilterTags:  map[string]string{"udup_server": "true"},
		RequestAck:  true,
	}

	qr, err := a.serf.Query(QueryRPCConfig, nil, params)
	if err != nil {
		log.Infof("query:%v,err:%v,proc: Error sending query", QueryRPCConfig, err)
		return nil, err
	}
	defer qr.Close()

	ackCh := qr.AckCh()
	respCh := qr.ResponseCh()

	var rpcAddr []byte
	for !qr.Finished() {
		select {
		case ack, ok := <-ackCh:
			if ok {
				log.Infof("query:%v,from:%v,proc: Received ack", QueryRPCConfig, ack)
			}
		case resp, ok := <-respCh:
			if ok {
				log.Infof("query:%v,from:%v,payload:%v,proc: Received response", QueryRPCConfig, resp.From, string(resp.Payload))

				rpcAddr = resp.Payload
			}
		}
	}
	log.Infof("query:%v,proc: Done receiving acks and responses", QueryRPCConfig)

	return rpcAddr, nil
}

func (a *Agent) selectServer() serf.Member {
	servers := a.listServers()
	server := servers[rand.Intn(len(servers))]

	return server
}
