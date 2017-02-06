package agent

import (
	"encoding/json"
	"math/rand"

	"github.com/docker/libkv/store"
	"github.com/hashicorp/serf/serf"
	"github.com/ngaut/log"
)

const (
	RestartJob = "RestartJob"
	RunJob     = "RunJob"
	RPCConfig  = "RpcConfig"
)

type RunQueryParam struct {
	Job     *Job   `json:"job"`
	RPCAddr string `json:"rpc_addr"`
}

// Send a serf run query to the cluster, this is used to ask a node or nodes
// to run a Job.
func (a *Agent) RunQuery(job *Job) {
	var params *serf.QueryParam

	job, err := a.store.JobByName(job.Name)

	//Job can be removed and the RestartJob not yet received.
	//In this case, the job will not be found in the store.
	if err == store.ErrKeyNotFound {
		log.Infof("Job not found, cancelling this execution")
		return
	}

	params = &serf.QueryParam{
		FilterNodes: []string{a.config.NodeName},
		FilterTags:  nil,
		RequestAck:  true,
	}

	rqp := &RunQueryParam{
		Job:     job,
		RPCAddr: a.getRPCAddr(),
	}
	rqpJson, _ := json.Marshal(rqp)

	log.Infof("query:%v,job_name:%v,json:%v,agent: Sending query", RunJob, job.Name, string(rqpJson))

	qr, err := a.serf.Query(RunJob, rqpJson, params)
	if err != nil {
		log.Infof("query:%v,err:%v,agent: Sending query error", RunJob, err)
	}
	defer qr.Close()

	ackCh := qr.AckCh()
	respCh := qr.ResponseCh()

	for !qr.Finished() {
		select {
		case ack, ok := <-ackCh:
			if ok {
				log.Infof("query:%v,from:%v,agent: Received ack", RunJob, ack)
			}
		case resp, ok := <-respCh:
			if ok {
				log.Infof("query:%v,from:%v,response:%v,agent: Received response", RunJob, resp.From, string(resp.Payload))

				// Save execution to store
				a.JobRegister(resp.Payload)
			}
		}
	}
	log.Infof("query:%v,agent: Done receiving acks and responses", RunJob)
}

// Broadcast a jobRestartQuery to the cluster, only server members
// will attend to this. Forces a scheduler restart and reload all jobs.
func (a *Agent) jobRestartQuery(leaderName string) {
	params := &serf.QueryParam{
		FilterNodes: []string{leaderName},
		RequestAck:  true,
	}

	qr, err := a.serf.Query(RestartJob, []byte(""), params)
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
	log.Infof("query:%v,agent: Done receiving acks and responses", RestartJob)
}

// Broadcast a query to get the RPC config of one udup_server, any that could
// attend later RPC calls.
func (a *Agent) queryRPCConfig() ([]byte, error) {
	nodeName := a.selectServer().Name

	params := &serf.QueryParam{
		FilterNodes: []string{nodeName},
		FilterTags:  map[string]string{"udup_server": "true"},
		RequestAck:  true,
	}

	qr, err := a.serf.Query(RPCConfig, nil, params)
	if err != nil {
		log.Infof("query:%v,err:%v,proc: Error sending query", RPCConfig, err)
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
				log.Infof("query:%v,from:%v,proc: Received ack", RPCConfig, ack)
			}
		case resp, ok := <-respCh:
			if ok {
				log.Infof("query:%v,from:%v,payload:%v,proc: Received response", RPCConfig, resp.From, string(resp.Payload))

				rpcAddr = resp.Payload
			}
		}
	}
	log.Infof("query:%v,proc: Done receiving acks and responses", RPCConfig)

	return rpcAddr, nil
}

func (a *Agent) selectServer() serf.Member {
	servers := a.listServers()
	server := servers[rand.Intn(len(servers))]

	return server
}
