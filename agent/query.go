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
	QueryStartJob           = "start:job"
	QueryStopJob          = "stop:job"
	QueryRPCConfig        = "rpc:config"
)

type RunQueryParam struct {
	Job     *Job   `json:"job"`
	RPCAddr string `json:"rpc_addr"`
}

// Send a serf run query to the cluster, this is used to ask a node or nodes
// to run a Job.
func (a *Agent) StartJobQuery(j *Job) {
	var params *serf.QueryParam

	job, err := a.store.GetJob(j.Name)

	//Job can be removed and the QuerySchedulerRestart not yet received.
	//In this case, the job will not be found in the store.
	if err == store.ErrKeyNotFound {
		log.Infof("query: Job not found, cancelling this execution")
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

	log.Debug("query: Sending query:%v; job_name:%v; json:%v", QueryStartJob, job.Name, string(rqpJson))

	qr, err := a.serf.Query(QueryStartJob, rqpJson, params)
	if err != nil {
		log.Errorf("query: Sending query error:%v; query:%v", err, QueryStartJob)
	}
	defer qr.Close()

	ackCh := qr.AckCh()
	respCh := qr.ResponseCh()

	for !qr.Finished() {
		select {
		case ack, ok := <-ackCh:
			if ok {
				log.Debug("query: Received ack:%v; from:%v", QueryStartJob, ack)
			}
		case resp, ok := <-respCh:
			if ok {
				log.Debug("query: Received response:%v; query:%v; from:%v", string(resp.Payload), QueryStartJob, resp.From)
				a.upsertJob(resp.Payload)
			}
		}
	}
	log.Infof("query: Done receiving acks and responses:%v", QueryStartJob)
}

func (a *Agent) StopJobQuery(j *Job) {
	var params *serf.QueryParam

	job, err := a.store.GetJob(j.Name)

	//Job can be removed and the QuerySchedulerRestart not yet received.
	//In this case, the job will not be found in the store.
	if err == store.ErrKeyNotFound {
		log.Infof("query: Job not found, cancelling this execution")
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

	log.Debug("query: Sending query:%v; job_name:%v; json:%v", QueryStopJob, job.Name, string(rqpJson))

	qr, err := a.serf.Query(QueryStopJob, rqpJson, params)
	if err != nil {
		log.Errorf("query: Sending query error:%v; query:%v", err, QueryStopJob)
	}
	defer qr.Close()

	ackCh := qr.AckCh()
	respCh := qr.ResponseCh()

	for !qr.Finished() {
		select {
		case ack, ok := <-ackCh:
			if ok {
				log.Debug("query: Received ack:%v; from:%v", QueryStopJob, ack)
			}
		case resp, ok := <-respCh:
			if ok {
				log.Debug("query: Received response:%v; query:%v; from:%v", string(resp.Payload), QueryStopJob, resp.From)
				a.upsertJob(resp.Payload)
			}
		}
	}
	log.Infof("query: Done receiving acks and responses:%v", QueryStopJob)
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
		log.Errorf("query: Error sending the scheduler reload query:%v", err)
	}
	defer qr.Close()

	ackCh := qr.AckCh()
	respCh := qr.ResponseCh()

	for !qr.Finished() {
		select {
		case ack, ok := <-ackCh:
			if ok {
				log.Infof("query: Received ack from:%v", ack)
			}
		case resp, ok := <-respCh:
			if ok {
				log.Infof("query: Received response:%v; from:%v", resp.From, string(resp.Payload))
			}
		}
	}
	log.Infof("query: Done receiving acks and responses:%v", QuerySchedulerRestart)
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
		log.Errorf("query: Error sending query:%v; query:%v", err, QueryRPCConfig)
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
				log.Infof("query: Received ack:%v; from:%v", QueryRPCConfig, ack)
			}
		case resp, ok := <-respCh:
			if ok {
				log.Infof("query: Received response:%v; from:%v; payload:%v", QueryRPCConfig, resp.From, string(resp.Payload))
				rpcAddr = resp.Payload
			}
		}
	}
	log.Infof("query: Done receiving acks and responses:%v", QueryRPCConfig)

	return rpcAddr, nil
}

func (a *Agent) selectServer() serf.Member {
	servers := a.listServers()
	server := servers[rand.Intn(len(servers))]

	return server
}
