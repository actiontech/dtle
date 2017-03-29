package agent

import (
	"encoding/json"
	"math/rand"
	"strconv"

	"github.com/docker/libkv/store"
	"github.com/hashicorp/serf/serf"
	"github.com/ngaut/log"
)

const (
	QueryStartJob  = "start:job"
	QueryStopJob   = "stop:job"
	QueryRPCConfig = "rpc:config"
	QueryGenId     = "generate:id"
)

type RunQueryParam struct {
	Job     *Job   `json:"job"`
	Type    string `json:"type"`
	RPCAddr string `json:"rpc_addr"`
}

// Send a serf run query to the cluster, this is used to ask a node or nodes
// to run a Job.
func (j *Job) StartJobQuery(k string) {
	var params *serf.QueryParam
	params = &serf.QueryParam{
		FilterNodes: []string{j.Processors[k].NodeName},
		RequestAck:  true,
	}

	rqp := &RunQueryParam{
		Job:     j,
		Type:    k,
		RPCAddr: j.agent.getRPCAddr(),
	}
	rqpJson, _ := json.Marshal(rqp)

	log.Debug("Sending query:%v; job_name:%v; json:%v", QueryStartJob, j.Name, string(rqpJson))

	qr, err := j.agent.serf.Query(QueryStartJob, rqpJson, params)
	if err != nil {
		log.Errorf("Sending query error:%v; query:%v", err, QueryStartJob)
	}
	defer qr.Close()

	ackCh := qr.AckCh()
	respCh := qr.ResponseCh()

	for !qr.Finished() {
		select {
		case ack, ok := <-ackCh:
			if ok {
				log.Infof("Received ack:%v; from:%v", QueryStartJob, ack)
			}
		case resp, ok := <-respCh:
			if ok {
				log.Infof("Received response:%v; query:%v; from:%v", string(resp.Payload), QueryStartJob, resp.From)
				//j.agent.upsertJob(resp.Payload)
			}
		}
	}
	log.Infof("Done receiving acks and responses:%v", QueryStartJob)
}

func (j *Job) StopJobQuery(k string) {
	log.Infof("agent StopJobQuery:%v", j.agent.config.NodeName)
	var params *serf.QueryParam

	job, err := j.agent.store.GetJob(j.Name)

	//In this case, the job will not be found in the store.
	if err == store.ErrKeyNotFound {
		log.Infof("Job not found, cancelling this execution")
	}

	var statusAlive bool
	for _, m := range j.agent.serf.Members() {
		if m.Name == j.Processors[k].NodeName && m.Status == serf.StatusAlive {
			statusAlive = true
		}
	}
	if !statusAlive {
		// Lock the job while editing
		if err = job.Lock(); err != nil {
			log.Fatal(err)
		}

		job.Status = Queued
		for _, p := range job.Processors {
			p.Running = false
		}
		if err := j.agent.store.UpsertJob(job); err != nil {
			log.Fatal(err)
		}

		// Release the lock
		if err = job.Unlock(); err != nil {
			log.Fatal(err)
		}
		return
	}

	params = &serf.QueryParam{
		FilterNodes: []string{j.Processors[k].NodeName},
		RequestAck:  true,
	}

	rqp := &RunQueryParam{
		Job:     j,
		RPCAddr: j.agent.getRPCAddr(),
	}
	rqpJson, _ := json.Marshal(rqp)

	log.Debug("Sending query:%v; job_name:%v; json:%v", QueryStopJob, job.Name, string(rqpJson))

	qr, err := j.agent.serf.Query(QueryStopJob, rqpJson, params)
	if err != nil {
		log.Errorf("Sending query error:%v; query:%v", err, QueryStopJob)
	}
	defer qr.Close()

	ackCh := qr.AckCh()
	respCh := qr.ResponseCh()

	for !qr.Finished() {
		select {
		case ack, ok := <-ackCh:
			if ok {
				log.Debug("Received ack:%v; from:%v", QueryStopJob, ack)
			}
		case resp, ok := <-respCh:
			if ok {
				log.Debug("Received response:%v; query:%v; from:%v", string(resp.Payload), QueryStopJob, resp.From)
				//j.agent.upsertJob(resp.Payload)
			}
		}
	}
	log.Infof("Done receiving acks and responses:%v", QueryStopJob)
}

func (a *Agent) upsertJob(payload []byte) *Job {
	var j Job
	if err := json.Unmarshal(payload, &j); err != nil {
		log.Fatal(err)
	}

	// Save the new execution to store
	if err := a.store.UpsertJob(&j); err != nil {
		log.Fatal(err)
	}

	return &j
}

func (a *Agent) genServerId(leaderName string) (uint32, error) {
	if a.config.NodeName == leaderName {
		serverID, err := a.idWorker.NextId()
		if err != nil {
			return 0, err
		}
		return serverID, nil
	}
	params := &serf.QueryParam{
		FilterNodes: []string{leaderName},
		RequestAck:  true,
	}

	qr, err := a.serf.Query(QueryGenId, nil, params)
	if err != nil {
		log.Errorf("Error sending query:%v; query:%v", err, QueryGenId)
		return 0, err
	}
	defer qr.Close()

	ackCh := qr.AckCh()
	respCh := qr.ResponseCh()

	var serverId []byte
	for !qr.Finished() {
		select {
		case ack, ok := <-ackCh:
			if ok {
				log.Infof("Received ack:%v; from:%v", QueryGenId, ack)
			}
		case resp, ok := <-respCh:
			if ok {
				log.Infof("Received response:%v; from:%v; payload:%v", QueryGenId, resp.From, string(resp.Payload))
				serverId = resp.Payload
			}
		}
	}
	log.Infof("Done receiving acks and responses:%v", QueryGenId)

	uid, err := strconv.ParseUint(string(serverId), 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(uid), nil
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

	qr, err := a.serf.Query(QueryRPCConfig, nil, params)
	if err != nil {
		log.Errorf("Error sending query:%v; query:%v", err, QueryRPCConfig)
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
				log.Infof("Received ack:%v; from:%v", QueryRPCConfig, ack)
			}
		case resp, ok := <-respCh:
			if ok {
				log.Infof("Received response:%v; from:%v; payload:%v", QueryRPCConfig, resp.From, string(resp.Payload))
				rpcAddr = resp.Payload
			}
		}
	}
	log.Infof("Done receiving acks and responses:%v", QueryRPCConfig)

	return rpcAddr, nil
}

func (a *Agent) selectServer() serf.Member {
	servers := a.listServers()
	server := servers[rand.Intn(len(servers))]

	return server
}

func (a *Agent) selectAgent() serf.Member {
	agents := a.listAlivedMembers()
	agent := agents[rand.Intn(len(agents))]

	return agent
}
