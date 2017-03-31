package agent

import (
	"encoding/json"
	"math/rand"
	"strconv"

	"github.com/Sirupsen/logrus"
	"github.com/docker/libkv/store"
	"github.com/hashicorp/serf/serf"

	ulog "udup/logger"
)

const (
	QueryStartJob   = "start:job"
	QueryStopJob    = "stop:job"
	QueryEnqueueJob = "enqueue:job"
	QueryRPCConfig  = "rpc:config"
	QueryGenId      = "generate:id"
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

	ulog.Logger.WithFields(logrus.Fields{
		"query":    QueryStartJob,
		"job_name": j.Name,
		"json":     string(rqpJson),
	}).Debug("agent: Sending query")

	qr, err := j.agent.serf.Query(QueryStartJob, rqpJson, params)
	if err != nil {
		ulog.Logger.WithField("query", QueryStartJob).WithError(err).Fatal("agent: Sending query error")
	}
	defer qr.Close()

	ackCh := qr.AckCh()
	respCh := qr.ResponseCh()

	for !qr.Finished() {
		select {
		case ack, ok := <-ackCh:
			if ok {
				ulog.Logger.WithFields(logrus.Fields{
					"query": QueryStartJob,
					"from":  ack,
				}).Debug("agent: Received ack")
			}
		case resp, ok := <-respCh:
			if ok {
				ulog.Logger.WithFields(logrus.Fields{
					"query":    QueryStartJob,
					"from":     resp.From,
					"response": string(resp.Payload),
				}).Debug("agent: Received response")
			}
		}
	}
	ulog.Logger.WithFields(logrus.Fields{
		"query": QueryStartJob,
	}).Debug("agent: Done receiving acks and responses")
}

func (j *Job) StopJobQuery(k string) {
	var params *serf.QueryParam

	job, err := j.agent.store.GetJob(j.Name)

	//In this case, the job will not be found in the store.
	if err == store.ErrKeyNotFound {
		ulog.Logger.Debug("Job not found, cancelling this execution")
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
			ulog.Logger.Fatal(err)
		}

		job.Status = Queued
		for _, p := range job.Processors {
			p.Running = false
		}
		if err := j.agent.store.UpsertJob(job); err != nil {
			ulog.Logger.Fatal(err)
		}

		// Release the lock
		if err = job.Unlock(); err != nil {
			ulog.Logger.Fatal(err)
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

	ulog.Logger.WithFields(logrus.Fields{
		"query":    QueryStopJob,
		"job_name": j.Name,
		"json":     string(rqpJson),
	}).Debug("agent: Sending query")

	qr, err := j.agent.serf.Query(QueryStopJob, rqpJson, params)
	if err != nil {
		ulog.Logger.WithField("query", QueryStopJob).WithError(err).Fatal("agent: Sending query error")
	}
	defer qr.Close()

	ackCh := qr.AckCh()
	respCh := qr.ResponseCh()

	for !qr.Finished() {
		select {
		case ack, ok := <-ackCh:
			if ok {
				ulog.Logger.WithFields(logrus.Fields{
					"query": QueryStopJob,
					"from":  ack,
				}).Debug("agent: Received ack")
			}
		case resp, ok := <-respCh:
			if ok {
				ulog.Logger.WithFields(logrus.Fields{
					"query":    QueryStopJob,
					"from":     resp.From,
					"response": string(resp.Payload),
				}).Debug("agent: Received response")
			}
		}
	}

	ulog.Logger.WithFields(logrus.Fields{
		"query": QueryStopJob,
	}).Debug("agent: Done receiving acks and responses")
}

func (j *Job) EnqueueJobQuery(k string) {
	var params *serf.QueryParam

	job, err := j.agent.store.GetJob(j.Name)

	//In this case, the job will not be found in the store.
	if err == store.ErrKeyNotFound {
		ulog.Logger.Debug("Job not found, cancelling this execution")
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
			ulog.Logger.Fatal(err)
		}

		job.Status = Queued
		for _, p := range job.Processors {
			p.Running = false
		}
		if err := j.agent.store.UpsertJob(job); err != nil {
			ulog.Logger.Fatal(err)
		}

		// Release the lock
		if err = job.Unlock(); err != nil {
			ulog.Logger.Fatal(err)
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

	ulog.Logger.WithFields(logrus.Fields{
		"query":    QueryEnqueueJob,
		"job_name": j.Name,
		"json":     string(rqpJson),
	}).Debug("agent: Sending query")

	qr, err := j.agent.serf.Query(QueryEnqueueJob, rqpJson, params)
	if err != nil {
		ulog.Logger.WithField("query", QueryEnqueueJob).WithError(err).Fatal("agent: Sending query error")
	}
	defer qr.Close()

	ackCh := qr.AckCh()
	respCh := qr.ResponseCh()

	for !qr.Finished() {
		select {
		case ack, ok := <-ackCh:
			if ok {
				ulog.Logger.WithFields(logrus.Fields{
					"query": QueryEnqueueJob,
					"from":  ack,
				}).Debug("agent: Received ack")
			}
		case resp, ok := <-respCh:
			if ok {
				ulog.Logger.WithFields(logrus.Fields{
					"query":    QueryEnqueueJob,
					"from":     resp.From,
					"response": string(resp.Payload),
				}).Debug("agent: Received response")
			}
		}
	}
	ulog.Logger.WithFields(logrus.Fields{
		"query": QueryEnqueueJob,
	}).Debug("agent: Done receiving acks and responses")
}

func (a *Agent) upsertJob(payload []byte) *Job {
	var j Job
	if err := json.Unmarshal(payload, &j); err != nil {
		ulog.Logger.Fatal(err)
	}

	// Save the new execution to store
	if err := a.store.UpsertJob(&j); err != nil {
		ulog.Logger.Fatal(err)
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
		ulog.Logger.WithField("query", QueryGenId).WithError(err).Fatal("agent: Sending query error")
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
				ulog.Logger.WithFields(logrus.Fields{
					"query": QueryGenId,
					"from":  ack,
				}).Debug("agent: Received ack")
			}
		case resp, ok := <-respCh:
			if ok {
				ulog.Logger.WithFields(logrus.Fields{
					"query":    QueryGenId,
					"from":     resp.From,
					"response": string(resp.Payload),
				}).Debug("agent: Received response")
				serverId = resp.Payload
			}
		}
	}
	ulog.Logger.WithFields(logrus.Fields{
		"query": QueryGenId,
	}).Debug("agent: Done receiving acks and responses")

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
		ulog.Logger.WithField("query", QueryRPCConfig).WithError(err).Fatal("agent: Sending query error")
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
				ulog.Logger.WithFields(logrus.Fields{
					"query": QueryRPCConfig,
					"from":  ack,
				}).Debug("agent: Received ack")
			}
		case resp, ok := <-respCh:
			if ok {
				ulog.Logger.WithFields(logrus.Fields{
					"query":    QueryRPCConfig,
					"from":     resp.From,
					"response": string(resp.Payload),
				}).Debug("agent: Received response")
				rpcAddr = resp.Payload
			}
		}
	}
	ulog.Logger.WithFields(logrus.Fields{
		"query": QueryRPCConfig,
	}).Debug("agent: Done receiving acks and responses")

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
