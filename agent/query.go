package agent

import (
	"encoding/json"
	"math/rand"

	"github.com/docker/libkv/store"
	"github.com/hashicorp/serf/serf"
	"github.com/ngaut/log"
)

const (
	QueryStartJob   = "start:job"
	QueryStopJob    = "stop:job"
	QueryEnqueueJob = "equeue:job"
	QueryRPCConfig  = "rpc:config"
)

type RunQueryParam struct {
	JobName string `json:"job_name"`
	Type    string `json:"type"`
	RPCAddr string `json:"rpc_addr"`
}

// Send a serf run query to the cluster, this is used to ask a node or nodes
// to run a Job.
func (a *Agent) StartJobQuery(j *Job, k string) {
	var params *serf.QueryParam
	params = &serf.QueryParam{
		FilterNodes: []string{j.Processors[k].NodeName},
		RequestAck:  true,
	}

	rqp := &RunQueryParam{
		JobName: j.Name,
		Type:    k,
		RPCAddr: a.getRPCAddr(),
	}
	rqpJson, _ := json.Marshal(rqp)

	log.Debug("Sending query:%v; job_name:%v; json:%v", QueryStartJob, j.Name, string(rqpJson))

	qr, err := a.serf.Query(QueryStartJob, rqpJson, params)
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
				//a.upsertJob(resp.Payload,Running)
			}
		}
	}
	log.Infof("Done receiving acks and responses:%v", QueryStartJob)
}

func (a *Agent) StopJobQuery(j *Job, k string) {
	var params *serf.QueryParam

	job, err := a.store.GetJob(j.Name)

	//In this case, the job will not be found in the store.
	if err == store.ErrKeyNotFound {
		log.Infof("Job not found, cancelling this execution")
	}

	var statusAlive bool
	for _, m := range a.serf.Members() {
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
		if err := a.store.UpsertJob(job); err != nil {
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
		JobName: j.Name,
		Type:    k,
		RPCAddr: a.getRPCAddr(),
	}
	rqpJson, _ := json.Marshal(rqp)

	log.Debug("Sending query:%v; job_name:%v; json:%v", QueryStopJob, job.Name, string(rqpJson))

	qr, err := a.serf.Query(QueryStopJob, rqpJson, params)
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
				//a.upsertJob(resp.Payload,Stopped)
			}
		}
	}
	log.Infof("Done receiving acks and responses:%v", QueryStopJob)
}

func (a *Agent) EnqueueJobQuery(j *Job) {
	log.Infof("agent EnqueueJobQuery:%v", a.config.NodeName)
	var params *serf.QueryParam

	job, err := a.store.GetJob(j.Name)

	//In this case, the job will not be found in the store.
	if err == store.ErrKeyNotFound {
		log.Infof("Job not found, cancelling this execution")
	}

	var statusAlive bool
	/*for _, m := range a.serf.Members() {
		if m.Name == job.NodeName && m.Status == serf.StatusAlive {
			statusAlive = true
		}
	}*/
	if !statusAlive {
		// Lock the job while editing
		if err = job.Lock(); err != nil {
			log.Fatal(err)
		}

		job.Status = Queued
		for _, p := range job.Processors {
			p.Running = false
		}
		if err := a.store.UpsertJob(job); err != nil {
			log.Fatal(err)
		}

		// Release the lock
		if err = job.Unlock(); err != nil {
			log.Fatal(err)
		}
		return
	}

	/*// Jobs that have dependent jobs are a bit more expensive because we need to call the Status() method for every execution.
	// Check first if there's dependent jobs and then check for the job status to begin executiong dependent jobs on success.
	if len(job.DependentJobs) > 0 {
		for _, djn := range job.DependentJobs {
			dj, err := a.store.GetJob(djn)
			if err != nil {
				return
			}
			statusAlive = false
			for _, m := range a.serf.Members() {
				if m.Name == dj.NodeName && m.Status == serf.StatusAlive {
					statusAlive = true
				}
			}
			if !statusAlive {
				return
			}
			dj.Enqueue()
		}
	}

	params = &serf.QueryParam{
		FilterNodes: []string{j.NodeName},
		RequestAck:  true,
	}*/

	rqp := &RunQueryParam{
		/*Type:     k,
		DriverConfig:v,*/
		RPCAddr: a.getRPCAddr(),
	}
	rqpJson, _ := json.Marshal(rqp)

	log.Debug("Sending query:%v; job_name:%v; json:%v", QueryEnqueueJob, job.Name, string(rqpJson))

	qr, err := a.serf.Query(QueryEnqueueJob, rqpJson, params)
	if err != nil {
		log.Errorf("Sending query error:%v; query:%v", err, QueryEnqueueJob)
	}
	defer qr.Close()

	ackCh := qr.AckCh()
	respCh := qr.ResponseCh()

	for !qr.Finished() {
		select {
		case ack, ok := <-ackCh:
			if ok {
				log.Debug("Received ack:%v; from:%v", QueryEnqueueJob, ack)
			}
		case resp, ok := <-respCh:
			if ok {
				log.Debug("Received response:%v; query:%v; from:%v", string(resp.Payload), QueryEnqueueJob, resp.From)
				a.upsertJob(resp.Payload, Queued)
			}
		}
	}
	log.Infof("Done receiving acks and responses:%v", QueryEnqueueJob)
}

func (a *Agent) upsertJob(payload []byte, status JobStatus) *Job {
	var j Job
	if err := json.Unmarshal(payload, &j); err != nil {
		log.Fatal(err)
	}

	/*j.Agent = a
	// Lock the job while editing
	if err := j.Lock(); err != nil {
		log.Fatal("rpc:", err)
	}

	j.Status = status
	if status == Running {
		for _, p := range j.Processors {
			p.Running = true
		}
	}else {
		for _, p := range j.Processors {
			p.Running = false
		}
	}*/
	// Save the new execution to store
	if err := a.store.UpsertJob(&j); err != nil {
		log.Fatal(err)
	}

	// Release the lock
	/*if err := j.Unlock(); err != nil {
		log.Fatal("rpc:", err)
	}*/

	return &j
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
