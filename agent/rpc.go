package agent

import (
	"errors"
	"net"
	"net/http"
	"net/rpc"

	"github.com/ngaut/log"
	"github.com/docker/libkv/store"
	"github.com/hashicorp/serf/serf"
)

var (
	ErrExecutionDoneForDeletedJob = errors.New("rpc: Received execution done for a deleted job.")
)

type RPCServer struct {
	agent *Agent
}

func (rpcs *RPCServer) GetJob(jobName string, job *Job) error {
	log.Infof("job:%v,rpc: Received GetJob",jobName)

	j, err := rpcs.agent.store.JobByName(jobName)
	if err != nil {
		return err
	}

	// Copy the data structure
	job.Tags = j.Tags

	return nil
}

func (rpcs *RPCServer) ExecutionDone(execution Job, reply *serf.NodeResponse) error {
	log.Infof("job:%v,rpc: eceived execution done",execution.Name)

	// Load the job from the store
	job, err := rpcs.agent.store.JobByName(execution.Name)
	if err != nil {
		if err == store.ErrKeyNotFound {
			log.Warning(ErrExecutionDoneForDeletedJob)
			return ErrExecutionDoneForDeletedJob
		}
		log.Fatal("rpc:", err)
		return err
	}
	// Lock the job while editing
	if err = job.Lock(); err != nil {
		log.Fatal("rpc:", err)
	}

	// Get the defined output types for the job, and call them
	//process

	if err := rpcs.agent.store.UpsertJob(job); err != nil {
		log.Fatal("rpc:", err)
	}

	// Release the lock
	if err = job.Unlock(); err != nil {
		log.Fatal("rpc:", err)
	}

	reply.From = rpcs.agent.config.NodeName
	reply.Payload = []byte("saved")

	// Jobs that have dependent jobs are a bit more expensive because we need to call the Status() method for every execution.
	// Check first if there's dependent jobs and then check for the job status to begin executiong dependent jobs on success.
	if len(job.DependentJobs) > 0 && job.Status() == Success {
		for _, djn := range job.DependentJobs {
			dj, err := rpcs.agent.store.JobByName(djn)
			if err != nil {
				return err
			}
			dj.Run()
		}
	}

	return nil
}

var workaroundRPCHTTPMux = 0

func listenRPC(a *Agent) {
	r := &RPCServer{
		agent: a,
	}

	log.Infof("rpc_addr:%v,rpc: Registering RPC server",a.getRPCAddr())

	rpc.Register(r)

	// ===== workaround ==========
	// This is needed mainly for testing
	// see: https://github.com/golang/go/issues/13395
	oldMux := http.DefaultServeMux
	if workaroundRPCHTTPMux > 0 {
		mux := http.NewServeMux()
		http.DefaultServeMux = mux
	}
	workaroundRPCHTTPMux = workaroundRPCHTTPMux + 1
	// ===========================

	rpc.HandleHTTP()

	// workaround
	http.DefaultServeMux = oldMux

	l, e := net.Listen("tcp", a.getRPCAddr())
	if e != nil {
		log.Fatal("rpc:", e)
	}
	go http.Serve(l, nil)
}

type RPCClient struct {
	//Addres of the server to call
	ServerAddr string
}

func (rpcc *RPCClient) callExecutionDone(job *Job) error {
	client, err := rpc.DialHTTP("tcp", rpcc.ServerAddr)
	if err != nil {
		log.Infof("err:%v,server_addr:%v,rpc: error dialing.",err,rpcc.ServerAddr)
		return err
	}
	defer client.Close()

	// Synchronous call
	var reply serf.NodeResponse
	err = client.Call("RPCServer.ExecutionDone", job, &reply)
	if err != nil {
		log.Infof("err:%v,rpc: Error calling ExecutionDone",err)
		return err
	}
	log.Debug("rpc: from: ", reply.From)

	return nil
}

func (rpcc *RPCClient) GetJob(jobName string) (*Job, error) {
	client, err := rpc.DialHTTP("tcp", rpcc.ServerAddr)
	if err != nil {
		log.Infof("err:%v,server_addr:%v,rpc: error dialing.",err,rpcc.ServerAddr)
		return nil, err
	}
	defer client.Close()

	// Synchronous call
	var job Job
	err = client.Call("RPCServer.GetJob", jobName, &job)
	if err != nil {
		log.Infof("err:%v,rpc: Error calling GetJob",err)
		return nil, err
	}

	return &job, nil
}
