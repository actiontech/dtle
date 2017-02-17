package agent

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/rpc"

	"github.com/docker/libkv/store"
	"github.com/hashicorp/serf/serf"
	"github.com/ngaut/log"

	"udup/plugins"
	uconf "udup/config"
)

var (
	ErrForDeletedJob = errors.New("rpc: Received err for a deleted job.")
)

type RPCServer struct {
	agent *Agent
}

func (rpcs *RPCServer) GetJob(jobName string, job *Job) error {
	log.Infof("job:%v,rpc: Received GetJob", jobName)

	j, err := rpcs.agent.store.GetJob(jobName)
	if err != nil {
		return err
	}

	// Copy the data structure
	job.Tags = j.Tags

	return nil
}

func (rpcs *RPCServer) RunProcess(j Job, reply *serf.NodeResponse) error {
	log.Infof("----job:%v,rpc: eceived execution done", j.Name)

	// Load the job from the store
	job, err := rpcs.agent.store.GetJob(j.Name)
	if err != nil {
		if err == store.ErrKeyNotFound {
			log.Warning(ErrForDeletedJob)
			return ErrForDeletedJob
		}
		log.Fatal("rpc:", err)
		return err
	}
	// Lock the job while editing
	if err = job.Lock(); err != nil {
		log.Fatal("rpc:", err)
	}

	// Get the defined output types for the job, and call them
	for k, v := range job.Processors {
		log.Infof("plugin:%v,rpc: Processing execution with plugin", k)
		switch v.Driver {
		case plugins.MysqlDriverAttr:
			{
				driver, err := rpcs.agent.createDriver(v)
				if err != nil {
					return fmt.Errorf("failed to create driver of task '%s': %v",
						job.Name, err)
				}

				errCh := make(chan error)
				v.ErrCh = errCh
				go job.listenOnPanicAbort(v)
				// Start the job
				err = driver.Start(k, v)
				if err != nil {
					return err
				}
			}
		default:
			{
				return fmt.Errorf("Unknown job type : %+v", v.Driver)
			}
		}
	}

	if err := rpcs.agent.store.UpsertJob(job); err != nil {
		log.Fatal("rpc:", err)
	}

	// Release the lock
	if err = job.Unlock(); err != nil {
		log.Fatal("rpc:", err)
	}

	reply.From = rpcs.agent.config.NodeName
	reply.Payload = []byte("saved")

	// If the execution failed, retry it until retries limit (default: don't retry)
	if !job.Success && job.Attempt < job.Retries+1 {
		job.Attempt++

		log.Infof("attempt:%v,attempt:%v,Retrying execution", job.Attempt, job)

		rpcs.agent.RunQuery(job)
		return nil
	}

	// Jobs that have dependent jobs are a bit more expensive because we need to call the Status() method for every execution.
	// Check first if there's dependent jobs and then check for the job status to begin executiong dependent jobs on success.
	if len(job.DependentJobs) > 0 && job.Status() == Success {
		for _, djn := range job.DependentJobs {
			dj, err := rpcs.agent.store.GetJob(djn)
			if err != nil {
				return err
			}
			dj.Run()
		}
	}

	return nil
}

// createDriver makes a driver for the task
func (a *Agent) createDriver(cfg *uconf.DriverConfig) (plugins.Driver, error) {
	driverCtx := plugins.NewDriverContext(cfg.Driver, a.config)
	driver, err := plugins.DiscoverPlugins(cfg.Driver, driverCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to create driver '%s': %v",
			cfg.Driver, err)
	}
	return driver, err
}

var workaroundRPCHTTPMux = 0

func listenRPC(a *Agent) {
	r := &RPCServer{
		agent: a,
	}

	log.Infof("rpc_addr:%v,rpc: Registering RPC server", a.getRPCAddr())

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

func (rpcc *RPCClient) callRunJob(job *Job) error {
	client, err := rpc.DialHTTP("tcp", rpcc.ServerAddr)
	if err != nil {
		log.Infof("err:%v,server_addr:%v,rpc: error dialing.", err, rpcc.ServerAddr)
		return err
	}
	defer client.Close()

	// Synchronous call
	var reply serf.NodeResponse
	err = client.Call("RPCServer.RunProcess", job, &reply)
	if err != nil {
		log.Infof("err:%v,rpc: Error calling RunProcess", err)
		return err
	}
	log.Debug("rpc: from: ", reply.From)

	return nil
}

func (rpcc *RPCClient) GetJob(jobName string) (*Job, error) {
	client, err := rpc.DialHTTP("tcp", rpcc.ServerAddr)
	if err != nil {
		log.Infof("err:%v,server_addr:%v,rpc: error dialing.", err, rpcc.ServerAddr)
		return nil, err
	}
	defer client.Close()

	// Synchronous call
	var job Job
	err = client.Call("RPCServer.GetJob", jobName, &job)
	if err != nil {
		log.Infof("err:%v,rpc: Error calling GetJob", err)
		return nil, err
	}

	return &job, nil
}
