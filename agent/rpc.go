package agent

import (
	"errors"
	"fmt"
	"net/rpc"

	"github.com/hashicorp/serf/serf"
	"github.com/ngaut/log"

	uconf "udup/config"
	"udup/plugins"
)

var (
	ErrForDeletedJob = errors.New("Received err for a deleted job.")
)

type RPCServer struct {
	agent *Agent
}

type JobResponse struct {
	Payload	 []*Job
}

func (rpcs *RPCServer) GetJob(jobName string, job *Job) error {
	j, err := rpcs.agent.store.GetJob(jobName)
	if err != nil {
		return err
	}

	// Copy the data structure
	job.Name = j.Name
	job.NodeName = j.NodeName
	job.Agent = j.Agent
	job.Processors = j.Processors
	return nil
}

func (rpcs *RPCServer) GetJobs(nodeName string,js *JobResponse) error {
	jobs, err := rpcs.agent.store.GetJobByNode(nodeName)
	if err != nil {
		return err
	}

	// Copy the data structure
	js.Payload = jobs.Payload
	return nil
}

func (rpcs *RPCServer) Upsert(job *Job,reply *serf.NodeResponse) error {
	job.Agent = rpcs.agent
	// Lock the job while editing
	if err := job.Lock(); err != nil {
		log.Fatal("rpc:", err)
	}
	if err := rpcs.agent.store.UpsertJob(job); err != nil {
		return err
	}
	// Release the lock
	if err := job.Unlock(); err != nil {
		log.Fatal("rpc:", err)
	}
	reply.From = rpcs.agent.config.NodeName
	reply.Payload = []byte("saved")

	return nil
}

func (rpcc *RPCClient) startJob(job *Job) error {
	// Jobs that have dependent jobs are a bit more expensive because we need to call the Status() method for every execution.
	// Check first if there's dependent jobs and then check for the job status to begin executiong dependent jobs on success.
	if job.ParentJob != "" {
	OUTER:
		for {
			pj, err := rpcc.CallGetJob(job.ParentJob)
			if err != nil {
				return fmt.Errorf("agent: Error on rpc.GetJob call")
			}

			for _, m := range job.Agent.serf.Members() {
				if m.Name == pj.NodeName && m.Status == serf.StatusAlive && pj.Status == Running{
					break OUTER
				}
			}
		}
	}

	// Get the defined output types for the job, and call them
	for k, v := range job.Processors {
		log.Infof("Processing execution with plugin:%v", k)
		errCh := make(chan error)
		v.ErrCh = errCh
		go rpcc.listenOnPanicAbort(job,v)

		switch v.Driver {
		case plugins.MysqlDriverAttr:
			{
				gtidCh := make(chan string)
				v.GtidCh = gtidCh
				go rpcc.listenOnGtid(job,v)
				// Start the job
				go rpcc.setupDriver(k, v, job)
			}
		default:
			{
				return fmt.Errorf("Unknown job type : %+v", v.Driver)
			}
		}
	}

	job.Status = Running

	err := rpcc.CallUpsertJob(job)
	if err != nil {
		return fmt.Errorf("agent: Error on rpc.GetJob call")
	}

	return nil
}

func (rpcc *RPCClient) setupDriver(k string, v *uconf.DriverConfig, j *Job) {
	var subject string
	for {
		if k == plugins.ProcessorTypeApply {
			subject = j.Name
			break
		}

		if j.ParentJob != "" {
			pj, err := rpcc.CallGetJob(j.ParentJob)
			if err != nil {
				log.Errorf("agent: Error on rpc.GetJob call")
			}
			if pj.Processors["apply"].Running == true {
				v.Gtid = pj.Processors["apply"].Gtid
				subject = pj.Name
				break
			}
		} else {
			if j.Processors["apply"].Running == true {
				v.Gtid = j.Processors["apply"].Gtid
				break
			}
		}
	}

	driver, err := createDriver(v)
	if err != nil {
		v.ErrCh <- fmt.Errorf("failed to create driver of job '%s': %v",
			j.Name, err)

	}

	err = driver.Start(subject,k, v)
	if err != nil {
		v.ErrCh <- err
	}
	v.Running = true
	err = rpcc.CallUpsertJob(j)
	if err != nil {
		log.Errorf("agent: Error on rpc.UpsertJob call")
	}

	j.Agent.processorPlugins[jobDriver{name: j.Name, tp: k}] = driver
}

func (rpcc *RPCClient) stopJob(j *Job) error {
	if err := rpcc.Stop(j,Stopped); err != nil {
		return err
	}

	return nil
}

func (rpcc *RPCClient) Stop(job *Job,status JobStatus) error {
	// Lock the job while editing
	for k, v := range job.Agent.processorPlugins {
		if k.name == job.Name {
			if err := v.Stop(k.tp); err != nil {
				return err
			}
		}
	}

	job.Status = status
	for _, p := range job.Processors {
		p.Running = false
	}
	if err := rpcc.CallUpsertJob(job);err != nil {
		return fmt.Errorf("agent: Error on rpc.UpsertJob call")
	}

	return nil
}

func (rpcc *RPCClient) enqueueJob(j *Job) error {
	for _, djn := range j.DependentJobs {
		dj, err := rpcc.CallGetJob(djn)
		if err != nil {
			return fmt.Errorf("agent: Error on rpc.GetJob call")
		}

		if err := rpcc.Stop(dj,Queued); err != nil {
			return err
		}
	}
	if err := rpcc.Stop(j,Queued); err != nil {
		return err
	}

	return nil
}

func (rpcc *RPCClient) enqueueJobs(nodeName string) error {
	// Load the job from the store
	jobs, err := rpcc.CallGetJobs(nodeName)
	if err != nil {
		return err
	}

	for _,job :=range jobs.Payload {
		if job.Status == Running{
			rpcc.enqueueJob(job)

		}
	}

	return nil
}

func (rpcc *RPCClient) dequeueJobs(nodeName string,a *Agent) error {
	// Load the job from the store
	jobs, err := rpcc.CallGetJobs(nodeName)
	if err != nil {
		return err
	}

	for _,j :=range jobs.Payload {
		if j.ParentJob != "" {
			pj, err := rpcc.CallGetJob(j.ParentJob)
			if err != nil {
				log.Errorf("failed to get parent job '%s': %v",
					j.ParentJob, err)
			}
			if pj.NodeName == nodeName && a.config.NodeName == nodeName && pj.Status == Queued {
				if err := rpcc.startJob(pj); err != nil {
					log.Errorf("Error dequeue job command,err:%v", err)
				}
			}
		}else {
			if a.config.NodeName == nodeName && j.Status == Queued {
				if err := rpcc.startJob(j); err != nil {
					log.Errorf("Error dequeue job command,err:%v", err)
				}
			}
		}
	}

	return nil
}

// createDriver makes a driver for the task
func createDriver(cfg *uconf.DriverConfig) (plugins.Driver, error) {
	driverCtx := plugins.NewDriverContext(cfg.Driver, cfg)
	driver, err := plugins.DiscoverPlugins(cfg.Driver, driverCtx)
	if err != nil {
		return nil, fmt.Errorf("Failed to create driver '%s': %v",
			cfg.Driver, err)
	}
	return driver, err
}

type RPCClient struct {
	//Addres of the server to call
	ServerAddr string
}


func (rpcc *RPCClient) listenOnPanicAbort(job *Job,cfg *uconf.DriverConfig) {
	err := <-cfg.ErrCh
	log.Errorf("Run failed: %v", err)
	rpcc.enqueueJob(job)
}

func (rpcc *RPCClient) listenOnGtid(j *Job,cfg *uconf.DriverConfig) {
	for gtid := range cfg.GtidCh {
		j.Processors["apply"].Gtid = gtid
		if err := rpcc.CallUpsertJob(j);err != nil {
			log.Errorf("agent: Error on rpc.UpsertJob call")
		}
	}
}

func (rpcc *RPCClient) CallGetJob(jobName string) (*Job, error) {
	client, err := rpc.DialHTTP("tcp", rpcc.ServerAddr)
	if err != nil {
		log.Errorf("Error dialing: %v,server_addr:%v.", err, rpcc.ServerAddr)
		return nil, err
	}
	defer client.Close()

	// Synchronous call
	var job Job
	err = client.Call("RPCServer.GetJob", jobName, &job)
	if err != nil {
		log.Errorf("Error calling GetJob: %v", err)
		return nil, err
	}

	return &job, nil
}

func (rpcc *RPCClient) CallGetJobs(nodeName string) (*JobResponse, error) {
	client, err := rpc.DialHTTP("tcp", rpcc.ServerAddr)
	if err != nil {
		log.Errorf("Error dialing: %v,server_addr:%v.", err, rpcc.ServerAddr)
		return nil, err
	}
	defer client.Close()

	// Synchronous call
	var jobs JobResponse
	err = client.Call("RPCServer.GetJobs", nodeName, &jobs)
	if err != nil {
		log.Errorf("Error calling GetJobs: %v", err)
		return nil, err
	}

	return &jobs, nil
}

func (rpcc *RPCClient) CallUpsertJob(j *Job) error {
	client, err := rpc.DialHTTP("tcp", rpcc.ServerAddr)
	if err != nil {
		log.Errorf("Error dialing: %v,server_addr:%v.", err, rpcc.ServerAddr)
		return err
	}
	defer client.Close()

	// Synchronous call
	var reply serf.NodeResponse
	err = client.Call("RPCServer.Upsert", j,&reply)
	if err != nil {
		log.Errorf("Error calling Upsert: %v", err)
		return  err
	}

	return  nil
}