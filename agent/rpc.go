package agent

import (
	"fmt"
	"net/rpc"

	"github.com/hashicorp/serf/serf"
	"github.com/ngaut/log"

	"time"
	uconf "udup/config"
	"udup/plugins"
)

type RPCServer struct {
	agent *Agent
}

type JobResponse struct {
	Payload []*Job
}

func (rpcs *RPCServer) GetJob(jobName string, job *Job) error {
	j, err := rpcs.agent.store.GetJob(jobName)
	if err != nil {
		return err
	}

	// Copy the data structure
	job.Name = j.Name
	job.Failover = j.Failover
	job.agent = j.agent
	job.Processors = j.Processors
	return nil
}

func (rpcs *RPCServer) GetJobs(nodeName string, js *JobResponse) error {
	jobs, err := rpcs.agent.store.GetJobByNode(nodeName)
	if err != nil {
		return err
	}

	// Copy the data structure
	js.Payload = jobs.Payload
	return nil
}

func (rpcs *RPCServer) Upsert(job *Job, reply *serf.NodeResponse) error {
	job.agent = rpcs.agent
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

func (rpcc *RPCClient) startJob(j *Job, k string) error {
	// Get the defined output types for the job, and call them
	log.Infof("Processing execution with plugin:%v", k)

	ts := time.Now()
	if k == plugins.DataSrc {
	OUTER:
		for {
			ej, err := rpcc.CallGetJob(j.Name)
			if err != nil {
				return fmt.Errorf("agent: Error on rpc.GetJob call")
			}

			for _, member := range rpcc.agent.serf.Members() {
				if member.Name == ej.Processors[plugins.DataDest].NodeName &&
					member.Status == serf.StatusAlive && ej.Processors[plugins.DataDest].Running {
					break OUTER
				}
			}

			endTime := time.Now()
			if endTime.Sub(ts) > defaultWaitTime {
				log.Infof("timed out after waiting %s", defaultWaitTime)
				if j.Failover {
					j.Processors[plugins.DataDest].NodeName = j.agent.selectAgent().Name
					for k, _ := range j.Processors {
						j.Processors[k].NatsAddr = rpcc.agent.getNatsAddr(j.Processors[plugins.DataDest].NodeName)
						if err := rpcc.CallUpsertJob(j); err != nil {
							log.Errorf("error on rpc.UpsertJob call")
						}
					}
					j.StartJobQuery(plugins.DataDest)
				} else {
					rpcc.enqueueJob(j)
					return fmt.Errorf("error status")
				}
			}
			time.Sleep(defaultCheckInterval)
		}
	}

	job, err := rpcc.CallGetJob(j.Name)
	if err != nil {
		return fmt.Errorf("agent: Error on rpc.GetJob call")
	}

	errCh := make(chan error)
	job.Processors[k].ErrCh = errCh

	go rpcc.listenOnPanicAbort(job, k)
	job.Processors[k].Running = true

	switch job.Processors[k].Driver {
	case plugins.MysqlDriverAttr:
		{
			if k == plugins.DataDest {
				gtidCh := make(chan string)
				job.Processors[k].GtidCh = gtidCh
				go rpcc.listenOnGtid(job, k)
			} else {
				job.Status = Running
			}
			// Start the job
			go rpcc.setupDriver(job, k, job.Processors[k])
		}
	default:
		{
			return fmt.Errorf("Unknown job type : %+v", job.Processors[k].Driver)
		}
	}

	err = rpcc.CallUpsertJob(job)
	if err != nil {
		return fmt.Errorf("agent: Error on rpc.GetJob call")
	}

	return nil
}

func (rpcc *RPCClient) setupDriver(j *Job, k string, v *uconf.DriverConfig) {
	driver, err := createDriver(v)
	if err != nil {
		v.ErrCh <- fmt.Errorf("failed to create driver of job '%s': %v",
			j, err)

	}
	v.Running = true
	v.Gtid = j.Processors[plugins.DataDest].Gtid
	v.NatsAddr = rpcc.agent.getNatsAddr(j.Processors[plugins.DataDest].NodeName)
	err = driver.Start(j.Name, k, v)
	if err != nil {
		v.ErrCh <- err
	}

	rpcc.agent.processorPlugins[jobDriver{name: j.Name, tp: k}] = driver
}

func (rpcc *RPCClient) stopJob(j *Job) error {
	if err := rpcc.stop(j, Stopped); err != nil {
		return err
	}

	return nil
}

func (rpcc *RPCClient) stop(job *Job, status JobStatus) error {
	// Lock the job while editing
	for k, v := range rpcc.agent.processorPlugins {
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
	if err := rpcc.CallUpsertJob(job); err != nil {
		return fmt.Errorf("agent: Error on rpc.UpsertJob call")
	}

	return nil
}

func (rpcc *RPCClient) enqueueJob(j *Job) error {
	if err := rpcc.stop(j, Queued); err != nil {
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

	for _, job := range jobs.Payload {
		if job.Status == Running {
			if job.Failover {
				if nodeName == job.Processors[plugins.DataDest].NodeName {
					job.StopJobQuery(plugins.DataSrc)
					job.Processors[plugins.DataDest].NodeName = job.agent.selectAgent().Name
					for k, v := range job.Processors {
						if k == plugins.DataSrc && v.NodeName == nodeName {
							job.Processors[k].NodeName = job.agent.selectAgent().Name
						}
						job.Processors[k].NatsAddr = rpcc.agent.getNatsAddr(job.Processors[plugins.DataDest].NodeName)
						if err := rpcc.CallUpsertJob(job); err != nil {
							log.Errorf("agent: Error on rpc.UpsertJob call")
						}
					}
					for k, _ := range job.Processors {
						job.StartJobQuery(k)
					}

				} else {
					job.StopJobQuery(plugins.DataSrc)
					job.Processors[plugins.DataSrc].NodeName = job.agent.selectAgent().Name
					job.Processors[plugins.DataSrc].NatsAddr = rpcc.agent.getNatsAddr(job.Processors[plugins.DataDest].NodeName)
					if err := rpcc.CallUpsertJob(job); err != nil {
						log.Errorf("agent: Error on rpc.UpsertJob call")
					}
					job.StartJobQuery(plugins.DataSrc)
				}
			} else {
				rpcc.enqueueJob(job)
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
	agent      *Agent
}

func (rpcc *RPCClient) listenOnPanicAbort(job *Job, k string) {
	for err := range job.Processors[k].ErrCh {
		if err != nil {
			log.Errorf("Run failed: %v", err)
			rpcc.enqueueJob(job)
		}
	}
}

func (rpcc *RPCClient) listenOnGtid(job *Job, k string) {
	for gtid := range job.Processors[k].GtidCh {
		if gtid != "" {
			j, err := rpcc.CallGetJob(job.Name)
			if err != nil {
				log.Errorf("agent: Error on rpc.GetJob call")
			}

			j.Processors[k].Gtid = gtid
			if err := rpcc.CallUpsertJob(j); err != nil {
				log.Errorf("agent: Error on rpc.UpsertJob call")
			}
		}
	}
}

func (rpcc *RPCClient) CallGetJob(jobName string) (*Job, error) {
	if rpcc.agent.config.Server.Enabled {
		j, err := rpcc.agent.store.GetJob(jobName)
		if err != nil {
			log.Errorf("failed to get job '%s': %v",
				jobName, err)
		}
		return j, err
	}
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
	if rpcc.agent.config.Server.Enabled {
		js, err := rpcc.agent.store.GetJobByNode(nodeName)
		if err != nil {
			log.Errorf("failed to get job '%s': %v",
				nodeName, err)
		}
		return js, err
	}
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
	if rpcc.agent.config.Server.Enabled {
		err := rpcc.agent.store.UpsertJob(j)
		if err != nil {
			log.Errorf("failed to upsert job '%s': %v",
				j.Name, err)
			return err
		}
		return nil
	}
	client, err := rpc.DialHTTP("tcp", rpcc.ServerAddr)
	if err != nil {
		log.Errorf("Error dialing: %v,server_addr:%v.", err, rpcc.ServerAddr)
		return err
	}
	defer client.Close()

	// Synchronous call
	var reply serf.NodeResponse
	err = client.Call("RPCServer.Upsert", j, &reply)
	if err != nil {
		log.Errorf("Error calling Upsert: %v", err)
		return err
	}

	return nil
}
