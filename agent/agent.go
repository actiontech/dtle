package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/docker/leadership"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/serf/serf"
	gnatsd "github.com/nats-io/gnatsd/server"
	stand "github.com/nats-io/nats-streaming-server/server"

	uconf "udup/config"
	ulog "udup/logger"
	"udup/plugins"
	uutil "udup/plugins/mysql/util"
)

var (
	// Error thrown on obtained leader from store is not found in member list
	ErrLeaderNotFound = errors.New("No member leader found in member list")
)

const (
	defaultCheckInterval = 5 * time.Second
	defaultWaitTime      = 60 * time.Second
	defaultRecoverTime   = 10 * time.Second
	defaultLeaderTTL     = 20 * time.Second
)

type Agent struct {
	config    *uconf.Config
	serf      *serf.Serf
	store     *Store
	eventCh   chan serf.Event
	candidate *leadership.Candidate
	stand     *stand.StanServer
	idWorker  *uutil.IdWorker

	processorPlugins map[jobDriver]plugins.Driver
	shutdown         bool
	shutdownCh       chan struct{}
	shutdownLock     sync.Mutex
}

type jobDriver struct {
	name string
	tp   string
}

// NewAgent is used to create a new agent with the given configuration
func NewAgent(config *uconf.Config) (*Agent, error) {
	a := &Agent{
		config:           config,
		processorPlugins: make(map[jobDriver]plugins.Driver),
		shutdownCh:       make(chan struct{}),
	}

	var err error
	if err = a.setupNatsServer(); err != nil {
		return nil, err
	}

	// Initialize the wan Serf
	a.serf, err = a.setupSerf()
	if err != nil {
		a.Shutdown()
		return nil, fmt.Errorf("failed to start serf: %v", err)
	}

	// Join startup nodes if specified
	if err := a.startupJoin(config); err != nil {
		ulog.Logger.Errorf(err.Error())
		return nil, err
	}

	if err := a.setupDrivers(); err != nil {
		return nil, fmt.Errorf("failed to setup drivers: %v", err)
	}

	if err := a.setupServer(); err != nil {
		return nil, fmt.Errorf("failed to setup server: %v", err)
	}

	if err := a.setupSched(); err != nil {
		return nil, fmt.Errorf("failed to setup scheduler: %v", err)
	}

	go a.eventLoop()
	return a, nil
}

func (a *Agent) startupJoin(config *uconf.Config) error {
	if len(config.Client.Join) == 0 {
		return nil
	}

	ulog.Logger.Infof("agent: Joining cluster...")
	ulog.Logger.Infof("agent: Joining: %v replay: %v", config.Client.Join, true)
	n, err := a.serf.Join(config.Client.Join, false)
	if err != nil {
		return err
	}

	ulog.Logger.Infof(fmt.Sprintf("agent: Join completed. Synced with %d initial agents", n))
	return nil
}

// setupServer is used to setup the server if enabled
func (a *Agent) setupServer() error {
	if !a.config.Server.Enabled {
		return nil
	}
	var err error
	snsEpoch := uint32(time.Now().UnixNano())
	a.idWorker, err = uutil.NewIdWorker(0, 0, snsEpoch)
	if err != nil {
		a.Shutdown()
		return fmt.Errorf("failed to new id worker: %v", err)
	}

	a.store = SetupStore(a.config.Consul.Addrs, a)
	// Initialize the RPC layer
	if err := a.setupRPC(); err != nil {
		a.Shutdown()
		return fmt.Errorf("failed to start RPC layer: %v", err)
	}

	// Setup the HTTP server
	a.NewHTTPServer()
	a.participate()

	return nil
}

func (a *Agent) setupSched() error {
	rpcServer, err := a.queryRPCConfig()
	if err != nil {
		return err
	}

	rc := &RPCClient{ServerAddr: string(rpcServer), agent: a}

	jobs, err := rc.CallGetJobs(a.config.NodeName)
	if err != nil {
		return err
	}

	for _, job := range jobs.Payload {
		if job.Status == Running || job.Status == Queued {
			ulog.Logger.Infof("agent: Start job: %v", job.Name)
			for k, v := range job.Processors {
				if v.NodeName == a.config.NodeName {
					go rc.startJob(job, k)
				}
			}
		}
	}

	return nil
}

var workaroundRPCHTTPMux = 0

// setupRPC is used to setup the RPC listener
func (a *Agent) setupRPC() error {
	r := &RPCServer{
		agent: a,
	}

	ulog.Logger.WithFields(logrus.Fields{
		"rpc_addr": a.getRPCAddr(),
	}).Debug("rpc: Registering RPC server")

	rpc.Register(r)

	oldMux := http.DefaultServeMux
	if workaroundRPCHTTPMux > 0 {
		mux := http.NewServeMux()
		http.DefaultServeMux = mux
	}
	workaroundRPCHTTPMux = workaroundRPCHTTPMux + 1

	rpc.HandleHTTP()

	http.DefaultServeMux = oldMux

	l, err := net.Listen("tcp", a.getRPCAddr())
	if err != nil {
		ulog.Logger.Fatal(err)
		return err
	}
	go http.Serve(l, nil)
	return nil
}

func (a *Agent) setupDrivers() error {
	var avail []string
	ctx := plugins.NewDriverContext("", nil)
	for name := range plugins.BuiltinProcessors {
		_, err := plugins.DiscoverPlugins(name, ctx)
		if err != nil {
			return err
		}
		avail = append(avail, name)

	}

	ulog.Logger.Debugf("agent: Available drivers %v", avail)

	return nil
}

func (a *Agent) setupNatsServer() error {
	host, port, err := net.SplitHostPort(a.config.Nats.Addr)
	p, err := strconv.Atoi(port)
	if err != nil {
		return err
	}

	nOpts := gnatsd.Options{
		Host:       host,
		Port:       p,
		MaxPayload: (100 * 1024 * 1024),
		Trace:      true,
		Debug:      true,
	}
	ulog.Logger.Infof("agent: Starting nats-streaming-server [%s]", a.config.Nats.Addr)
	sOpts := stand.GetDefaultOptions()
	sOpts.ID = uconf.DefaultClusterID
	if a.config.Nats.StoreType == "file" {
		sOpts.StoreType = a.config.Nats.StoreType
		sOpts.FilestoreDir = a.config.Nats.FilestoreDir
	}
	s, err := stand.RunServerWithOpts(sOpts, &nOpts)
	if err != nil {
		return err
	}
	a.stand = s
	return nil
}

// setupSerf is used to create the agent we use
func (a *Agent) setupSerf() (*serf.Serf, error) {
	serfConfig := serf.DefaultConfig()
	serfConfig.MemberlistConfig = memberlist.DefaultWANConfig()

	serfAddr, err := net.ResolveTCPAddr("tcp", a.config.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Serf address %q: %v", a.config.BindAddr, err)
	}
	serfConfig.MemberlistConfig.BindAddr = serfAddr.IP.String()
	serfConfig.MemberlistConfig.BindPort = serfAddr.Port
	serfConfig.MemberlistConfig.AdvertiseAddr = serfAddr.IP.String()
	serfConfig.MemberlistConfig.AdvertisePort = serfAddr.Port

	serfConfig.NodeName = a.config.NodeName
	serfConfig.Init()
	if a.config.Server.Enabled {
		serfConfig.Tags["udup_server"] = "true"
	}
	serfConfig.Tags["udup_version"] = a.config.Version
	serfConfig.Tags["role"] = "udup"
	serfConfig.Tags["region"] = a.config.Region
	serfConfig.Tags["dc"] = a.config.Datacenter

	natsAddr, err := net.ResolveTCPAddr("tcp", a.config.Nats.Addr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Nats address %q: %v", a.config.Nats.Addr, err)
	}
	serfConfig.Tags["nats_ip"] = natsAddr.IP.String()
	serfConfig.Tags["nats_port"] = strconv.Itoa(natsAddr.Port)

	serfConfig.QuerySizeLimit = 1024 * 10
	serfConfig.CoalescePeriod = 3 * time.Second
	serfConfig.QuiescentPeriod = time.Second
	serfConfig.UserCoalescePeriod = 3 * time.Second
	serfConfig.UserQuiescentPeriod = time.Second
	if a.config.ReconnectInterval != 0 {
		serfConfig.ReconnectInterval = a.config.ReconnectInterval
	}
	if a.config.ReconnectTimeout != 0 {
		serfConfig.ReconnectTimeout = a.config.ReconnectTimeout
	}
	if a.config.TombstoneTimeout != 0 {
		serfConfig.TombstoneTimeout = a.config.TombstoneTimeout
	}
	serfConfig.EnableNameConflictResolution = !a.config.DisableNameResolution
	serfConfig.RejoinAfterLeave = a.config.RejoinAfterLeave

	// Create a channel to listen for events from Serf
	a.eventCh = make(chan serf.Event, 64)
	serfConfig.EventCh = a.eventCh

	// Start Serf
	ulog.Logger.Debugf("agent: Udup agent starting")

	serfConfig.LogOutput = ioutil.Discard
	serfConfig.MemberlistConfig.LogOutput = ioutil.Discard

	return serf.Create(serfConfig)
}

// Utility method to get leader nodename
func (a *Agent) leaderMember() (*serf.Member, error) {
	leaderName := a.store.GetLeader()
	for _, member := range a.serf.Members() {
		if member.Name == string(leaderName) {
			return &member, nil
		}
	}
	return nil, ErrLeaderNotFound
}

func (a *Agent) listServers() []serf.Member {
	members := []serf.Member{}

	for _, member := range a.serf.Members() {
		if key, ok := member.Tags["udup_server"]; ok {
			if key == "true" && member.Status == serf.StatusAlive {
				members = append(members, member)
			}
		}
	}
	return members
}

func (a *Agent) listAlivedMembers() []serf.Member {
	members := []serf.Member{}

	for _, member := range a.serf.Members() {
		if member.Status == serf.StatusAlive {
			members = append(members, member)
		}
	}
	return members
}

func (a *Agent) getNatsAddr(nodeName string) string {
	for _, member := range a.serf.Members() {
		if member.Name == nodeName {
			if ip, ok := member.Tags["nats_ip"]; ok {
				if port, ok := member.Tags["nats_port"]; ok {
					return fmt.Sprintf("%s:%s", ip, port)
				}
			}
			return ""
		}
	}
	return ""
}

// Listens to events from Serf and handle the event.
func (a *Agent) eventLoop() {
	serfShutdownCh := a.serf.ShutdownCh()
	ulog.Logger.Info("agent: Listen for events")
	for {
		select {
		case e := <-a.eventCh:
			ulog.Logger.WithFields(logrus.Fields{
				"event": e.String(),
			}).Debug("agent: Received event")

			// Log all member events
			go func() {
				if e, ok := e.(serf.MemberEvent); ok {
					for _, m := range e.Members {
						ulog.Logger.WithFields(logrus.Fields{
							"node":   a.config.NodeName,
							"member": m.Name,
							"event":  e.EventType(),
						}).Debug("agent: Member event")

						switch e.EventType() {
						case serf.EventMemberLeave, serf.EventMemberFailed:
							if err := a.enqueueJobs(m.Name); err != nil {
								ulog.Logger.Errorf("agent: Error enqueue job command,err:%v", err)
							}
						default:
							//ignore
						}
					}
				}
			}()

			if e.EventType() == serf.EventQuery {
				query := e.(*serf.Query)

				switch query.Name {
				case QueryStartJob:
					{
						ulog.Logger.WithFields(logrus.Fields{
							"query":   query.Name,
							"payload": string(query.Payload),
							"at":      query.LTime,
						}).Debug("agent: Starting job")
						var rqp RunQueryParam
						if err := json.Unmarshal(query.Payload, &rqp); err != nil {
							ulog.Logger.WithField("query", QueryStartJob).Fatal("agent: Error unmarshaling query payload")
						}

						ulog.Logger.WithFields(logrus.Fields{
							"job": rqp.Job.Name,
						}).Info("agent: Starting job")

						job := rqp.Job
						k := rqp.Type

						go func() {
							if err := a.startJob(job, k); err != nil {
								ulog.Logger.WithError(err).Errorf("agent: Error start job command,err:%v", err)
								ulog.Logger.WithField("query", QueryStartJob).Fatalf("agent: Error start job command,err:%v", err)
							}
						}()

						jobJson, _ := json.Marshal(job)
						query.Respond(jobJson)
					}
				case QueryStopJob:
					{
						ulog.Logger.WithFields(logrus.Fields{
							"query":   query.Name,
							"payload": string(query.Payload),
							"at":      query.LTime,
						}).Debug("agent: Stopping job")

						var rqp RunQueryParam
						if err := json.Unmarshal(query.Payload, &rqp); err != nil {
							ulog.Logger.WithField("query", QueryStopJob).Fatal("agent: Error unmarshaling query payload")
						}

						ulog.Logger.WithFields(logrus.Fields{
							"job": rqp.Job.Name,
						}).Info("agent: Stopping job")

						job := rqp.Job
						k := rqp.Type

						go func() {
							if err := a.stopJob(job, k); err != nil {
								ulog.Logger.WithError(err).Errorf("agent: Error stop job command,err:%v", err)
								ulog.Logger.WithField("query", QueryStopJob).Fatalf("agent: Error stop job command,err:%v", err)
							}
						}()

						jobJson, _ := json.Marshal(job)
						query.Respond(jobJson)
					}
				case QueryEnqueueJob:
					{
						ulog.Logger.WithFields(logrus.Fields{
							"query":   query.Name,
							"payload": string(query.Payload),
							"at":      query.LTime,
						}).Debug("agent: Enqueue job")

						var rqp RunQueryParam
						if err := json.Unmarshal(query.Payload, &rqp); err != nil {
							ulog.Logger.Errorf("agent: Error unmarshaling query payload,Query:%v", QueryEnqueueJob)
						}

						ulog.Logger.WithFields(logrus.Fields{
							"job": rqp.Job.Name,
						}).Info("agent: Enqueue job")

						job := rqp.Job
						k := rqp.Type

						go func() {
							if err := a.enqueueJob(job, k); err != nil {
								ulog.Logger.WithError(err).Errorf("agent: Error enqueue job command,err:%v", err)
								ulog.Logger.WithField("query", QueryEnqueueJob).Fatalf("agent: Error enqueue job command,err:%v", err)
							}
						}()

						jobJson, _ := json.Marshal(job)
						query.Respond(jobJson)
					}
				case QueryRPCConfig:
					{
						if a.config.Server.Enabled {
							ulog.Logger.WithFields(logrus.Fields{
								"query":   query.Name,
								"payload": string(query.Payload),
								"at":      query.LTime,
							}).Debug("agent: RPC Config requested")

							query.Respond([]byte(a.getRPCAddr()))
						}
					}
				case QueryGenId:
					{
						if a.config.Server.Enabled {
							ulog.Logger.WithFields(logrus.Fields{
								"query":   query.Name,
								"payload": string(query.Payload),
								"at":      query.LTime,
							}).Debug("agent: Generate id requested")

							serverID, err := a.idWorker.NextId()
							if err != nil {
								ulog.Logger.Fatal(err)
							}
							query.Respond([]byte(strconv.FormatUint(uint64(serverID), 10)))
						}
					}
				default:
					{
						return
					}
				}
			}

		case <-serfShutdownCh:
			ulog.Logger.Warn("agent: Serf shutdown detected, quitting")
			return
		}
	}
}

func (a *Agent) startJob(j *Job, k string) (err error) {
	var rpcServer []byte
	if !a.config.Server.Enabled {
		rpcServer, err = a.queryRPCConfig()
		if err != nil {
			return err
		}
	}

	rc := &RPCClient{ServerAddr: string(rpcServer), agent: a}
	return rc.startJob(j, k)
}

func (a *Agent) stopJob(j *Job, k string) (err error) {
	var rpcServer []byte
	if !a.config.Server.Enabled {
		rpcServer, err = a.queryRPCConfig()
		if err != nil {
			return err
		}
	}

	rc := &RPCClient{ServerAddr: string(rpcServer), agent: a}
	return rc.stopJob(j, k)
}

func (a *Agent) enqueueJobs(nodeName string) (err error) {
	var rpcServer []byte
	if !a.config.Server.Enabled {
		rpcServer, err = a.queryRPCConfig()
		if err != nil {
			return err
		}
	}
	rc := &RPCClient{ServerAddr: string(rpcServer), agent: a}
	return rc.enqueueJobs(nodeName)
}

func (a *Agent) enqueueJob(job *Job, k string) (err error) {
	var rpcServer []byte
	if !a.config.Server.Enabled {
		rpcServer, err = a.queryRPCConfig()
		if err != nil {
			return err
		}
	}

	rc := &RPCClient{ServerAddr: string(rpcServer), agent: a}
	return rc.enqueueJob(job, k)
}

func (a *Agent) participate() {
	a.candidate = leadership.NewCandidate(a.store.Client, a.store.LeaderKey(), a.config.NodeName, defaultLeaderTTL)

	go func() {
		for {
			a.runForElection()
			// retry
			time.Sleep(defaultRecoverTime)
		}
	}()
}

// Leader election routine
func (a *Agent) runForElection() {
	ulog.Logger.Info("agent: Running for election")
	electedCh, errCh := a.candidate.RunForElection()

	for {
		select {
		case isElected := <-electedCh:
			if isElected {
				ulog.Logger.Info("agent: Cluster leadership acquired")
				// If this server is elected as the leader, start the scheduler
				jobs, err := a.store.GetJobs()
				if err != nil {
					ulog.Logger.Errorf("agent: Error on rpc.GetJob call")
				}
				for _, job := range jobs {
					if job.Status == Running {
						for _, v := range job.Processors {
							m := &serf.Member{}
							for _, member := range a.serf.Members() {
								if v.NodeName == member.Name {
									m = &member
								}
							}
							if m == nil || m.Status != serf.StatusAlive {
								job.Status = Queued
								for _, p := range job.Processors {
									p.Running = false
								}
								if err := a.store.UpsertJob(job); err != nil {
									ulog.Logger.Errorf("agent: Error on rpc.UpsertJob call")
								}
							}
						}
					}
				}

			} else {
				ulog.Logger.Info("agent: Cluster leadership lost")
				// Always stop the schedule of this server to prevent multiple servers with the scheduler on
			}

		case err := <-errCh:
			ulog.Logger.WithError(err).Debugf("agent: Leader election failed, channel is probably closed,err:%v", err)
			// Always stop the schedule of this server to prevent multiple servers with the scheduler on
			return
		}
	}
}

// This function is called when a client request the RPCAddress
// of the current member.
func (a *Agent) getRPCAddr() string {
	bindIp := a.serf.LocalMember().Addr

	return fmt.Sprintf("%s:%d", bindIp, a.config.RPCPort)
}

// Shutdown is used to terminate the agent.
func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()

	if a.shutdown {
		return nil
	}

	ulog.Logger.Infof("agent: Requesting shutdown")
	for k, v := range a.processorPlugins {
		if err := v.Stop(k.tp); err != nil {
			return err
		}
	}
	a.stand.Shutdown()
	ulog.Logger.Infof("agent: Shutdown complete")
	a.shutdown = true
	close(a.shutdownCh)
	return nil
}
