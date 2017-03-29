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

	"github.com/docker/leadership"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/serf/serf"
	gnatsd "github.com/nats-io/gnatsd/server"
	stand "github.com/nats-io/nats-streaming-server/server"
	"github.com/ngaut/log"

	uconf "udup/config"
	"udup/plugins"
	uutil "udup/plugins/mysql/util"
)

var (
	// Error thrown on obtained leader from store is not found in member list
	ErrLeaderNotFound = errors.New("No member leader found in member list")
)

const (
	defaultRecoverTime = 10 * time.Second
	defaultLeaderTTL   = 20 * time.Second
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
		log.Errorf(err.Error())
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
	if len(config.StartJoin) == 0 {
		return nil
	}

	log.Infof("Joining cluster...")
	log.Infof("Joining: %v replay: %v", config.StartJoin, true)
	n, err := a.serf.Join(config.StartJoin, false)
	if err != nil {
		return err
	}

	log.Infof(fmt.Sprintf("Join completed. Synced with %d initial agents", n))
	return nil
}

// setupServer is used to setup the server if enabled
func (a *Agent) setupServer() error {
	if !a.config.Server {
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
		if job.Status == Running {
			log.Infof("Start job: %v", job.Name)
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

	log.Infof("Registering RPC server: %v", a.getRPCAddr())

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
		log.Fatal(err)
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

	log.Debugf("Available drivers %v", avail)

	return nil
}

func (a *Agent) setupNatsServer() error {
	host, port, err := net.SplitHostPort(a.config.NatsAddr)
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
	log.Infof("Starting nats-streaming-server [%s]", a.config.NatsAddr)
	sOpts := stand.GetDefaultOptions()
	sOpts.ID = uconf.DefaultClusterID
	if a.config.StoreType == "FILE" {
		sOpts.StoreType = a.config.StoreType
		sOpts.FilestoreDir = a.config.FilestoreDir
	}
	s := stand.RunServerWithOpts(sOpts, &nOpts)
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

	serfConfig.NodeName = a.config.NodeName
	serfConfig.Init()
	if a.config.Server {
		serfConfig.Tags["udup_server"] = "true"
	}
	serfConfig.Tags["udup_version"] = a.config.Version
	serfConfig.Tags["role"] = "udup"
	serfConfig.Tags["region"] = a.config.Region
	serfConfig.Tags["dc"] = a.config.Datacenter

	natsAddr, err := net.ResolveTCPAddr("tcp", a.config.NatsAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Nats address %q: %v", a.config.NatsAddr, err)
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
	log.Info("Udup agent starting")

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
	log.Info("Listen for events")
	for {
		select {
		case e := <-a.eventCh:
			log.Infof("Received event: %v", e.String())

			// Log all member events
			go func() {
				if e, ok := e.(serf.MemberEvent); ok {
					for _, m := range e.Members {
						log.Infof("Member event: %v; Node:%v; Member:%v.", e.EventType(), a.config.NodeName, m.Name)
						switch e.EventType() {
						case serf.EventMemberJoin, serf.EventMemberUpdate, serf.EventMemberReap:
							if err := a.dequeueJobs(m.Name); err != nil {
								log.Errorf("Error dequeue job command,err:%v", err)
							}

						case serf.EventMemberLeave, serf.EventMemberFailed:
							if err := a.enqueueJobs(m.Name); err != nil {
								log.Errorf("Error enqueue job command,err:%v", err)
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
						log.Infof("Running job: Query:%v; Payload:%v; LTime:%v,", query.Name, string(query.Payload), query.LTime)

						var rqp RunQueryParam
						if err := json.Unmarshal(query.Payload, &rqp); err != nil {
							log.Errorf("Error unmarshaling query payload,Query:%v", QueryStartJob)
						}

						job := rqp.Job
						k := rqp.Type

						go func() {
							if err := a.startJob(job, k); err != nil {
								log.Errorf("Error start job command,err:%v", err)
							}
						}()

						jobJson, _ := json.Marshal(job)
						query.Respond(jobJson)
					}
				case QueryStopJob:
					{
						log.Debugf("Stop job: Query:%v; Payload:%v; LTime:%v,", query.Name, string(query.Payload), query.LTime)

						var rqp RunQueryParam
						if err := json.Unmarshal(query.Payload, &rqp); err != nil {
							log.Errorf("Error unmarshaling query payload,Query:%v", QueryStopJob)
						}

						job := rqp.Job

						go func() {
							if err := a.stopJob(job); err != nil {
								log.Errorf("Error stop job command,err:%v", err)
							}
						}()

						jobJson, _ := json.Marshal(job)
						query.Respond(jobJson)
					}
				case QueryRPCConfig:
					{
						if a.config.Server {
							log.Infof("RPC Config requested,Query:%v; Payload:%v; LTime:%v", query.Name, string(query.Payload), query.LTime)

							query.Respond([]byte(a.getRPCAddr()))
						}
					}
				case QueryGenId:
					{
						if a.config.Server {
							log.Infof("RPC Config requested,Query:%v; Payload:%v; LTime:%v", query.Name, string(query.Payload), query.LTime)
							serverID, err := a.idWorker.NextId()
							if err != nil {
								log.Fatal(err)
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
			log.Warn("Serf shutdown detected, quitting")
			return
		}
	}
}

func (a *Agent) startJob(j *Job, k string) (err error) {
	var rpcServer []byte
	if !a.config.Server {
		rpcServer, err = a.queryRPCConfig()
		if err != nil {
			return err
		}
	}

	rc := &RPCClient{ServerAddr: string(rpcServer), agent: a}
	return rc.startJob(j, k)
}

func (a *Agent) stopJob(j *Job) (err error) {
	var rpcServer []byte
	if !a.config.Server {
		rpcServer, err = a.queryRPCConfig()
		if err != nil {
			return err
		}
	}

	rc := &RPCClient{ServerAddr: string(rpcServer), agent: a}
	return rc.stopJob(j)
}

func (a *Agent) enqueueJobs(nodeName string) (err error) {
	var rpcServer []byte
	if !a.config.Server {
		rpcServer, err = a.queryRPCConfig()
		if err != nil {
			return err
		}
	}
	rc := &RPCClient{ServerAddr: string(rpcServer), agent: a}
	return rc.enqueueJobs(nodeName)
}

func (a *Agent) enqueueJob(j string) (err error) {
	var rpcServer []byte
	if !a.config.Server {
		rpcServer, err = a.queryRPCConfig()
		if err != nil {
			return err
		}
	}

	rc := &RPCClient{ServerAddr: string(rpcServer), agent: a}
	job, err := rc.CallGetJob(j)
	if err != nil {
		return fmt.Errorf("agent: Error on rpc.GetJob call")
	}
	return rc.enqueueJob(job)
}

func (a *Agent) dequeueJobs(nodeName string) (err error) {
	var rpcServer []byte
	if !a.config.Server {
		rpcServer, err = a.queryRPCConfig()
		if err != nil {
			return err
		}
	}

	rc := &RPCClient{ServerAddr: string(rpcServer), agent: a}
	return rc.dequeueJobs(nodeName)
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
	log.Info("Running for election")
	electedCh, errCh := a.candidate.RunForElection()

	for {
		select {
		case isElected := <-electedCh:
			if isElected {
				log.Info("Cluster leadership acquired")
				// If this server is elected as the leader, start the scheduler

			} else {
				log.Info("Cluster leadership lost")
				// Always stop the schedule of this server to prevent multiple servers with the scheduler on
			}

		case err := <-errCh:
			log.Errorf("agent: Leader election failed, channel is probably closed,err:%v", err)
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

	log.Infof("Requesting shutdown")
	for k, v := range a.processorPlugins {
		if err := v.Stop(k.tp); err != nil {
			return err
		}
	}
	a.stand.Shutdown()
	log.Infof("Shutdown complete")
	a.shutdown = true
	close(a.shutdownCh)
	return nil
}
