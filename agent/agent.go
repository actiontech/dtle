package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/rpc"
	"net/http"
	"runtime"
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
	gnatsd    *gnatsd.Server
	stand     *stand.StanServer

	processorPlugins map[jobDriver]plugins.Driver
	Plugins          map[string]string
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

	if err := a.setupNatsServer(); err != nil {
		return nil, err
	}

	// Initialize the wan Serf
	var err error
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
	a.store = SetupStore(a.config.Consul.Addrs, a)
	// Initialize the RPC layer
	if err := a.setupRPC(); err != nil {
		a.Shutdown()
		log.Errorf("failed to start RPC layer: %s", err)
		return fmt.Errorf("Failed to start RPC layer: %v", err)
	}
	a.participate()

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
	gnats := gnatsd.New(&nOpts)
	go gnats.Start()
	// Wait for accept loop(s) to be started
	if !gnats.ReadyForConnections(10 * time.Second) {
		log.Infof("Unable to start NATS Server in Go Routine")
	}
	a.gnatsd = gnats
	sOpts := stand.GetDefaultOptions()
	sOpts.ID = uconf.DefaultClusterID
	if a.config.StoreType == "FILE" {
		sOpts.StoreType = a.config.StoreType
		sOpts.FilestoreDir = a.config.FilestoreDir
	}
	sOpts.NATSServerURL = fmt.Sprintf("nats://%s", a.config.NatsAddr)
	s := stand.RunServerWithOpts(sOpts, nil)
	a.stand = s
	return nil
}

// setupSerf is used to create the agent we use
func (a *Agent) setupSerf() (*serf.Serf, error) {
	serfConfig := serf.DefaultConfig()
	serfConfig.MemberlistConfig = memberlist.DefaultWANConfig()

	bindIP, bindPort, err := a.getBindAddr()
	if err != nil {
		return nil, err
	}
	serfConfig.MemberlistConfig.BindAddr = bindIP
	serfConfig.MemberlistConfig.BindPort = bindPort
	serfConfig.NodeName = a.config.NodeName
	serfConfig.Init()
	if a.config.Server {
		serfConfig.Tags["udup_server"] = "true"
	}
	serfConfig.Tags["udup_version"] = a.config.Version
	serfConfig.Tags["role"] = "udup"
	serfConfig.Tags["region"] = a.config.Region
	serfConfig.Tags["dc"] = a.config.Datacenter

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

func (a *Agent) getBindAddr() (string, int, error) {
	bindIP, bindPort, err := a.config.AddrParts(a.config.BindAddr)
	if err != nil {
		log.Errorf(fmt.Sprintf("Invalid bind address: [%s]", err))
		return "", 0, err
	}

	// Check if we have an interface
	if iface, _ := a.config.NetworkInterface(); iface != nil {
		addrs, err := iface.Addrs()
		if err != nil {
			log.Errorf(fmt.Sprintf("Failed to get interface addresses: [%s]", err))
			return "", 0, err
		}
		if len(addrs) == 0 {
			log.Errorf(fmt.Sprintf("Interface '%s' has no addresses", a.config.Interface))
			return "", 0, err
		}

		// If there is no bind IP, pick an address
		if bindIP == "0.0.0.0" {
			found := false
			for _, ad := range addrs {
				var addrIP net.IP
				if runtime.GOOS == "windows" {
					// Waiting for https://github.com/golang/go/issues/5395 to use IPNet only
					addr, ok := ad.(*net.IPAddr)
					if !ok {
						continue
					}
					addrIP = addr.IP
				} else {
					addr, ok := ad.(*net.IPNet)
					if !ok {
						continue
					}
					addrIP = addr.IP
				}

				// Skip self-assigned IPs
				if addrIP.IsLinkLocalUnicast() {
					continue
				}

				// Found an IP
				found = true
				bindIP = addrIP.String()
				log.Infof(fmt.Sprintf("Using interface '%s' address '%s'",
					a.config.Interface, bindIP))

				// Update the configuration
				bindAddr := &net.TCPAddr{
					IP:   net.ParseIP(bindIP),
					Port: bindPort,
				}
				a.config.BindAddr = bindAddr.String()
				break
			}
			if !found {
				log.Errorf(fmt.Sprintf("Failed to find usable address for interface '%s'", a.config.Interface))
				return "", 0, err
			}

		} else {
			// If there is a bind IP, ensure it is available
			found := false
			for _, ad := range addrs {
				addr, ok := ad.(*net.IPNet)
				if !ok {
					continue
				}
				if addr.IP.String() == bindIP {
					found = true
					break
				}
			}
			if !found {
				log.Errorf(fmt.Sprintf("Interface '%s' has no '%s' address",
					a.config.Interface, bindIP))
				return "", 0, err
			}
		}
	}
	return bindIP, bindPort, nil
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

// Listens to events from Serf and handle the event.
func (a *Agent) eventLoop() {
	serfShutdownCh := a.serf.ShutdownCh()
	log.Info("Listen for events")
	for {
		select {
		case e := <-a.eventCh:
			log.Infof("Received event: %v", e.String())

			// Log all member events
			go func(){
				if e, ok := e.(serf.MemberEvent); ok {
					for _, m := range e.Members {
						log.Infof("Member event: %v; Node:%v; Member:%v.", e.EventType(), a.config.NodeName, m.Name)
						switch e.EventType() {
						case serf.EventMemberJoin :
							if err := a.dequeueJobs(m.Name); err != nil {
								log.Errorf("Error dequeue job command,err:%v", err)
							}

						/*case serf.EventMemberLeave:

						case serf.EventMemberFailed:

						case serf.EventMemberUpdate:

						case serf.EventMemberReap:*/
						default:
							if err := a.enqueueJobs(m.Name); err != nil {
								log.Errorf("Error enqueue job command,err:%v", err)
							}
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

						log.Infof("Starting job: %v", rqp.Job.Name)

						job := rqp.Job
						job.NodeName = a.config.NodeName

						go func() {
							if err := a.startJob(job); err != nil {
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

						log.Infof("Stopping job: %v", rqp.Job.Name)

						job := rqp.Job
						job.NodeName = a.config.NodeName

						go func() {
							if err := a.stopJob(job); err != nil {
								log.Errorf("Error stop job command,err:%v", err)
							}
						}()

						jobJson, _ := json.Marshal(job)
						query.Respond(jobJson)
					}
				case QueryEnqueueJob:
					{
						log.Debugf("Enqueue job: Query:%v; Payload:%v; LTime:%v,", query.Name, string(query.Payload), query.LTime)

						var rqp RunQueryParam
						if err := json.Unmarshal(query.Payload, &rqp); err != nil {
							log.Errorf("Error unmarshaling query payload,Query:%v", QueryEnqueueJob)
						}

						log.Infof("Stopping job: %v", rqp.Job.Name)

						job := rqp.Job
						job.NodeName = a.config.NodeName

						go func() {
							if err := a.enqueueJob(job); err != nil {
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

func (a *Agent) startJob(job *Job) (err error) {
	var rpcServer []byte
	if !a.config.Server{
		rpcServer, err = a.queryRPCConfig()
		if err != nil {
			return err
		}
	}

	rc := &RPCClient{ServerAddr: string(rpcServer),agent:a}
	return rc.startJob(job)
}

func (a *Agent) stopJob(job *Job) (err error) {
	var rpcServer []byte
	if !a.config.Server{
		rpcServer, err = a.queryRPCConfig()
		if err != nil {
			return err
		}
	}

	rc := &RPCClient{ServerAddr: string(rpcServer),agent:a}
	return rc.stopJob(job)
}

func (a *Agent) enqueueJobs(nodeName string) (err error) {
	var rpcServer []byte
	if !a.config.Server{
		rpcServer, err = a.queryRPCConfig()
		if err != nil {
			return err
		}
	}
	rc := &RPCClient{ServerAddr: string(rpcServer),agent:a}
	return rc.enqueueJobs(nodeName)
}

func (a *Agent) enqueueJob(job *Job) (err error) {
	var rpcServer []byte
	if !a.config.Server{
		rpcServer, err = a.queryRPCConfig()
		if err != nil {
			return err
		}
	}

	rc := &RPCClient{ServerAddr: string(rpcServer),agent:a}
	return rc.enqueueJob(job)
}


func (a *Agent) dequeueJobs(nodeName string) (err error) {
	var rpcServer []byte
	if !a.config.Server{
		rpcServer, err = a.queryRPCConfig()
		if err != nil {
			return err
		}
	}

	rc := &RPCClient{ServerAddr: string(rpcServer),agent:a}
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
				log.Info("Restarting scheduler")
				jobs, err := a.store.GetJobs()
				if err != nil {
					log.Fatal(err)
				}
				for _, job := range jobs {
					if job.Status == Running {
						log.Infof("Start job: %v", job.Name)
						go job.Start(true)
					}
				}
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
	a.gnatsd.Shutdown()
	log.Infof("Shutdown complete")
	a.shutdown = true
	close(a.shutdownCh)
	return nil
}
