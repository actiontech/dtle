package agent

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/docker/leadership"
	"github.com/hashicorp/serf/serf"
	"github.com/ngaut/log"

	uconf "udup/config"
	"udup/plugins"
)

const (
	serfSnapshot       = "serf/snapshot"
	defaultRecoverTime = 10 * time.Second
	defaultLeaderTTL   = 20 * time.Second
)

type Agent struct {
	config    *uconf.Config
	serf      *serf.Serf
	store     *Store
	eventCh   chan serf.Event
	candidate *leadership.Candidate

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

// NewAgent is used to create a new agent with the given configuration
func NewAgent(config *uconf.Config) (*Agent, error) {
	a := &Agent{
		config:     config,
		shutdownCh: make(chan struct{}),
	}

	go a.listenOnPanicAbort()

	if a.serf = a.setupSerf(); a.serf == nil {
		return nil, fmt.Errorf("Can not setup serf")
	}

	a.join(a.config.StartJoin, true)

	if a.config.Server {
		a.store = NewStore(a.config.Consul.Addrs, a)
		a.ServeHTTP()
		listenRPC(a)
		//a.participate()
	}

	if err := a.setupPlugins(); err != nil {
		return nil, fmt.Errorf("plugin setup failed: %v", err)
	}

	go a.eventLoop()

	return a, nil
}

// setupSerf is used to create the agent we use
func (a *Agent) setupSerf() *serf.Serf {
	config := a.config

	bindIP, bindPort, err := config.AddrParts(config.BindAddr)
	if err != nil {
		log.Infof(fmt.Sprintf("Invalid bind address: %s", err))
		return nil
	}

	// Check if we have an interface
	if iface, _ := config.NetworkInterface(); iface != nil {
		addrs, err := iface.Addrs()
		if err != nil {
			log.Infof(fmt.Sprintf("Failed to get interface addresses: %s", err))
			return nil
		}
		if len(addrs) == 0 {
			log.Infof(fmt.Sprintf("Interface '%s' has no addresses", config.Interface))
			return nil
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
					config.Interface, bindIP))

				// Update the configuration
				bindAddr := &net.TCPAddr{
					IP:   net.ParseIP(bindIP),
					Port: bindPort,
				}
				config.BindAddr = bindAddr.String()
				break
			}
			if !found {
				log.Infof(fmt.Sprintf("Failed to find usable address for interface '%s'", config.Interface))
				return nil
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
				log.Infof(fmt.Sprintf("Interface '%s' has no '%s' address",
					config.Interface, bindIP))
				return nil
			}
		}
	}

	serfConfig := serf.DefaultConfig()
	serfConfig.MemberlistConfig.BindAddr = bindIP
	serfConfig.MemberlistConfig.BindPort = bindPort
	serfConfig.NodeName = config.NodeName
	serfConfig.Init()
	if config.Server {
		serfConfig.Tags["udup_server"] = "true"
	}
	serfConfig.Tags["udup_version"] = config.Version
	serfConfig.Tags["role"] = "udup"
	serfConfig.Tags["region"] = config.Region
	serfConfig.Tags["dc"] = config.Datacenter
	serfConfig.SnapshotPath = serfSnapshot
	serfConfig.SnapshotPath = filepath.Join("./", serfSnapshot)
	if err := ensurePath(serfConfig.SnapshotPath, false); err != nil {
		return nil
	}
	serfConfig.CoalescePeriod = 3 * time.Second
	serfConfig.QuiescentPeriod = time.Second
	serfConfig.UserCoalescePeriod = 3 * time.Second
	serfConfig.UserQuiescentPeriod = time.Second
	if config.ReconnectInterval != 0 {
		serfConfig.ReconnectInterval = config.ReconnectInterval
	}
	if config.ReconnectTimeout != 0 {
		serfConfig.ReconnectTimeout = config.ReconnectTimeout
	}
	if config.TombstoneTimeout != 0 {
		serfConfig.TombstoneTimeout = config.TombstoneTimeout
	}
	serfConfig.EnableNameConflictResolution = !config.DisableNameResolution
	serfConfig.RejoinAfterLeave = config.RejoinAfterLeave

	// Create a channel to listen for events from Serf
	a.eventCh = make(chan serf.Event, 64)
	serfConfig.EventCh = a.eventCh

	// Start Serf
	log.Info("agent: Udup agent starting")

	serfConfig.LogOutput = ioutil.Discard
	serfConfig.MemberlistConfig.LogOutput = ioutil.Discard

	// Create serf first
	serf, err := serf.Create(serfConfig)
	if err != nil {
		log.Error(err)
		return nil
	}

	return serf
}

func (a *Agent) setupPlugins() error {
	var avail []string
	ctx := plugins.NewPluginContext("", nil)
	for name := range plugins.BuiltinProcessors {
		_, err := plugins.DiscoverPlugins(name, ctx)
		if err != nil {
			return err
		}
		avail = append(avail, name)

	}

	log.Debugf("[DEBUG] client: available plugins %v", avail)

	return nil
}

// ensurePath is used to make sure a path exists
func ensurePath(path string, dir bool) error {
	if !dir {
		path = filepath.Dir(path)
	}
	return os.MkdirAll(path, 0755)
}

// Join asks the Serf instance to join. See the Serf.Join function.
func (a *Agent) join(addrs []string, replay bool) (n int, err error) {
	log.Infof("agent: joining: %v replay: %v", addrs, replay)
	ignoreOld := !replay
	n, err = a.serf.Join(addrs, ignoreOld)
	if n > 0 {
		log.Infof("agent: joined: %d nodes", n)
	}
	if err != nil {
		log.Warnf("agent: error joining: %v", err)
	}
	return
}

// Utility method to get leader nodename
func (a *Agent) leaderMember() (*serf.Member, error) {
	leaderName := a.store.GetLeader()
	for _, member := range a.serf.Members() {
		if member.Name == string(leaderName) {
			return &member, nil
		}
	}
	return nil, fmt.Errorf("No member leader found in member list")
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
	log.Info("agent: Listen for events")
	for {
		select {
		case e := <-a.eventCh:
			log.Infof("event:%v,agent: Received event", e.String())

			// Log all member events
			if failed, ok := e.(serf.MemberEvent); ok {
				for _, member := range failed.Members {
					log.Infof("node:%v,member:%v,event:%v,agent: Member event", a.config.NodeName, member.Name, e.EventType())
				}
			}

			if e.EventType() == serf.EventQuery {
				query := e.(*serf.Query)
				if query.Name == RestartJob && a.config.Server {
					jobs, err := a.store.GetJobs()
					if err != nil {
						log.Fatal(err)
					}
					for _, job := range jobs {
						go job.Run()
					}
				}
				if query.Name == RunJob {
					var rqp RunQueryParam
					if err := json.Unmarshal(query.Payload, &rqp); err != nil {
						log.Infof("agent: Error unmarshaling query payload:%v", err)
					}

					log.Infof("job:%v,Starting job", rqp.Job.Name)

					rpcc := RPCClient{ServerAddr: rqp.RPCAddr}
					job, err := rpcc.GetJob(rqp.Job.Name)
					if err != nil {
						log.Infof("err:%v,agent: Error on rpc.GetJob call", err)
					}

					go func() {
						if err := a.invokeJob(job); err != nil {
							log.Infof("err:%v,agent: Error invoking job command", err)
						}
					}()
					jobJson, _ := json.Marshal(job)
					query.Respond(jobJson)
				}
				if query.Name == RPCConfig && a.config.Server {
					log.Infof("query:%v,payload:%v,at:%v,agent: RPC Config requested", query.Name, string(query.Payload), query.LTime)

					query.Respond([]byte(a.getRPCAddr()))
				}
			}

		case <-serfShutdownCh:
			log.Warn("agent: Serf shutdown detected, quitting")
			return
		}
	}
}

func (a *Agent) invokeJob(job *Job) error {
	log.Infof("------invoke job")
	rpcServer, err := a.queryRPCConfig()
	if err != nil {
		return err
	}

	rc := &RPCClient{ServerAddr: string(rpcServer)}
	return rc.callExecutionDone(job)
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
	log.Info("agent: Running for election")
	electedCh, errCh := a.candidate.RunForElection()

	for {
		select {
		case isElected := <-electedCh:
			if isElected {
				log.Info("agent: Cluster leadership acquired")
				// If this server is elected as the leader, start the scheduler
			} else {
				log.Info("agent: Cluster leadership lost")
				// Always stop the schedule of this server to prevent multiple servers with the scheduler on
			}

		case err := <-errCh:
			log.Infof("err:%v,Leader election failed, channel is probably closed", err)
			// Always stop the schedule of this server to prevent multiple servers with the scheduler on
			return
		}
	}
}

func (a *Agent) JobRegister(payload []byte) *Job {
	var job Job
	if err := json.Unmarshal(payload, &job); err != nil {
		log.Fatal(err)
	}

	// Save the new execution to store
	if err := a.store.UpsertJob(&job); err != nil {
		log.Fatal(err)
	}

	return &job
}

// This function is called when a client request the RPCAddress
// of the current member.
func (a *Agent) getRPCAddr() string {
	bindIp := a.serf.LocalMember().Addr
	//bindIp := "192.168.99.1"

	return fmt.Sprintf("%s:%d", bindIp, a.config.RPCPort)
}

// listenOnPanicAbort aborts on abort request
func (a *Agent) listenOnPanicAbort() {
	err := <-a.config.PanicAbort
	log.Errorf("agent run failed: %v", err)
	a.Shutdown()
}

// Shutdown is used to terminate the agent.
func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()

	if a.shutdown {
		return nil
	}

	log.Infof("[INFO] agent: shutdown complete")
	a.shutdown = true
	close(a.shutdownCh)
	return nil
}
