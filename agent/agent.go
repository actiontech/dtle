package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/docker/leadership"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/serf/serf"
	"github.com/ngaut/log"

	uconf "udup/config"
	"udup/plugins"
)

var (
	// Error thrown on obtained leader from store is not found in member list
	ErrLeaderNotFound = errors.New("No member leader found in member list")
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
	sched     *Scheduler
	candidate *leadership.Candidate
	ready     bool

	ProcessorPlugins map[string]string
	shutdown         bool
	shutdownCh       chan struct{}
	shutdownLock     sync.Mutex
}

// NewAgent is used to create a new agent with the given configuration
func NewAgent(config *uconf.Config) (*Agent, error) {
	a := &Agent{
		config:     config,
		shutdownCh: make(chan struct{}),
	}

	// Initialize the wan Serf
	var err error
	a.serf, err = a.setupSerf()
	if err != nil {
		a.Shutdown()
		return nil, fmt.Errorf("Failed to start serf: %v", err)
	}
	a.join(a.config.StartJoin, true)

	if err := a.setupDrivers(); err != nil {
		return nil, fmt.Errorf("plugin setup failed: %v", err)
	}

	if a.config.Server {
		a.store = NewStore(a.config.Consul.Addrs, a)
		a.sched = NewScheduler()

		a.ServeHTTP()
		listenRPC(a)
		a.participate()
	}
	go a.eventLoop()
	a.ready = true
	return a, nil
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

	log.Debugf("[DEBUG] client: available plugins %v", avail)

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
	serfConfig.SnapshotPath = filepath.Join("./", serfSnapshot)
	if err := ensurePath(serfConfig.SnapshotPath, false); err != nil {
		return nil, err
	}
	serfConfig.QuerySizeLimit = 1024 * 10
	serfConfig.CoalescePeriod = 6 * time.Second
	serfConfig.QuiescentPeriod = time.Second
	serfConfig.UserCoalescePeriod = 6 * time.Second
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
	log.Info("agent: Udup agent starting")

	serfConfig.LogOutput = ioutil.Discard
	serfConfig.MemberlistConfig.LogOutput = ioutil.Discard

	return serf.Create(serfConfig)
}

func (a *Agent) getBindAddr() (string, int, error) {
	bindIP, bindPort, err := a.config.AddrParts(a.config.BindAddr)
	if err != nil {
		log.Infof(fmt.Sprintf("Invalid bind address: %s", err))
		return "", 0, err
	}

	// Check if we have an interface
	if iface, _ := a.config.NetworkInterface(); iface != nil {
		addrs, err := iface.Addrs()
		if err != nil {
			log.Infof(fmt.Sprintf("Failed to get interface addresses: %s", err))
			return "", 0, err
		}
		if len(addrs) == 0 {
			log.Infof(fmt.Sprintf("Interface '%s' has no addresses", a.config.Interface))
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
				log.Infof(fmt.Sprintf("Failed to find usable address for interface '%s'", a.config.Interface))
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
				log.Infof(fmt.Sprintf("Interface '%s' has no '%s' address",
					a.config.Interface, bindIP))
				return "", 0, err
			}
		}
	}
	return bindIP, bindPort, nil
}

// ensurePath is used to make sure a path exists
func ensurePath(path string, dir bool) error {
	if !dir {
		path = filepath.Dir(path)
	}
	return os.MkdirAll(path, 0755)
}

// Start or restart scheduler
func (a *Agent) schedule() {
	log.Debug("agent: Restarting scheduler")
	jobs, err := a.store.GetJobs()
	if err != nil {
		log.Fatal(err)
	}
	a.sched.Restart(jobs)
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

				if query.Name == QuerySchedulerRestart && a.config.Server {
					a.schedule()
				}

				if query.Name == QueryRunJob {
					log.Debug("query:%v,payload:%v,at:%v,agent: Running job", query.Name, string(query.Payload), query.LTime)

					var rqp RunQueryParam
					if err := json.Unmarshal(query.Payload, &rqp); err != nil {
						log.Infof("query:%v,agent: Error unmarshaling query payload", QueryRunJob)
					}

					log.Infof("job:%v,Starting job", rqp.Job.Name)

					/*rpcc := RPCClient{ServerAddr: rqp.RPCAddr}
					job, err := rpcc.GetJob(rqp.Job.Name)
					if err != nil {
						log.Infof("err:%v,agent: Error on rpc.GetJob call", err)
					}*/

					job := rqp.Job
					job.StartedAt = time.Now()
					job.NodeName = a.config.NodeName

					go func() {
						if err := a.invokeJob(job); err != nil {
							log.Infof("err:%v,agent: Error invoking job command", err)
						}
					}()

					jobJson, _ := json.Marshal(job)
					query.Respond(jobJson)
				}

				if query.Name == QueryRPCConfig && a.config.Server {
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

// invokeJob will execute the given job. Depending on the event.
func (a *Agent) invokeJob(job *Job) error {
	job.FinishedAt = time.Now()
	job.Success = true

	rpcServer, err := a.queryRPCConfig()
	if err != nil {
		return err
	}

	rc := &RPCClient{ServerAddr: string(rpcServer)}
	return rc.callRunJob(job)
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
				a.schedule()
			} else {
				log.Info("agent: Cluster leadership lost")
				// Always stop the schedule of this server to prevent multiple servers with the scheduler on
				a.sched.Stop()
			}

		case err := <-errCh:
			log.Infof("err:%v,Leader election failed, channel is probably closed", err)
			// Always stop the schedule of this server to prevent multiple servers with the scheduler on
			a.sched.Stop()
			return
		}
	}
}

func (a *Agent) processFilteredNodes(job *Job) ([]string, map[string]string, error) {
	var nodes []string
	tags := make(map[string]string)

	// Actually copy the map
	for key, val := range job.Tags {
		tags[key] = val
	}

	for jtk, jtv := range tags {
		var tc []string
		if tc = strings.Split(jtv, ":"); len(tc) == 2 {
			tv := tc[0]

			// Set original tag to clean tag
			tags[jtk] = tv

			count, err := strconv.Atoi(tc[1])
			if err != nil {
				return nil, nil, err
			}

			for _, member := range a.serf.Members() {
				if member.Status == serf.StatusAlive {
					for mtk, mtv := range member.Tags {
						if mtk == jtk && mtv == tv {
							if len(nodes) < count {
								nodes = append(nodes, member.Name)
							}
						}
					}
				}
			}
		}
	}

	return nodes, tags, nil
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

	log.Infof("[INFO] agent: requesting shutdown")

	log.Infof("[INFO] agent: shutdown complete")
	a.shutdown = true
	close(a.shutdownCh)
	return nil
}
