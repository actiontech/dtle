/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package models

import (
	"net"
	"time"

	"github.com/actiontech/dtle/internal"

	"github.com/hashicorp/serf/coordinate"
)

const (
	// HealthAny is special, and is used as a wild card,
	// not as a specific state.
	HealthPassing  = "passing"
	HealthWarning  = "warning"
	HealthCritical = "critical"
)

const (
	RegisterRequestType MessageType = iota
	DeregisterRequestType
)

// RaftIndex is used to track the index used while creating
// or modifying a given struct type.
type RaftIndex struct {
	CreateIndex uint64
	ModifyIndex uint64
}

// NodeListRequest is used to parameterize a list request
type NodeListRequest struct {
	QueryOptions
}

// NodeRegisterRequest is used for Node.Register endpoint
// to register a node as being a schedulable entity.
type NodeRegisterRequest struct {
	Node *Node
	WriteRequest
}

// NodeDeregisterRequest is used for Node.Deregister endpoint
// to deregister a node as being a schedulable entity.
type NodeDeregisterRequest struct {
	NodeID string
	WriteRequest
}

// NodeServerInfo is used to in NodeUpdateResponse to return Udup server
// information used in RPC server lists.
type NodeServerInfo struct {
	// RPCAdvertiseAddr is the IP endpoint that a Udup Server wishes to
	// be contacted at for RPCs.
	RPCAdvertiseAddr string

	// RpcMajorVersion is the major version number the Udup Server
	// supports
	RPCMajorVersion int32

	// RpcMinorVersion is the minor version number the Udup Server
	// supports
	RPCMinorVersion int32

	// Datacenter is the datacenter that a Udup server belongs to
	Datacenter string
}

// NodeUpdateStatusRequest is used for Node.UpdateStatus endpoint
// to update the status of a node.
type NodeUpdateStatusRequest struct {
	NodeID string
	Status string
	WriteRequest
}

// NodeEvaluateRequest is used to re-evaluate the ndoe
type NodeEvaluateRequest struct {
	NodeID string
	WriteRequest
}

// NodeSpecificRequest is used when we just need to specify a target node
type NodeSpecificRequest struct {
	Datacenter string
	NodeID     string
	QueryOptions
}

// NodeUpdateResponse is used to respond to a node update
type NodeUpdateResponse struct {
	HeartbeatTTL    time.Duration
	EvalIDs         []string
	EvalCreateIndex uint64
	NodeModifyIndex uint64

	// LeaderRPCAddr is the RPC address of the current Raft Leader.  If
	// empty, the current Udup Server is in the minority of a partition.
	LeaderRPCAddr string

	// NumNodes is the number of Udup nodes attached to this quorum of
	// Udup Servers at the time of the response.  This value can
	// fluctuate based on the health of the cluster between heartbeats.
	NumNodes int32

	// Servers is the full list of known Udup servers in the local
	// region.
	Servers []*NodeServerInfo

	QueryMeta
}

// NodeAllocsResponse is used to return allocs for a single node
type NodeAllocsResponse struct {
	Allocs []*Allocation
	QueryMeta
}

// NodeClientAllocsResponse is used to return allocs meta data for a single node
type NodeClientAllocsResponse struct {
	Allocs map[string]uint64
	QueryMeta
}

// SingleNodeResponse is used to return a single node
type SingleNodeResponse struct {
	Node *Node
	QueryMeta
}

// JobListResponse is used for a list request
type NodeListResponse struct {
	Nodes []*NodeListStub
	QueryMeta
}

const (
	NodeStatusInit  = "initializing"
	NodeStatusReady = "ready"
	NodeStatusDown  = "down"
)

// ValidNodeStatus is used to check if a node status is valid
func ValidNodeStatus(status string) bool {
	switch status {
	case NodeStatusInit, NodeStatusReady, NodeStatusDown:
		return true
	default:
		return false
	}
}

// Node is a representation of a schedulable client node
type Node struct {
	// ID is a unique identifier for the node. It can be constructed
	// by doing a concatenation of the Name and Datacenter as a simple
	// approach. Alternatively a UUID may be used.
	ID string

	// Datacenter for this node
	Datacenter string

	// Node name
	Name string

	// HTTPAddr is the address on which the Udup client is listening for http
	// requests
	HTTPAddr string

	NatsAddr string
	NatsAdvertiseAddr string

	// Attributes is an arbitrary set of key/value
	// data that can be used for constraints. Examples
	// include "kernel.name=linux", "arch=386", "driver.docker=1",
	// "docker.runtime=1.8.3"
	Attributes map[string]string

	// ComputedClass is a unique id that identifies nodes with a common set of
	// attributes and capabilities.
	ComputedClass string

	// Status of this node
	Status string

	// StatusDescription is meant to provide more human useful information
	StatusDescription string

	// StatusUpdatedAt is the time stamp at which the state of the node was
	// updated
	StatusUpdatedAt int64

	// Raft Indexes
	CreateIndex uint64
	ModifyIndex uint64
}

// Ready returns if the node is ready for running allocations
func (n *Node) Ready() bool {
	return n.Status == NodeStatusReady
}

func (n *Node) Copy() *Node {
	if n == nil {
		return nil
	}
	nn := new(Node)
	*nn = *n
	nn.Attributes = internal.CopyMapStringString(nn.Attributes)
	return nn
}

// TerminalStatus returns if the current status is terminal and
// will no longer transition.
func (n *Node) TerminalStatus() bool {
	switch n.Status {
	case NodeStatusDown:
		return true
	default:
		return false
	}
}

// Stub returns a summarized version of the node
func (n *Node) Stub() *NodeListStub {
	return &NodeListStub{
		ID:                n.ID,
		Datacenter:        n.Datacenter,
		Name:              n.Name,
		Status:            n.Status,
		HTTPAddr:          n.HTTPAddr,
		StatusDescription: n.StatusDescription,
		CreateIndex:       n.CreateIndex,
		ModifyIndex:       n.ModifyIndex,
	}
}

// NodeListStub is used to return a subset of job information
// for the job list
type NodeListStub struct {
	ID                string
	Datacenter        string
	Name              string
	HTTPAddr          string
	Status            string
	StatusDescription string
	CreateIndex       uint64
	ModifyIndex       uint64
}

// ServerMembersResponse has the list of servers in a cluster
type ServerMembersResponse struct {
	ServerName   string
	ServerRegion string
	ServerDC     string
	Members      []*ServerMember
}

// ServerMember holds information about a Udup server agent in a cluster
type ServerMember struct {
	Name        string
	Addr        net.IP
	Port        uint16
	Tags        map[string]string
	Status      string
	ProtocolMin uint8
	ProtocolMax uint8
	ProtocolCur uint8
	DelegateMin uint8
	DelegateMax uint8
	DelegateCur uint8
}

type Nodes []*Node

// Used to return information about a provided services.
// Maps service name to available tags
type Services map[string][]string

// ServiceNode represents a node that is part of a service. Address and
// TaggedAddresses are node-related fields that are always empty in the state
// store and are filled in on the way out by parseServiceNodes(). This is also
// why PartialClone() skips them, because we know they are blank already so it
// would be a waste of time to copy them.
type ServiceNode struct {
	Node                     string
	Address                  string
	TaggedAddresses          map[string]string
	ServiceID                string
	ServiceName              string
	ServiceTags              []string
	ServiceAddress           string
	ServicePort              int
	ServiceEnableTagOverride bool

	RaftIndex
}

// NodeService is a service provided by a node
type NodeService struct {
	ID                string
	Service           string
	Tags              []string
	Address           string
	Port              int
	EnableTagOverride bool

	RaftIndex
}

// NodeInfo is used to dump all associated information about
// a node. This is currently used for the UI only, as it is
// rather expensive to generate.
type NodeInfo struct {
	Node            string
	Address         string
	TaggedAddresses map[string]string
	Services        []*NodeService
	Checks          []*HealthCheck
}

// ToNodeService converts the given service node to a node service.
func (s *ServiceNode) ToNodeService() *NodeService {
	return &NodeService{
		ID:                s.ServiceID,
		Service:           s.ServiceName,
		Tags:              s.ServiceTags,
		Address:           s.ServiceAddress,
		Port:              s.ServicePort,
		EnableTagOverride: s.ServiceEnableTagOverride,
		RaftIndex: RaftIndex{
			CreateIndex: s.CreateIndex,
			ModifyIndex: s.ModifyIndex,
		},
	}
}

// CheckID is a strongly typed string used to uniquely represent a Consul
// Check on an Agent (a CheckID is not globally unique).
type CheckID string

// HealthCheck represents a single check on a given node
type HealthCheck struct {
	Node        string
	CheckID     CheckID // Unique per-node ID
	Name        string  // Check name
	Status      string  // The current check status
	Notes       string  // Additional notes with the status
	Output      string  // Holds output of script runs
	ServiceID   string  // optional associated service
	ServiceName string  // optional service name

	RaftIndex
}

// NodeDump is used to dump all the nodes with all their
// associated data. This is currently used for the UI only,
// as it is rather expensive to generate.
type NodeDump []*NodeInfo

type IndexedNodes struct {
	Nodes Nodes
	QueryMeta
}

type IndexedServices struct {
	Services Services
	QueryMeta
}

type NodeServices struct {
	Node     *Node
	Services map[string]*NodeService
}

type ServiceNodes []*ServiceNode

type HealthChecks []*HealthCheck

type IndexedServiceNodes struct {
	ServiceNodes ServiceNodes
	QueryMeta
}

type IndexedNodeServices struct {
	NodeServices *NodeServices
	QueryMeta
}

type IndexedHealthChecks struct {
	HealthChecks HealthChecks
	QueryMeta
}

type IndexedCheckServiceNodes struct {
	Nodes CheckServiceNodes
	QueryMeta
}

type IndexedNodeDump struct {
	Dump NodeDump
	QueryMeta
}

// CheckServiceNode is used to provide the node, its service
// definition, as well as a HealthCheck that is associated.
type CheckServiceNode struct {
	Node    *Node
	Service *NodeService
	Checks  HealthChecks
}

type CheckServiceNodes []CheckServiceNode

// RegisterRequest is used for the Catalog.Register endpoint
// to register a node as providing a service. If no service
// is provided, the node is registered.
type RegisterRequest struct {
	Datacenter      string
	Node            string
	Address         string
	TaggedAddresses map[string]string
	Service         *NodeService
	Check           *HealthCheck
	Checks          HealthChecks
	WriteRequest
}

func (r *RegisterRequest) RequestDatacenter() string {
	return r.Datacenter
}

// DeregisterRequest is used for the Catalog.Deregister endpoint
// to deregister a node as providing a service. If no service is
// provided the entire node is deregistered.
type DeregisterRequest struct {
	Datacenter string
	Node       string
	ServiceID  string
	CheckID    CheckID
	WriteRequest
}

func (r *DeregisterRequest) RequestDatacenter() string {
	return r.Datacenter
}

// QuerySource is used to pass along information about the source node
// in queries so that we can adjust the response based on its network
// coordinates.
type QuerySource struct {
	Datacenter string
	Node       string
}

// DCSpecificRequest is used to query about a specific DC
type DCSpecificRequest struct {
	Datacenter string
	Source     QuerySource
	QueryOptions
}

func (r *DCSpecificRequest) RequestDatacenter() string {
	return r.Datacenter
}

// Coordinate stores a node name with its associated network coordinate.
type Coordinate struct {
	Node  string
	Coord *coordinate.Coordinate
}

type Coordinates []*Coordinate

// IndexedCoordinates is used to represent a list of nodes and their
// corresponding raw coordinates.
type IndexedCoordinates struct {
	Coordinates Coordinates
	QueryMeta
}

// DatacenterMap is used to represent a list of nodes with their raw coordinates,
// associated with a datacenter.
type DatacenterMap struct {
	Datacenter  string
	Coordinates Coordinates
}

// CoordinateUpdateRequest is used to update the network coordinate of a given
// node.
type CoordinateUpdateRequest struct {
	Datacenter string
	Node       string
	Coord      *coordinate.Coordinate
	WriteRequest
}

// RequestDatacenter returns the datacenter for a given update request.
func (c *CoordinateUpdateRequest) RequestDatacenter() string {
	return c.Datacenter
}
