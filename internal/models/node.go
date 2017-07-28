package models

import (
	"net"
	"time"

	"udup/internal"
)

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
	NodeID string
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
