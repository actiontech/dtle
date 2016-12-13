package structs

import (
	"bytes"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gorhill/cronexpr"
	"github.com/hashicorp/go-multierror"
	"github.com/mitchellh/copystructure"
	"github.com/ugorji/go/codec"

	hcodec "github.com/hashicorp/go-msgpack/codec"
)

var (
	ErrNoLeader     = fmt.Errorf("No cluster leader")
	ErrNoRegionPath = fmt.Errorf("No path to region")
)

type MessageType uint8

const (
	NodeRegisterRequestType MessageType = iota
	NodeDeregisterRequestType
	NodeUpdateStatusRequestType
	NodeUpdateDrainRequestType
	JobRegisterRequestType
	JobDeregisterRequestType
	EvalUpdateRequestType
	EvalDeleteRequestType
	AllocUpdateRequestType
	AllocClientUpdateRequestType
	ReconcileJobSummariesRequestType
)

const (
	// IgnoreUnknownTypeFlag is set along with a MessageType
	// to indicate that the message type can be safely ignored
	// if it is not recognized. This is for future proofing, so
	// that new commands can be added in a way that won't cause
	// old servers to crash when the FSM attempts to process them.
	IgnoreUnknownTypeFlag MessageType = 128

	// ApiMajorVersion is returned as part of the Status.Version request.
	// It should be incremented anytime the APIs are changed in a way
	// that would break clients for sane client versioning.
	ApiMajorVersion = 1

	// ApiMinorVersion is returned as part of the Status.Version request.
	// It should be incremented anytime the APIs are changed to allow
	// for sane client versioning. Minor changes should be compatible
	// within the major version.
	ApiMinorVersion = 1

	ProtocolVersion = "protocol"
	APIMajorVersion = "api.major"
	APIMinorVersion = "api.minor"
)

// RPCInfo is used to describe common information about query
type RPCInfo interface {
	RequestRegion() string
	IsRead() bool
	AllowStaleRead() bool
}

// QueryOptions is used to specify various flags for read queries
type QueryOptions struct {
	// The target region for this query
	Region string

	// If set, wait until query exceeds given index. Must be provided
	// with MaxQueryTime.
	MinQueryIndex uint64

	// Provided with MinQueryIndex to wait for change.
	MaxQueryTime time.Duration

	// If set, any follower can service the request. Results
	// may be arbitrarily stale.
	AllowStale bool

	// If set, used as prefix for resource list searches
	Prefix string
}

func (q QueryOptions) RequestRegion() string {
	return q.Region
}

// QueryOption only applies to reads, so always true
func (q QueryOptions) IsRead() bool {
	return true
}

func (q QueryOptions) AllowStaleRead() bool {
	return q.AllowStale
}

type WriteRequest struct {
	// The target region for this write
	Region string
}

func (w WriteRequest) RequestRegion() string {
	// The target region for this request
	return w.Region
}

// WriteRequest only applies to writes, always false
func (w WriteRequest) IsRead() bool {
	return false
}

func (w WriteRequest) AllowStaleRead() bool {
	return false
}

// QueryMeta allows a query response to include potentially
// useful metadata about a query
type QueryMeta struct {
	// This is the index associated with the read
	Index uint64

	// If AllowStale is used, this is time elapsed since
	// last contact between the follower and leader. This
	// can be used to gauge staleness.
	LastContact time.Duration

	// Used to indicate if there is a known leader node
	KnownLeader bool
}

// WriteMeta allows a write response to include potentially
// useful metadata about the write
type WriteMeta struct {
	// This is the index associated with the write
	Index uint64
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

// NodeUpdateDrainRequest is used for updatin the drain status
type NodeUpdateDrainRequest struct {
	NodeID string
	Drain  bool
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

// JobRegisterRequest is used for Job.Register endpoint
// to register a job as being a schedulable entity.
type JobRegisterRequest struct {
	Job *Job

	// If EnforceIndex is set then the job will only be registered if the passed
	// JobModifyIndex matches the current Jobs index. If the index is zero, the
	// register only occurs if the job is new.
	EnforceIndex   bool
	JobModifyIndex uint64

	WriteRequest
}

// JobDeregisterRequest is used for Job.Deregister endpoint
// to deregister a job as being a schedulable entity.
type JobDeregisterRequest struct {
	JobID string
	WriteRequest
}

// JobEvaluateRequest is used when we just need to re-evaluate a target job
type JobEvaluateRequest struct {
	JobID string
	WriteRequest
}

// JobSpecificRequest is used when we just need to specify a target job
type JobSpecificRequest struct {
	JobID string
	QueryOptions
}

// JobListRequest is used to parameterize a list request
type JobListRequest struct {
	QueryOptions
}

// JobPlanRequest is used for the Job.Plan endpoint to trigger a dry-run
// evaluation of the Job.
type JobPlanRequest struct {
	Job  *Job
	Diff bool // Toggles an annotated diff
	WriteRequest
}

// JobSummaryRequest is used when we just need to get a specific job summary
type JobSummaryRequest struct {
	JobID string
	QueryOptions
}

// NodeListRequest is used to parameterize a list request
type NodeListRequest struct {
	QueryOptions
}

// EvalUpdateRequest is used for upserting evaluations.
type EvalUpdateRequest struct {
	Evals     []*Evaluation
	EvalToken string
	WriteRequest
}

// EvalDeleteRequest is used for deleting an evaluation.
type EvalDeleteRequest struct {
	Evals  []string
	Allocs []string
	WriteRequest
}

// EvalSpecificRequest is used when we just need to specify a target evaluation
type EvalSpecificRequest struct {
	EvalID string
	QueryOptions
}

// EvalAckRequest is used to Ack/Nack a specific evaluation
type EvalAckRequest struct {
	EvalID string
	Token  string
	WriteRequest
}

// EvalDequeueRequest is used when we want to dequeue an evaluation
type EvalDequeueRequest struct {
	Schedulers []string
	Timeout    time.Duration
	WriteRequest
}

// EvalListRequest is used to list the evaluations
type EvalListRequest struct {
	QueryOptions
}

// PlanRequest is used to submit an allocation plan to the leader
type PlanRequest struct {
	Plan *Plan
	WriteRequest
}

// AllocUpdateRequest is used to submit changes to allocations, either
// to cause evictions or to assign new allocaitons. Both can be done
// within a single transaction
type AllocUpdateRequest struct {
	// Alloc is the list of new allocations to assign
	Alloc []*Allocation

	// Job is the shared parent job of the allocations.
	// It is pulled out since it is common to reduce payload size.
	Job *Job

	WriteRequest
}

// AllocListRequest is used to request a list of allocations
type AllocListRequest struct {
	QueryOptions
}

// AllocSpecificRequest is used to query a specific allocation
type AllocSpecificRequest struct {
	AllocID string
	QueryOptions
}

// AllocsGetRequest is used to query a set of allocations
type AllocsGetRequest struct {
	AllocIDs []string
	QueryOptions
}

// PeriodicForceReqeuest is used to force a specific periodic job.
type PeriodicForceRequest struct {
	JobID string
	WriteRequest
}

// GenericRequest is used to request where no
// specific information is needed.
type GenericRequest struct {
	QueryOptions
}

// GenericResponse is used to respond to a request where no
// specific response information is needed.
type GenericResponse struct {
	WriteMeta
}

// VersionResponse is used for the Status.Version reseponse
type VersionResponse struct {
	Build    string
	Versions map[string]int
	QueryMeta
}

// JobRegisterResponse is used to respond to a job registration
type JobRegisterResponse struct {
	EvalID          string
	EvalCreateIndex uint64
	JobModifyIndex  uint64
	QueryMeta
}

// JobDeregisterResponse is used to respond to a job deregistration
type JobDeregisterResponse struct {
	EvalID          string
	EvalCreateIndex uint64
	JobModifyIndex  uint64
	QueryMeta
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

// NodeDrainUpdateResponse is used to respond to a node drain update
type NodeDrainUpdateResponse struct {
	EvalIDs         []string
	EvalCreateIndex uint64
	NodeModifyIndex uint64
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

// SingleJobResponse is used to return a single job
type SingleJobResponse struct {
	Job *Job
	QueryMeta
}

// JobSummaryResponse is used to return a single job summary
type JobSummaryResponse struct {
	JobSummary *JobSummary
	QueryMeta
}

// JobListResponse is used for a list request
type JobListResponse struct {
	Jobs []*JobListStub
	QueryMeta
}

// JobPlanResponse is used to respond to a job plan request
type JobPlanResponse struct {
	// Annotations stores annotations explaining decisions the scheduler made.
	Annotations *PlanAnnotations

	// FailedTGAllocs is the placement failures per task group.
	FailedTGAllocs map[string]*AllocMetric

	// JobModifyIndex is the modification index of the job. The value can be
	// used when running `udup run` to ensure that the Job wasn’t modified
	// since the last plan. If the job is being created, the value is zero.
	JobModifyIndex uint64

	// CreatedEvals is the set of evaluations created by the scheduler. The
	// reasons for this can be rolling-updates or blocked evals.
	CreatedEvals []*Evaluation

	// Diff contains the diff of the job and annotations on whether the change
	// causes an in-place update or create/destroy
	Diff *JobDiff

	// NextPeriodicLaunch is the time duration till the job would be launched if
	// submitted.
	NextPeriodicLaunch time.Time

	WriteMeta
}

// SingleAllocResponse is used to return a single allocation
type SingleAllocResponse struct {
	Alloc *Allocation
	QueryMeta
}

// AllocsGetResponse is used to return a set of allocations
type AllocsGetResponse struct {
	Allocs []*Allocation
	QueryMeta
}

// JobAllocationsResponse is used to return the allocations for a job
type JobAllocationsResponse struct {
	Allocations []*AllocListStub
	QueryMeta
}

// JobEvaluationsResponse is used to return the evaluations for a job
type JobEvaluationsResponse struct {
	Evaluations []*Evaluation
	QueryMeta
}

// SingleEvalResponse is used to return a single evaluation
type SingleEvalResponse struct {
	Eval *Evaluation
	QueryMeta
}

// EvalDequeueResponse is used to return from a dequeue
type EvalDequeueResponse struct {
	Eval  *Evaluation
	Token string
	QueryMeta
}

// PlanResponse is used to return from a PlanRequest
type PlanResponse struct {
	Result *PlanResult
	WriteMeta
}

// AllocListResponse is used for a list request
type AllocListResponse struct {
	Allocations []*AllocListStub
	QueryMeta
}

// EvalListResponse is used for a list request
type EvalListResponse struct {
	Evaluations []*Evaluation
	QueryMeta
}

// EvalAllocationsResponse is used to return the allocations for an evaluation
type EvalAllocationsResponse struct {
	Allocations []*AllocListStub
	QueryMeta
}

// PeriodicForceResponse is used to respond to a periodic job force launch
type PeriodicForceResponse struct {
	EvalID          string
	EvalCreateIndex uint64
	WriteMeta
}

const (
	NodeStatusInit  = "initializing"
	NodeStatusReady = "ready"
	NodeStatusDown  = "down"
)

// ShouldDrainNode checks if a given node status should trigger an
// evaluation. Some states don't require any further action.
func ShouldDrainNode(status string) bool {
	switch status {
	case NodeStatusInit, NodeStatusReady:
		return false
	case NodeStatusDown:
		return true
	default:
		panic(fmt.Sprintf("unhandled node status %s", status))
	}
}

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

	// Resources is the available resources on the client.
	// For example 'cpu=2' 'memory=2048'
	Resources *Resources

	// Reserved is the set of resources that are reserved,
	// and should be subtracted from the total resources for
	// the purposes of scheduling. This may be provide certain
	// high-watermark tolerances or because of external schedulers
	// consuming resources.
	Reserved *Resources

	// Links are used to 'link' this client to external
	// systems. For example 'consul=foo.dc1' 'aws=i-83212'
	// 'ami=ami-123'
	Links map[string]string

	// Meta is used to associate arbitrary metadata with this
	// client. This is opaque to Udup.
	Meta map[string]string

	// NodeClass is an opaque identifier used to group nodes
	// together for the purpose of determining scheduling pressure.
	NodeClass string

	// ComputedClass is a unique id that identifies nodes with a common set of
	// attributes and capabilities.
	ComputedClass string

	// Drain is controlled by the servers, and not the client.
	// If true, no jobs will be scheduled to this node, and existing
	// allocations will be drained.
	Drain bool

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
	return n.Status == NodeStatusReady && !n.Drain
}

func (n *Node) Copy() *Node {
	if n == nil {
		return nil
	}
	nn := new(Node)
	*nn = *n
	nn.Attributes = CopyMapStringString(nn.Attributes)
	nn.Resources = nn.Resources.Copy()
	nn.Reserved = nn.Reserved.Copy()
	nn.Links = CopyMapStringString(nn.Links)
	nn.Meta = CopyMapStringString(nn.Meta)
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
		NatsAddr:          n.NatsAddr,
		Name:              n.Name,
		NodeClass:         n.NodeClass,
		Drain:             n.Drain,
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
	NatsAddr          string
	Name              string
	NodeClass         string
	Drain             bool
	Status            string
	StatusDescription string
	CreateIndex       uint64
	ModifyIndex       uint64
}

// Resources is used to define the resources available
// on a client
type Resources struct {
	CPU      int
	MemoryMB int `mapstructure:"memory"`
	DiskMB   int `mapstructure:"disk"`
	IOPS     int
	Networks []*NetworkResource
}

const (
	BytesInMegabyte = 1024 * 1024
)

// DefaultResources returns the default resources for a task.
func DefaultResources() *Resources {
	return &Resources{
		CPU:      100,
		MemoryMB: 10,
		IOPS:     0,
	}
}

// DiskInBytes returns the amount of disk resources in bytes.
func (r *Resources) DiskInBytes() int64 {
	return int64(r.DiskMB * BytesInMegabyte)
}

// Merge merges this resource with another resource.
func (r *Resources) Merge(other *Resources) {
	if other.CPU != 0 {
		r.CPU = other.CPU
	}
	if other.MemoryMB != 0 {
		r.MemoryMB = other.MemoryMB
	}
	if other.DiskMB != 0 {
		r.DiskMB = other.DiskMB
	}
	if other.IOPS != 0 {
		r.IOPS = other.IOPS
	}
	if len(other.Networks) != 0 {
		r.Networks = other.Networks
	}
}

func (r *Resources) Canonicalize() {
	// Ensure that an empty and nil slices are treated the same to avoid scheduling
	// problems since we use reflect DeepEquals.
	if len(r.Networks) == 0 {
		r.Networks = nil
	}

	for _, n := range r.Networks {
		n.Canonicalize()
	}
}

// MeetsMinResources returns an error if the resources specified are less than
// the minimum allowed.
func (r *Resources) MeetsMinResources() error {
	var mErr multierror.Error
	if r.CPU < 20 {
		mErr.Errors = append(mErr.Errors, fmt.Errorf("minimum CPU value is 20; got %d", r.CPU))
	}
	if r.MemoryMB < 10 {
		mErr.Errors = append(mErr.Errors, fmt.Errorf("minimum MemoryMB value is 10; got %d", r.MemoryMB))
	}
	if r.IOPS < 0 {
		mErr.Errors = append(mErr.Errors, fmt.Errorf("minimum IOPS value is 0; got %d", r.IOPS))
	}
	for i, n := range r.Networks {
		if err := n.MeetsMinResources(); err != nil {
			mErr.Errors = append(mErr.Errors, fmt.Errorf("network resource at index %d failed: %v", i, err))
		}
	}

	return mErr.ErrorOrNil()
}

// Copy returns a deep copy of the resources
func (r *Resources) Copy() *Resources {
	if r == nil {
		return nil
	}
	newR := new(Resources)
	*newR = *r
	if r.Networks != nil {
		n := len(r.Networks)
		newR.Networks = make([]*NetworkResource, n)
		for i := 0; i < n; i++ {
			newR.Networks[i] = r.Networks[i].Copy()
		}
	}
	return newR
}

// NetIndex finds the matching net index using device name
func (r *Resources) NetIndex(n *NetworkResource) int {
	for idx, net := range r.Networks {
		if net.Device == n.Device {
			return idx
		}
	}
	return -1
}

// Superset checks if one set of resources is a superset
// of another. This ignores network resources, and the NetworkIndex
// should be used for that.
func (r *Resources) Superset(other *Resources) (bool, string) {
	if r.CPU < other.CPU {
		return false, "cpu exhausted"
	}
	if r.MemoryMB < other.MemoryMB {
		return false, "memory exhausted"
	}
	if r.DiskMB < other.DiskMB {
		return false, "disk exhausted"
	}
	if r.IOPS < other.IOPS {
		return false, "iops exhausted"
	}
	return true, ""
}

// Add adds the resources of the delta to this, potentially
// returning an error if not possible.
func (r *Resources) Add(delta *Resources) error {
	if delta == nil {
		return nil
	}
	r.CPU += delta.CPU
	r.MemoryMB += delta.MemoryMB
	r.DiskMB += delta.DiskMB
	r.IOPS += delta.IOPS

	for _, n := range delta.Networks {
		// Find the matching interface by IP or CIDR
		idx := r.NetIndex(n)
		if idx == -1 {
			r.Networks = append(r.Networks, n.Copy())
		} else {
			r.Networks[idx].Add(n)
		}
	}
	return nil
}

func (r *Resources) GoString() string {
	return fmt.Sprintf("*%#v", *r)
}

type Port struct {
	Label string
	Value int `mapstructure:"static"`
}

// NetworkResource is used to represent available network
// resources
type NetworkResource struct {
	Device        string // Name of the device
	CIDR          string // CIDR block of addresses
	IP            string // IP address
	MBits         int    // Throughput
	ReservedPorts []Port // Reserved ports
	DynamicPorts  []Port // Dynamically assigned ports
}

func (n *NetworkResource) Canonicalize() {
	// Ensure that an empty and nil slices are treated the same to avoid scheduling
	// problems since we use reflect DeepEquals.
	if len(n.ReservedPorts) == 0 {
		n.ReservedPorts = nil
	}
	if len(n.DynamicPorts) == 0 {
		n.DynamicPorts = nil
	}
}

// MeetsMinResources returns an error if the resources specified are less than
// the minimum allowed.
func (n *NetworkResource) MeetsMinResources() error {
	var mErr multierror.Error
	if n.MBits < 1 {
		mErr.Errors = append(mErr.Errors, fmt.Errorf("minimum MBits value is 1; got %d", n.MBits))
	}
	return mErr.ErrorOrNil()
}

// Copy returns a deep copy of the network resource
func (n *NetworkResource) Copy() *NetworkResource {
	if n == nil {
		return nil
	}
	newR := new(NetworkResource)
	*newR = *n
	if n.ReservedPorts != nil {
		newR.ReservedPorts = make([]Port, len(n.ReservedPorts))
		copy(newR.ReservedPorts, n.ReservedPorts)
	}
	if n.DynamicPorts != nil {
		newR.DynamicPorts = make([]Port, len(n.DynamicPorts))
		copy(newR.DynamicPorts, n.DynamicPorts)
	}
	return newR
}

// Add adds the resources of the delta to this, potentially
// returning an error if not possible.
func (n *NetworkResource) Add(delta *NetworkResource) {
	if len(delta.ReservedPorts) > 0 {
		n.ReservedPorts = append(n.ReservedPorts, delta.ReservedPorts...)
	}
	n.MBits += delta.MBits
	n.DynamicPorts = append(n.DynamicPorts, delta.DynamicPorts...)
}

func (n *NetworkResource) GoString() string {
	return fmt.Sprintf("*%#v", *n)
}

func (n *NetworkResource) MapLabelToValues(port_map map[string]int) map[string]int {
	labelValues := make(map[string]int)
	ports := append(n.ReservedPorts, n.DynamicPorts...)
	for _, port := range ports {
		if mapping, ok := port_map[port.Label]; ok {
			labelValues[port.Label] = mapping
		} else {
			labelValues[port.Label] = port.Value
		}
	}
	return labelValues
}

const (
	// JobTypeUdup is reserved for internal system tasks and is
	// always handled by the CoreScheduler.
	JobTypeCore    = "_core"
	JobTypeSync    = "sync"
	JobTypeSub     = "sub"
	JobTypeMigrate = "migrate"
)

const (
	JobStatusPending = "pending" // Pending means the job is waiting on scheduling
	JobStatusRunning = "running" // Running means the job has non-terminal allocations
	JobStatusDead    = "dead"    // Dead means all evaluation's and allocations are terminal
)

const (
	// JobMinPriority is the minimum allowed priority
	JobMinPriority = 1

	// JobDefaultPriority is the default priority if not
	// not specified.
	JobDefaultPriority = 50

	// JobMaxPriority is the maximum allowed priority
	JobMaxPriority = 100

	// Ensure CoreJobPriority is higher than any user
	// specified job so that it gets priority. This is important
	// for the system to remain healthy.
	CoreJobPriority = JobMaxPriority * 2
)

// JobSummary summarizes the state of the allocations of a job
type JobSummary struct {
	JobID   string
	Summary map[string]TaskGroupSummary

	// Raft Indexes
	CreateIndex uint64
	ModifyIndex uint64
}

// Copy returns a new copy of JobSummary
func (js *JobSummary) Copy() *JobSummary {
	newJobSummary := new(JobSummary)
	*newJobSummary = *js
	newTGSummary := make(map[string]TaskGroupSummary, len(js.Summary))
	for k, v := range js.Summary {
		newTGSummary[k] = v
	}
	newJobSummary.Summary = newTGSummary
	return newJobSummary
}

// TaskGroup summarizes the state of all the allocations of a particular
// TaskGroup
type TaskGroupSummary struct {
	Queued   int
	Complete int
	Failed   int
	Running  int
	Starting int
	Lost     int
}

// Job is the scope of a scheduling request to Udup. It is the largest
// scoped object, and is a named collection of task groups. Each task group
// is further composed of tasks. A task group (TG) is the unit of scheduling
// however.
type Job struct {
	// Region is the Udup region that handles scheduling this job
	Region string

	// ID is a unique identifier for the job per region. It can be
	// specified hierarchically like LineOfBiz/OrgName/Team/Project
	ID string

	// ParentID is the unique identifier of the job that spawned this job.
	ParentID string

	// Name is the logical name of the job used to refer to it. This is unique
	// per region, but not unique globally.
	Name string

	// Type is used to control various behaviors about the job. Most jobs
	// are service jobs, meaning they are expected to be long lived.
	// Some jobs are batch oriented meaning they run and then terminate.
	// This can be extended in the future to support custom schedulers.
	Type string

	// Priority is used to control scheduling importance and if this job
	// can preempt other jobs.
	Priority int

	// AllAtOnce is used to control if incremental scheduling of task groups
	// is allowed or if we must do a gang scheduling of the entire job. This
	// can slow down larger jobs if resources are not available.
	AllAtOnce bool `mapstructure:"all_at_once"`

	// Datacenters contains all the datacenters this job is allowed to span
	Datacenters []string

	// TaskGroups are the collections of task groups that this job needs
	// to run. Each task group is an atomic unit of scheduling and placement.
	TaskGroups []*TaskGroup

	// Update is used to control the update strategy
	Update UpdateStrategy

	// Periodic is used to define the interval the job is run at.
	Periodic *PeriodicConfig

	// Meta is used to associate arbitrary metadata with this
	// job. This is opaque to Udup.
	Meta map[string]string

	// Job status
	Status string

	// StatusDescription is meant to provide more human useful information
	StatusDescription string

	// Raft Indexes
	CreateIndex    uint64
	ModifyIndex    uint64
	JobModifyIndex uint64
}

// Canonicalize is used to canonicalize fields in the Job. This should be called
// when registering a Job.
func (j *Job) Canonicalize() {
	// Ensure that an empty and nil map are treated the same to avoid scheduling
	// problems since we use reflect DeepEquals.
	if len(j.Meta) == 0 {
		j.Meta = nil
	}

	for _, tg := range j.TaskGroups {
		tg.Canonicalize(j)
	}
}

// Copy returns a deep copy of the Job. It is expected that callers use recover.
// This job can panic if the deep copy failed as it uses reflection.
func (j *Job) Copy() *Job {
	if j == nil {
		return nil
	}
	nj := new(Job)
	*nj = *j
	nj.Datacenters = CopySliceString(nj.Datacenters)

	if j.TaskGroups != nil {
		tgs := make([]*TaskGroup, len(nj.TaskGroups))
		for i, tg := range nj.TaskGroups {
			tgs[i] = tg.Copy()
		}
		nj.TaskGroups = tgs
	}

	nj.Periodic = nj.Periodic.Copy()
	nj.Meta = CopyMapStringString(nj.Meta)
	return nj
}

// Validate is used to sanity check a job input
func (j *Job) Validate() error {
	var mErr multierror.Error
	if j.Region == "" {
		mErr.Errors = append(mErr.Errors, errors.New("Missing job region"))
	}
	if j.ID == "" {
		mErr.Errors = append(mErr.Errors, errors.New("Missing job ID"))
	} else if strings.Contains(j.ID, " ") {
		mErr.Errors = append(mErr.Errors, errors.New("Job ID contains a space"))
	}
	if j.Name == "" {
		mErr.Errors = append(mErr.Errors, errors.New("Missing job name"))
	}
	if j.Type == "" {
		mErr.Errors = append(mErr.Errors, errors.New("Missing job type"))
	}
	if j.Priority < JobMinPriority || j.Priority > JobMaxPriority {
		mErr.Errors = append(mErr.Errors, fmt.Errorf("Job priority must be between [%d, %d]", JobMinPriority, JobMaxPriority))
	}
	if len(j.Datacenters) == 0 {
		mErr.Errors = append(mErr.Errors, errors.New("Missing job datacenters"))
	}
	if len(j.TaskGroups) == 0 {
		mErr.Errors = append(mErr.Errors, errors.New("Missing job task groups"))
	}

	// Check for duplicate task groups
	taskGroups := make(map[string]int)
	for idx, tg := range j.TaskGroups {
		if tg.Name == "" {
			mErr.Errors = append(mErr.Errors, fmt.Errorf("Job task group %d missing name", idx+1))
		} else if existing, ok := taskGroups[tg.Name]; ok {
			mErr.Errors = append(mErr.Errors, fmt.Errorf("Job task group %d redefines '%s' from group %d", idx+1, tg.Name, existing+1))
		} else {
			taskGroups[tg.Name] = idx
		}
	}

	// Validate the task group
	for _, tg := range j.TaskGroups {
		if err := tg.Validate(); err != nil {
			outer := fmt.Errorf("Task group %s validation failed: %s", tg.Name, err)
			mErr.Errors = append(mErr.Errors, outer)
		}
	}

	// Validate periodic is only used with batch jobs.
	if j.IsPeriodic() && j.Periodic.Enabled {
		if j.Type != JobTypeMigrate {
			mErr.Errors = append(mErr.Errors,
				fmt.Errorf("Periodic can only be used with %q scheduler", JobTypeMigrate))
		}

		if err := j.Periodic.Validate(); err != nil {
			mErr.Errors = append(mErr.Errors, err)
		}
	}

	return mErr.ErrorOrNil()
}

// LookupTaskGroup finds a task group by name
func (j *Job) LookupTaskGroup(name string) *TaskGroup {
	for _, tg := range j.TaskGroups {
		if tg.Name == name {
			return tg
		}
	}
	return nil
}

// LookupAnotherTaskGroup finds a task group by name
func (j *Job) LookupAnotherTaskGroup(name string) *TaskGroup {
	for _, tg := range j.TaskGroups {
		if tg.Name != name {
			return tg
		}
	}
	return nil
}

// Stub is used to return a summary of the job
func (j *Job) Stub(summary *JobSummary) *JobListStub {
	return &JobListStub{
		ID:                j.ID,
		ParentID:          j.ParentID,
		Name:              j.Name,
		Type:              j.Type,
		Priority:          j.Priority,
		Status:            j.Status,
		StatusDescription: j.StatusDescription,
		CreateIndex:       j.CreateIndex,
		ModifyIndex:       j.ModifyIndex,
		JobModifyIndex:    j.JobModifyIndex,
		JobSummary:        summary,
	}
}

// IsPeriodic returns whether a job is periodic.
func (j *Job) IsPeriodic() bool {
	return j.Periodic != nil
}

// JobListStub is used to return a subset of job information
// for the job list
type JobListStub struct {
	ID                string
	ParentID          string
	Name              string
	Type              string
	Priority          int
	Status            string
	StatusDescription string
	JobSummary        *JobSummary
	CreateIndex       uint64
	ModifyIndex       uint64
	JobModifyIndex    uint64
}

// UpdateStrategy is used to modify how updates are done
type UpdateStrategy struct {
	// Stagger is the amount of time between the updates
	Stagger time.Duration

	// MaxParallel is how many updates can be done in parallel
	MaxParallel int `mapstructure:"max_parallel"`
}

// Rolling returns if a rolling strategy should be used
func (u *UpdateStrategy) Rolling() bool {
	return u.Stagger > 0 && u.MaxParallel > 0
}

const (
	// PeriodicSpecCron is used for a cron spec.
	PeriodicSpecCron = "cron"

	// PeriodicSpecTest is only used by unit tests. It is a sorted, comma
	// separated list of unix timestamps at which to launch.
	PeriodicSpecTest = "_internal_test"
)

// Periodic defines the interval a job should be run at.
type PeriodicConfig struct {
	// Enabled determines if the job should be run periodically.
	Enabled bool

	// Spec specifies the interval the job should be run as. It is parsed based
	// on the SpecType.
	Spec string

	// SpecType defines the format of the spec.
	SpecType string

	// ProhibitOverlap enforces that spawned jobs do not run in parallel.
	ProhibitOverlap bool `mapstructure:"prohibit_overlap"`
}

func (p *PeriodicConfig) Copy() *PeriodicConfig {
	if p == nil {
		return nil
	}
	np := new(PeriodicConfig)
	*np = *p
	return np
}

func (p *PeriodicConfig) Validate() error {
	if !p.Enabled {
		return nil
	}

	if p.Spec == "" {
		return fmt.Errorf("Must specify a spec")
	}

	switch p.SpecType {
	case PeriodicSpecCron:
		// Validate the cron spec
		if _, err := cronexpr.Parse(p.Spec); err != nil {
			return fmt.Errorf("Invalid cron spec %q: %v", p.Spec, err)
		}
	case PeriodicSpecTest:
		// No-op
	default:
		return fmt.Errorf("Unknown periodic specification type %q", p.SpecType)
	}

	return nil
}

// Next returns the closest time instant matching the spec that is after the
// passed time. If no matching instance exists, the zero value of time.Time is
// returned. The `time.Location` of the returned value matches that of the
// passed time.
func (p *PeriodicConfig) Next(fromTime time.Time) time.Time {
	switch p.SpecType {
	case PeriodicSpecCron:
		if e, err := cronexpr.Parse(p.Spec); err == nil {
			return e.Next(fromTime)
		}
	case PeriodicSpecTest:
		split := strings.Split(p.Spec, ",")
		if len(split) == 1 && split[0] == "" {
			return time.Time{}
		}

		// Parse the times
		times := make([]time.Time, len(split))
		for i, s := range split {
			unix, err := strconv.Atoi(s)
			if err != nil {
				return time.Time{}
			}

			times[i] = time.Unix(int64(unix), 0)
		}

		// Find the next match
		for _, next := range times {
			if fromTime.Before(next) {
				return next
			}
		}
	}

	return time.Time{}
}

const (
	// PeriodicLaunchSuffix is the string appended to the periodic jobs ID
	// when launching derived instances of it.
	PeriodicLaunchSuffix = "/periodic-"
)

// PeriodicLaunch tracks the last launch time of a periodic job.
type PeriodicLaunch struct {
	ID     string    // ID of the periodic job.
	Launch time.Time // The last launch time.

	// Raft Indexes
	CreateIndex uint64
	ModifyIndex uint64
}

var (
	defaultServiceJobRestartPolicy = RestartPolicy{
		Delay:    15 * time.Second,
		Attempts: 2,
		Interval: 1 * time.Minute,
		Mode:     RestartPolicyModeDelay,
	}
	defaultBatchJobRestartPolicy = RestartPolicy{
		Delay:    15 * time.Second,
		Attempts: 15,
		Interval: 7 * 24 * time.Hour,
		Mode:     RestartPolicyModeDelay,
	}
)

const (
	// RestartPolicyModeDelay causes an artificial delay till the next interval is
	// reached when the specified attempts have been reached in the interval.
	RestartPolicyModeDelay = "delay"

	// RestartPolicyModeFail causes a job to fail if the specified number of
	// attempts are reached within an interval.
	RestartPolicyModeFail = "fail"
)

// RestartPolicy configures how Tasks are restarted when they crash or fail.
type RestartPolicy struct {
	// Attempts is the number of restart that will occur in an interval.
	Attempts int

	// Interval is a duration in which we can limit the number of restarts
	// within.
	Interval time.Duration

	// Delay is the time between a failure and a restart.
	Delay time.Duration

	// Mode controls what happens when the task restarts more than attempt times
	// in an interval.
	Mode string
}

func (r *RestartPolicy) Copy() *RestartPolicy {
	if r == nil {
		return nil
	}
	nrp := new(RestartPolicy)
	*nrp = *r
	return nrp
}

func (r *RestartPolicy) Validate() error {
	switch r.Mode {
	case RestartPolicyModeDelay, RestartPolicyModeFail:
	default:
		return fmt.Errorf("Unsupported restart mode: %q", r.Mode)
	}

	// Check for ambiguous/confusing settings
	if r.Attempts == 0 && r.Mode != RestartPolicyModeFail {
		return fmt.Errorf("Restart policy %q with %d attempts is ambiguous", r.Mode, r.Attempts)
	}

	if r.Interval == 0 {
		return nil
	}
	if time.Duration(r.Attempts)*r.Delay > r.Interval {
		return fmt.Errorf("Udup can't restart the TaskGroup %v times in an interval of %v with a delay of %v", r.Attempts, r.Interval, r.Delay)
	}
	return nil
}

func NewRestartPolicy(jobType string) *RestartPolicy {
	switch jobType {
	case JobTypeMigrate, JobTypeSub:
		rp := defaultServiceJobRestartPolicy
		return &rp
	case JobTypeSync:
		rp := defaultBatchJobRestartPolicy
		return &rp
	}
	return nil
}

const (
	TaskTypeExtract = "extract"
	TaskTypeReplay  = "replay"
)

// TaskGroup is an atomic unit of placement. Each task group belongs to
// a job and may contain any number of tasks. A task group support running
// in many replicas using the same configuration..
type TaskGroup struct {
	// Name of the task group
	Name string

	NodeID string

	//RestartPolicy of a TaskGroup
	RestartPolicy *RestartPolicy

	// Tasks are the collection of tasks that this task group needs to run
	Tasks []*Task

	// EphemeralDisk is the disk resources that the task group requests
	EphemeralDisk *EphemeralDisk

	// Meta is used to associate arbitrary metadata with this
	// task group. This is opaque to Udup.
	Meta map[string]string
}

func (tg *TaskGroup) Copy() *TaskGroup {
	if tg == nil {
		return nil
	}
	ntg := new(TaskGroup)
	*ntg = *tg

	ntg.RestartPolicy = ntg.RestartPolicy.Copy()

	if tg.Tasks != nil {
		tasks := make([]*Task, len(ntg.Tasks))
		for i, t := range ntg.Tasks {
			tasks[i] = t.Copy()
		}
		ntg.Tasks = tasks
	}

	ntg.Meta = CopyMapStringString(ntg.Meta)

	if tg.EphemeralDisk != nil {
		ntg.EphemeralDisk = tg.EphemeralDisk.Copy()
	}
	return ntg
}

// Canonicalize is used to canonicalize fields in the TaskGroup.
func (tg *TaskGroup) Canonicalize(job *Job) {
	// Ensure that an empty and nil map are treated the same to avoid scheduling
	// problems since we use reflect DeepEquals.
	if len(tg.Meta) == 0 {
		tg.Meta = nil
	}

	// Set the default restart policy.
	if tg.RestartPolicy == nil {
		tg.RestartPolicy = NewRestartPolicy(job.Type)
	}

	for _, task := range tg.Tasks {
		task.Canonicalize(job, tg)
	}
}

// Validate is used to sanity check a task group
func (tg *TaskGroup) Validate() error {
	var mErr multierror.Error
	if tg.Name == "" {
		mErr.Errors = append(mErr.Errors, errors.New("Missing task group name"))
	}

	if len(tg.Tasks) == 0 {
		mErr.Errors = append(mErr.Errors, errors.New("Missing tasks for task group"))
	}

	if tg.RestartPolicy != nil {
		if err := tg.RestartPolicy.Validate(); err != nil {
			mErr.Errors = append(mErr.Errors, err)
		}
	} else {
		mErr.Errors = append(mErr.Errors, fmt.Errorf("Task Group %v should have a restart policy", tg.Name))
	}

	if tg.EphemeralDisk != nil {
		if err := tg.EphemeralDisk.Validate(); err != nil {
			mErr.Errors = append(mErr.Errors, err)
		}
	} else {
		mErr.Errors = append(mErr.Errors, fmt.Errorf("Task Group %v should have a ephemeral disk object", tg.Name))
	}

	// Check for duplicate tasks
	tasks := make(map[string]int)
	for idx, task := range tg.Tasks {
		if task.Name == "" {
			mErr.Errors = append(mErr.Errors, fmt.Errorf("Task %d missing name", idx+1))
		} else if existing, ok := tasks[task.Name]; ok {
			mErr.Errors = append(mErr.Errors, fmt.Errorf("Task %d redefines '%s' from task %d", idx+1, task.Name, existing+1))
		} else {
			tasks[task.Name] = idx
		}
	}

	// Validate the tasks
	for _, task := range tg.Tasks {
		if err := task.Validate(tg.EphemeralDisk); err != nil {
			outer := fmt.Errorf("Task %s validation failed: %s", task.Name, err)
			mErr.Errors = append(mErr.Errors, outer)
		}
	}
	return mErr.ErrorOrNil()
}

// LookupTask finds a task by name
func (tg *TaskGroup) LookupTask(name string) *Task {
	for _, t := range tg.Tasks {
		if t.Name == name {
			return t
		}
	}
	return nil
}

func (tg *TaskGroup) GoString() string {
	return fmt.Sprintf("*%#v", *tg)
}

const (
	// DefaultKillTimeout is the default timeout between signaling a task it
	// will be killed and killing it.
	DefaultKillTimeout = 5 * time.Second
)

// LogConfig provides configuration for log rotation
type LogConfig struct {
	MaxFiles      int `mapstructure:"max_files"`
	MaxFileSizeMB int `mapstructure:"max_file_size"`
}

// DefaultLogConfig returns the default LogConfig values.
func DefaultLogConfig() *LogConfig {
	return &LogConfig{
		MaxFiles:      10,
		MaxFileSizeMB: 10,
	}
}

// Validate returns an error if the log config specified are less than
// the minimum allowed.
func (l *LogConfig) Validate() error {
	var mErr multierror.Error
	if l.MaxFiles < 1 {
		mErr.Errors = append(mErr.Errors, fmt.Errorf("minimum number of files is 1; got %d", l.MaxFiles))
	}
	if l.MaxFileSizeMB < 1 {
		mErr.Errors = append(mErr.Errors, fmt.Errorf("minimum file size is 1MB; got %d", l.MaxFileSizeMB))
	}
	return mErr.ErrorOrNil()
}

// Task is a single process typically that is executed as part of a task group.
type Task struct {
	// Name of the task
	Name string

	// Driver is used to control which driver is used
	Driver string

	// Config is provided to the driver to initialize
	Config map[string]interface{}

	// Map of environment variables to be used by the driver
	Env map[string]string

	// Resources is the resources needed by this task
	Resources *Resources

	// Meta is used to associate arbitrary metadata with this
	// task. This is opaque to Udup.
	Meta map[string]string

	// KillTimeout is the time between signaling a task that it will be
	// killed and killing it.
	KillTimeout time.Duration `mapstructure:"kill_timeout"`

	// LogConfig provides configuration for log rotation
	LogConfig *LogConfig `mapstructure:"logs"`
}

func (t *Task) Copy() *Task {
	if t == nil {
		return nil
	}
	nt := new(Task)
	*nt = *t

	nt.Resources = nt.Resources.Copy()
	nt.Meta = CopyMapStringString(nt.Meta)

	if i, err := copystructure.Copy(nt.Config); err != nil {
		nt.Config = i.(map[string]interface{})
	}

	return nt
}

// Canonicalize canonicalizes fields in the task.
func (t *Task) Canonicalize(job *Job, tg *TaskGroup) {
	// Ensure that an empty and nil map are treated the same to avoid scheduling
	// problems since we use reflect DeepEquals.
	if len(t.Meta) == 0 {
		t.Meta = nil
	}
	if len(t.Config) == 0 {
		t.Config = nil
	}

	if t.Resources != nil {
		t.Resources.Canonicalize()
	}

	// Set the default timeout if it is not specified.
	if t.KillTimeout == 0 {
		t.KillTimeout = DefaultKillTimeout
	}
}

func (t *Task) GoString() string {
	return fmt.Sprintf("*%#v", *t)
}

func (t *Task) FindHostAndPortFor(portLabel string) (string, int) {
	for _, network := range t.Resources.Networks {
		if p, ok := network.MapLabelToValues(nil)[portLabel]; ok {
			return network.IP, p
		}
	}
	return "", 0
}

// Validate is used to sanity check a task
func (t *Task) Validate(ephemeralDisk *EphemeralDisk) error {
	var mErr multierror.Error
	if t.Name == "" {
		mErr.Errors = append(mErr.Errors, errors.New("Missing task name"))
	}
	if strings.ContainsAny(t.Name, `/\`) {
		// We enforce this so that when creating the directory on disk it will
		// not have any slashes.
		mErr.Errors = append(mErr.Errors, errors.New("Task name can not include slashes"))
	}
	if t.Driver == "" {
		mErr.Errors = append(mErr.Errors, errors.New("Missing task driver"))
	}
	if t.KillTimeout.Nanoseconds() < 0 {
		mErr.Errors = append(mErr.Errors, errors.New("KillTimeout must be a positive value"))
	}

	// Validate the resources.
	if t.Resources == nil {
		mErr.Errors = append(mErr.Errors, errors.New("Missing task resources"))
	} else if err := t.Resources.MeetsMinResources(); err != nil {
		mErr.Errors = append(mErr.Errors, err)
	}

	// Ensure the task isn't asking for disk resources
	if t.Resources != nil {
		if t.Resources.DiskMB > 0 {
			mErr.Errors = append(mErr.Errors, errors.New("Task can't ask for disk resources, they have to be specified at the task group level."))
		}
	}

	// Validate the log config
	if t.LogConfig == nil {
		mErr.Errors = append(mErr.Errors, errors.New("Missing Log Config"))
	} else if err := t.LogConfig.Validate(); err != nil {
		mErr.Errors = append(mErr.Errors, err)
	}

	if t.LogConfig != nil && ephemeralDisk != nil {
		logUsage := (t.LogConfig.MaxFiles * t.LogConfig.MaxFileSizeMB)
		if ephemeralDisk.SizeMB <= logUsage {
			mErr.Errors = append(mErr.Errors,
				fmt.Errorf("log storage (%d MB) must be less than requested disk capacity (%d MB)",
					logUsage, ephemeralDisk.SizeMB))
		}
	}

	return mErr.ErrorOrNil()
}

const (
	// TemplateChangeModeNoop marks that no action should be taken if the
	// template is re-rendered
	TemplateChangeModeNoop = "noop"

	// TemplateChangeModeSignal marks that the task should be signaled if the
	// template is re-rendered
	TemplateChangeModeSignal = "signal"

	// TemplateChangeModeRestart marks that the task should be restarted if the
	// template is re-rendered
	TemplateChangeModeRestart = "restart"
)

var (
	// TemplateChangeModeInvalidError is the error for when an invalid change
	// mode is given
	TemplateChangeModeInvalidError = errors.New("Invalid change mode. Must be one of the following: noop, signal, restart")
)

// Template represents a template configuration to be rendered for a given task
type Template struct {
	// SourcePath is the the path to the template to be rendered
	SourcePath string `mapstructure:"source"`

	// DestPath is the path to where the template should be rendered
	DestPath string `mapstructure:"destination"`

	// EmbededTmpl store the raw template. This is useful for smaller templates
	// where they are embeded in the job file rather than sent as an artificat
	EmbededTmpl string `mapstructure:"data"`

	// ChangeMode indicates what should be done if the template is re-rendered
	ChangeMode string `mapstructure:"change_mode"`

	// RestartSignal is the signal that should be sent if the change mode
	// requires it.
	RestartSignal string `mapstructure:"restart_signal"`

	// Splay is used to avoid coordinated restarts of processes by applying a
	// random wait between 0 and the given splay value before signalling the
	// application of a change
	Splay time.Duration `mapstructure:"splay"`

	// Once mode is used to indicate that template should be rendered only once
	Once bool `mapstructure:"once"`
}

// DefaultTemplate returns a default template.
func DefaultTemplate() *Template {
	return &Template{
		ChangeMode: TemplateChangeModeRestart,
		Splay:      5 * time.Second,
		Once:       false,
	}
}

func (t *Template) Copy() *Template {
	if t == nil {
		return nil
	}
	copy := new(Template)
	*copy = *t
	return copy
}

func (t *Template) Validate() error {
	var mErr multierror.Error

	// Verify we have something to render
	if t.SourcePath == "" && t.EmbededTmpl == "" {
		multierror.Append(&mErr, fmt.Errorf("Must specify a source path or have an embeded template"))
	}

	// Verify we can render somewhere
	if t.DestPath == "" {
		multierror.Append(&mErr, fmt.Errorf("Must specify a destination for the template"))
	}

	// Verify the destination doesn't escape
	escaped, err := pathEscapesAllocDir(t.DestPath)
	if err != nil {
		mErr.Errors = append(mErr.Errors, fmt.Errorf("invalid destination path: %v", err))
	} else if escaped {
		mErr.Errors = append(mErr.Errors, fmt.Errorf("destination escapes allocation directory"))
	}

	// Verify a proper change mode
	switch t.ChangeMode {
	case TemplateChangeModeNoop, TemplateChangeModeRestart:
	case TemplateChangeModeSignal:
		if t.RestartSignal == "" {
			multierror.Append(&mErr, fmt.Errorf("Must specify signal value when change mode is signal"))
		}
	default:
		multierror.Append(&mErr, TemplateChangeModeInvalidError)
	}

	// Verify the splay is positive
	if t.Splay < 0 {
		multierror.Append(&mErr, fmt.Errorf("Must specify positive splay value"))
	}

	return mErr.ErrorOrNil()
}

// Set of possible states for a task.
const (
	TaskStatePending = "pending" // The task is waiting to be run.
	TaskStateRunning = "running" // The task is currently running.
	TaskStateDead    = "dead"    // Terminal state of task.
)

// TaskState tracks the current state of a task and events that caused state
// transitions.
type TaskState struct {
	// The current state of the task.
	State string

	// Failed marks a task as having failed
	Failed bool

	// Series of task events that transition the state of the task.
	Events []*TaskEvent
}

func (ts *TaskState) Copy() *TaskState {
	if ts == nil {
		return nil
	}
	copy := new(TaskState)
	copy.State = ts.State

	if ts.Events != nil {
		copy.Events = make([]*TaskEvent, len(ts.Events))
		for i, e := range ts.Events {
			copy.Events[i] = e.Copy()
		}
	}
	return copy
}

/*
// Failed returns true if the task has has failed.
func (ts *TaskState) Failed() bool {
	l := len(ts.Events)
	if ts.State != TaskStateDead || l == 0 {
		return false
	}

	switch ts.Events[l-1].Type {
	case TaskDiskExceeded, TaskNotRestarting,
		TaskFailedValidation:
		return true
	default:
		return false
	}
}*/

// Successful returns whether a task finished successfully.
func (ts *TaskState) Successful() bool {
	l := len(ts.Events)
	if ts.State != TaskStateDead || l == 0 {
		return false
	}

	e := ts.Events[l-1]
	if e.Type != TaskTerminated {
		return false
	}

	return e.ExitCode == 0
}

const (
	// TaskDriveFailure indicates that the task could not be started due to a
	// failure in the driver.
	TaskDriverFailure = "Driver Failure"

	// TaskReceived signals that the task has been pulled by the client at the
	// given timestamp.
	TaskReceived = "Received"

	// TaskFailedValidation indicates the task was invalid and as such was not
	// run.
	TaskFailedValidation = "Failed Validation"

	// TaskStarted signals that the task was started and its timestamp can be
	// used to determine the running length of the task.
	TaskStarted = "Started"

	// TaskTerminated indicates that the task was started and exited.
	TaskTerminated = "Terminated"

	// TaskKilling indicates a kill signal has been sent to the task.
	TaskKilling = "Killing"

	// TaskKilled indicates a user has killed the task.
	TaskKilled = "Killed"

	// TaskRestarting indicates that task terminated and is being restarted.
	TaskRestarting = "Restarting"

	// TaskNotRestarting indicates that the task has failed and is not being
	// restarted because it has exceeded its restart policy.
	TaskNotRestarting = "Not Restarting"

	// TaskRestartSignal indicates that the task has been signalled to be
	// restarted
	TaskRestartSignal = "Restart Signaled"

	// TaskDiskExceeded indicates that one of the tasks in a taskgroup has
	// exceeded the requested disk resources.
	TaskDiskExceeded = "Disk Resources Exceeded"

	// TaskSiblingFailed indicates that a sibling task in the task group has
	// failed.
	TaskSiblingFailed = "Sibling task failed"
)

// TaskEvent is an event that effects the state of a task and contains meta-data
// appropriate to the events type.
type TaskEvent struct {
	Type string
	Time int64 // Unix Nanosecond timestamp

	// FailsTask marks whether this event fails the task
	FailsTask bool

	// Setup Failure fields.
	SetupError string

	// Restart fields.
	RestartReason string

	// Driver Failure fields.
	DriverError string // A driver error occurred while starting the task.

	// Task Terminated Fields.
	ExitCode int    // The exit code of the task.
	Signal   int    // The signal that terminated the task.
	Message  string // A possible message explaining the termination of the task.

	// Killing fields
	KillTimeout time.Duration

	// Task Killed Fields.
	KillError string // Error killing the task.

	// KillReason is the reason the task was killed
	KillReason string

	// TaskRestarting fields.
	StartDelay int64 // The sleep period before restarting the task in unix nanoseconds.

	// Validation fields
	ValidationError string // Validation error

	// The maximum allowed task disk size.
	DiskLimit int64

	// The recorded task disk size.
	DiskSize int64

	// Name of the sibling task that caused termination of the task that
	// the TaskEvent refers to.
	FailedSibling string
}

func (te *TaskEvent) GoString() string {
	return fmt.Sprintf("%v at %v", te.Type, te.Time)
}

func (te *TaskEvent) Copy() *TaskEvent {
	if te == nil {
		return nil
	}
	copy := new(TaskEvent)
	*copy = *te
	return copy
}

func NewTaskEvent(event string) *TaskEvent {
	return &TaskEvent{
		Type: event,
		Time: time.Now().UnixNano(),
	}
}

// SetSetupError is used to store an error that occured while setting up the
// task
func (e *TaskEvent) SetSetupError(err error) *TaskEvent {
	if err != nil {
		e.SetupError = err.Error()
	}
	return e
}

func (e *TaskEvent) SetFailsTask() *TaskEvent {
	e.FailsTask = true
	return e
}

func (e *TaskEvent) SetDriverError(err error) *TaskEvent {
	if err != nil {
		e.DriverError = err.Error()
	}
	return e
}

func (e *TaskEvent) SetExitCode(c int) *TaskEvent {
	e.ExitCode = c
	return e
}

func (e *TaskEvent) SetSignal(s int) *TaskEvent {
	e.Signal = s
	return e
}

func (e *TaskEvent) SetExitMessage(err error) *TaskEvent {
	if err != nil {
		e.Message = err.Error()
	}
	return e
}

func (e *TaskEvent) SetKillError(err error) *TaskEvent {
	if err != nil {
		e.KillError = err.Error()
	}
	return e
}

func (e *TaskEvent) SetKillReason(r string) *TaskEvent {
	e.KillReason = r
	return e
}

func (e *TaskEvent) SetRestartDelay(delay time.Duration) *TaskEvent {
	e.StartDelay = int64(delay)
	return e
}

func (e *TaskEvent) SetRestartReason(reason string) *TaskEvent {
	e.RestartReason = reason
	return e
}

func (e *TaskEvent) SetValidationError(err error) *TaskEvent {
	if err != nil {
		e.ValidationError = err.Error()
	}
	return e
}

func (e *TaskEvent) SetKillTimeout(timeout time.Duration) *TaskEvent {
	e.KillTimeout = timeout
	return e
}

func (e *TaskEvent) SetDiskLimit(limit int64) *TaskEvent {
	e.DiskLimit = limit
	return e
}

func (e *TaskEvent) SetDiskSize(size int64) *TaskEvent {
	e.DiskSize = size
	return e
}

func (e *TaskEvent) SetFailedSibling(sibling string) *TaskEvent {
	e.FailedSibling = sibling
	return e
}

// TaskArtifact is an artifact to download before running the task.
type TaskArtifact struct {
	// GetterSource is the source to download an artifact using go-getter
	GetterSource string `mapstructure:"source"`

	// GetterOptions are options to use when downloading the artifact using
	// go-getter.
	GetterOptions map[string]string `mapstructure:"options"`

	// RelativeDest is the download destination given relative to the task's
	// directory.
	RelativeDest string `mapstructure:"destination"`
}

func (ta *TaskArtifact) Copy() *TaskArtifact {
	if ta == nil {
		return nil
	}
	nta := new(TaskArtifact)
	*nta = *ta
	nta.GetterOptions = CopyMapStringString(ta.GetterOptions)
	return nta
}

func (ta *TaskArtifact) GoString() string {
	return fmt.Sprintf("%+v", ta)
}

// pathEscapesAllocDir returns if the given path escapes the allocation
// directory
func pathEscapesAllocDir(path string) (bool, error) {
	// Verify the destination doesn't escape the tasks directory
	alloc, err := filepath.Abs(filepath.Join("/", "foo/", "bar/"))
	if err != nil {
		return false, err
	}
	abs, err := filepath.Abs(filepath.Join(alloc, path))
	if err != nil {
		return false, err
	}
	rel, err := filepath.Rel(alloc, abs)
	if err != nil {
		return false, err
	}

	return strings.HasPrefix(rel, ".."), nil
}

func (ta *TaskArtifact) Validate() error {
	// Verify the source
	var mErr multierror.Error
	if ta.GetterSource == "" {
		mErr.Errors = append(mErr.Errors, fmt.Errorf("source must be specified"))
	}

	escaped, err := pathEscapesAllocDir(ta.RelativeDest)
	if err != nil {
		mErr.Errors = append(mErr.Errors, fmt.Errorf("invalid destination path: %v", err))
	} else if escaped {
		mErr.Errors = append(mErr.Errors, fmt.Errorf("destination escapes task's directory"))
	}

	// Verify the checksum
	if check, ok := ta.GetterOptions["checksum"]; ok {
		check = strings.TrimSpace(check)
		if check == "" {
			mErr.Errors = append(mErr.Errors, fmt.Errorf("checksum value can not be empty"))
			return mErr.ErrorOrNil()
		}

		parts := strings.Split(check, ":")
		if l := len(parts); l != 2 {
			mErr.Errors = append(mErr.Errors, fmt.Errorf(`checksum must be given as "type:value"; got %q`, check))
			return mErr.ErrorOrNil()
		}

		checksumVal := parts[1]
		checksumBytes, err := hex.DecodeString(checksumVal)
		if err != nil {
			mErr.Errors = append(mErr.Errors, fmt.Errorf("invalid checksum: %v", err))
			return mErr.ErrorOrNil()
		}

		checksumType := parts[0]
		expectedLength := 0
		switch checksumType {
		case "md5":
			expectedLength = md5.Size
		case "sha1":
			expectedLength = sha1.Size
		case "sha256":
			expectedLength = sha256.Size
		case "sha512":
			expectedLength = sha512.Size
		default:
			mErr.Errors = append(mErr.Errors, fmt.Errorf("unsupported checksum type: %s", checksumType))
			return mErr.ErrorOrNil()
		}

		if len(checksumBytes) != expectedLength {
			mErr.Errors = append(mErr.Errors, fmt.Errorf("invalid %s checksum: %v", checksumType, checksumVal))
			return mErr.ErrorOrNil()
		}
	}

	return mErr.ErrorOrNil()
}

// EphemeralDisk is an ephemeral disk object
type EphemeralDisk struct {
	// Sticky indicates whether the allocation is sticky to a node
	Sticky bool

	// SizeMB is the size of the local disk
	SizeMB int `mapstructure:"size"`
}

// DefaultEphemeralDisk returns a EphemeralDisk with default configurations
func DefaultEphemeralDisk() *EphemeralDisk {
	return &EphemeralDisk{
		SizeMB: 300,
	}
}

// Validate validates EphemeralDisk
func (d *EphemeralDisk) Validate() error {
	if d.SizeMB < 10 {
		return fmt.Errorf("minimum DiskMB value is 10; got %d", d.SizeMB)
	}
	return nil
}

// Copy copies the EphemeralDisk struct and returns a new one
func (d *EphemeralDisk) Copy() *EphemeralDisk {
	ld := new(EphemeralDisk)
	*ld = *d
	return ld
}

const (
	AllocDesiredStatusRun   = "run"   // Allocation should run
	AllocDesiredStatusStop  = "stop"  // Allocation should stop
	AllocDesiredStatusEvict = "evict" // Allocation should stop, and was evicted
)

const (
	AllocClientStatusPending  = "pending"
	AllocClientStatusRunning  = "running"
	AllocClientStatusComplete = "complete"
	AllocClientStatusFailed   = "failed"
	AllocClientStatusLost     = "lost"
)

// Allocation is used to allocate the placement of a task group to a node.
type Allocation struct {
	// ID of the allocation (UUID)
	ID string

	// ID of the evaluation that generated this allocation
	EvalID string

	// Name is a logical name of the allocation.
	Name string

	// NodeID is the node this is being placed on
	NodeID string

	// Job is the parent job of the task group being allocated.
	// This is copied at allocation time to avoid issues if the job
	// definition is updated.
	JobID string
	Job   *Job

	// TaskGroup is the name of the task group that should be run
	TaskGroup string

	// Resources is the total set of resources allocated as part
	// of this allocation of the task group.
	Resources *Resources

	// SharedResources are the resources that are shared by all the tasks in an
	// allocation
	SharedResources *Resources

	// TaskResources is the set of resources allocated to each
	// task. These should sum to the total Resources.
	TaskResources map[string]*Resources

	// Metrics associated with this allocation
	Metrics *AllocMetric

	// Desired Status of the allocation on the client
	DesiredStatus string

	// DesiredStatusDescription is meant to provide more human useful information
	DesiredDescription string

	// Status of the allocation on the client
	ClientStatus string

	// ClientStatusDescription is meant to provide more human useful information
	ClientDescription string

	// TaskStates stores the state of each task,
	TaskStates map[string]*TaskState

	// PreviousAllocation is the allocation that this allocation is replacing
	PreviousAllocation string

	// Raft Indexes
	CreateIndex uint64
	ModifyIndex uint64

	// AllocModifyIndex is not updated when the client updates allocations. This
	// lets the client pull only the allocs updated by the server.
	AllocModifyIndex uint64

	// CreateTime is the time the allocation has finished scheduling and been
	// verified by the plan applier.
	CreateTime int64
}

func (a *Allocation) Copy() *Allocation {
	if a == nil {
		return nil
	}
	na := new(Allocation)
	*na = *a

	na.Job = na.Job.Copy()
	na.Resources = na.Resources.Copy()
	na.SharedResources = na.SharedResources.Copy()

	if a.TaskResources != nil {
		tr := make(map[string]*Resources, len(na.TaskResources))
		for task, resource := range na.TaskResources {
			tr[task] = resource.Copy()
		}
		na.TaskResources = tr
	}

	na.Metrics = na.Metrics.Copy()

	if a.TaskStates != nil {
		ts := make(map[string]*TaskState, len(na.TaskStates))
		for task, state := range na.TaskStates {
			ts[task] = state.Copy()
		}
		na.TaskStates = ts
	}
	return na
}

// TerminalStatus returns if the desired or actual status is terminal and
// will no longer transition.
func (a *Allocation) TerminalStatus() bool {
	// First check the desired state and if that isn't terminal, check client
	// state.
	switch a.DesiredStatus {
	case AllocDesiredStatusStop, AllocDesiredStatusEvict:
		return true
	default:
	}

	switch a.ClientStatus {
	case AllocClientStatusComplete, AllocClientStatusFailed, AllocClientStatusLost:
		return true
	default:
		return false
	}
}

// Terminated returns if the allocation is in a terminal state on a client.
func (a *Allocation) Terminated() bool {
	if a.ClientStatus == AllocClientStatusFailed ||
		a.ClientStatus == AllocClientStatusComplete ||
		a.ClientStatus == AllocClientStatusLost {
		return true
	}
	return false
}

// RanSuccessfully returns whether the client has ran the allocation and all
// tasks finished successfully
func (a *Allocation) RanSuccessfully() bool {
	// Handle the case the client hasn't started the allocation.
	if len(a.TaskStates) == 0 {
		return false
	}

	// Check to see if all the tasks finised successfully in the allocation
	allSuccess := true
	for _, state := range a.TaskStates {
		allSuccess = allSuccess && state.Successful()
	}

	return allSuccess
}

// Stub returns a list stub for the allocation
func (a *Allocation) Stub() *AllocListStub {
	return &AllocListStub{
		ID:                 a.ID,
		EvalID:             a.EvalID,
		Name:               a.Name,
		NodeID:             a.NodeID,
		JobID:              a.JobID,
		TaskGroup:          a.TaskGroup,
		DesiredStatus:      a.DesiredStatus,
		DesiredDescription: a.DesiredDescription,
		ClientStatus:       a.ClientStatus,
		ClientDescription:  a.ClientDescription,
		TaskStates:         a.TaskStates,
		CreateIndex:        a.CreateIndex,
		ModifyIndex:        a.ModifyIndex,
		CreateTime:         a.CreateTime,
	}
}

var (
	// AllocationIndexRegex is a regular expression to find the allocation index.
	AllocationIndexRegex = regexp.MustCompile(".+\\[(\\d+)\\]$")
)

// Index returns the index of the allocation. If the allocation is from a task
// group with count greater than 1, there will be multiple allocations for it.
func (a *Allocation) Index() int {
	matches := AllocationIndexRegex.FindStringSubmatch(a.Name)
	if len(matches) != 2 {
		return -1
	}

	index, err := strconv.Atoi(matches[1])
	if err != nil {
		return -1
	}

	return index
}

// AllocListStub is used to return a subset of alloc information
type AllocListStub struct {
	ID                 string
	EvalID             string
	Name               string
	NodeID             string
	JobID              string
	TaskGroup          string
	DesiredStatus      string
	DesiredDescription string
	ClientStatus       string
	ClientDescription  string
	TaskStates         map[string]*TaskState
	CreateIndex        uint64
	ModifyIndex        uint64
	CreateTime         int64
}

// AllocMetric is used to track various metrics while attempting
// to make an allocation. These are used to debug a job, or to better
// understand the pressure within the system.
type AllocMetric struct {
	// NodesEvaluated is the number of nodes that were evaluated
	NodesEvaluated int

	// NodesFiltered is the number of nodes filtered due to a constraint
	NodesFiltered int

	// NodesAvailable is the number of nodes available for evaluation per DC.
	NodesAvailable map[string]int

	// ClassFiltered is the number of nodes filtered by class
	ClassFiltered map[string]int

	// ConstraintFiltered is the number of failures caused by constraint
	ConstraintFiltered map[string]int

	// NodesExhausted is the number of nodes skipped due to being
	// exhausted of at least one resource
	NodesExhausted int

	// ClassExhausted is the number of nodes exhausted by class
	ClassExhausted map[string]int

	// DimensionExhausted provides the count by dimension or reason
	DimensionExhausted map[string]int

	// Scores is the scores of the final few nodes remaining
	// for placement. The top score is typically selected.
	Scores map[string]float64

	// AllocationTime is a measure of how long the allocation
	// attempt took. This can affect performance and SLAs.
	AllocationTime time.Duration

	// CoalescedFailures indicates the number of other
	// allocations that were coalesced into this failed allocation.
	// This is to prevent creating many failed allocations for a
	// single task group.
	CoalescedFailures int
}

func (a *AllocMetric) Copy() *AllocMetric {
	if a == nil {
		return nil
	}
	na := new(AllocMetric)
	*na = *a
	na.NodesAvailable = CopyMapStringInt(na.NodesAvailable)
	na.ClassFiltered = CopyMapStringInt(na.ClassFiltered)
	na.ClassExhausted = CopyMapStringInt(na.ClassExhausted)
	na.DimensionExhausted = CopyMapStringInt(na.DimensionExhausted)
	na.Scores = CopyMapStringFloat64(na.Scores)
	return na
}

func (a *AllocMetric) EvaluateNode() {
	a.NodesEvaluated += 1
}

func (a *AllocMetric) FilterNode(node *Node) {
	a.NodesFiltered += 1
	if node != nil && node.NodeClass != "" {
		if a.ClassFiltered == nil {
			a.ClassFiltered = make(map[string]int)
		}
		a.ClassFiltered[node.NodeClass] += 1
	}
}

func (a *AllocMetric) ExhaustedNode(node *Node, dimension string) {
	a.NodesExhausted += 1
	if node != nil && node.NodeClass != "" {
		if a.ClassExhausted == nil {
			a.ClassExhausted = make(map[string]int)
		}
		a.ClassExhausted[node.NodeClass] += 1
	}
	if dimension != "" {
		if a.DimensionExhausted == nil {
			a.DimensionExhausted = make(map[string]int)
		}
		a.DimensionExhausted[dimension] += 1
	}
}

func (a *AllocMetric) ScoreNode(node *Node, name string, score float64) {
	if a.Scores == nil {
		a.Scores = make(map[string]float64)
	}
	key := fmt.Sprintf("%s.%s", node.ID, name)
	a.Scores[key] = score
}

const (
	EvalStatusBlocked   = "blocked"
	EvalStatusPending   = "pending"
	EvalStatusComplete  = "complete"
	EvalStatusFailed    = "failed"
	EvalStatusCancelled = "canceled"
)

const (
	EvalTriggerJobRegister   = "job-register"
	EvalTriggerJobDeregister = "job-deregister"
	EvalTriggerPeriodicJob   = "periodic-job"
	EvalTriggerNodeUpdate    = "node-update"
	EvalTriggerScheduled     = "scheduled"
	EvalTriggerRollingUpdate = "rolling-update"
	EvalTriggerMaxPlans      = "max-plan-attempts"
)

const (
	// CoreJobEvalGC is used for the garbage collection of evaluations
	// and allocations. We periodically scan evaluations in a terminal state,
	// in which all the corresponding allocations are also terminal. We
	// delete these out of the system to bound the state.
	CoreJobEvalGC = "eval-gc"

	// CoreJobNodeGC is used for the garbage collection of failed nodes.
	// We periodically scan nodes in a terminal state, and if they have no
	// corresponding allocations we delete these out of the system.
	CoreJobNodeGC = "node-gc"

	// CoreJobJobGC is used for the garbage collection of eligible jobs. We
	// periodically scan garbage collectible jobs and check if both their
	// evaluations and allocations are terminal. If so, we delete these out of
	// the system.
	CoreJobJobGC = "job-gc"

	// CoreJobForceGC is used to force garbage collection of all GCable objects.
	CoreJobForceGC = "force-gc"
)

// Evaluation is used anytime we need to apply business logic as a result
// of a change to our desired state (job specification) or the emergent state
// (registered nodes). When the inputs change, we need to "evaluate" them,
// potentially taking action (allocation of work) or doing nothing if the state
// of the world does not require it.
type Evaluation struct {
	// ID is a randonly generated UUID used for this evaluation. This
	// is assigned upon the creation of the evaluation.
	ID string

	// Priority is used to control scheduling importance and if this job
	// can preempt other jobs.
	Priority int

	// Type is used to control which schedulers are available to handle
	// this evaluation.
	Type string

	// TriggeredBy is used to give some insight into why this Eval
	// was created. (Job change, node failure, alloc failure, etc).
	TriggeredBy string

	// JobID is the job this evaluation is scoped to. Evaluations cannot
	// be run in parallel for a given JobID, so we serialize on this.
	JobID string

	// JobModifyIndex is the modify index of the job at the time
	// the evaluation was created
	JobModifyIndex uint64

	// NodeID is the node that was affected triggering the evaluation.
	NodeID string

	// NodeModifyIndex is the modify index of the node at the time
	// the evaluation was created
	NodeModifyIndex uint64

	// Status of the evaluation
	Status string

	// StatusDescription is meant to provide more human useful information
	StatusDescription string

	// Wait is a minimum wait time for running the eval. This is used to
	// support a rolling upgrade.
	Wait time.Duration

	// NextEval is the evaluation ID for the eval created to do a followup.
	// This is used to support rolling upgrades, where we need a chain of evaluations.
	NextEval string

	// PreviousEval is the evaluation ID for the eval creating this one to do a followup.
	// This is used to support rolling upgrades, where we need a chain of evaluations.
	PreviousEval string

	// BlockedEval is the evaluation ID for a created blocked eval. A
	// blocked eval will be created if all allocations could not be placed due
	// to constraints or lacking resources.
	BlockedEval string

	// FailedTGAllocs are task groups which have allocations that could not be
	// made, but the metrics are persisted so that the user can use the feedback
	// to determine the cause.
	FailedTGAllocs map[string]*AllocMetric

	// ClassEligibility tracks computed node classes that have been explicitly
	// marked as eligible or ineligible.
	ClassEligibility map[string]bool

	// EscapedComputedClass marks whether the job has constraints that are not
	// captured by computed node classes.
	EscapedComputedClass bool

	// AnnotatePlan triggers the scheduler to provide additional annotations
	// during the evaluation. This should not be set during normal operations.
	AnnotatePlan bool

	// SnapshotIndex is the Raft index of the snapshot used to process the
	// evaluation. As such it will only be set once it has gone through the
	// scheduler.
	SnapshotIndex uint64

	// QueuedAllocations is the number of unplaced allocations at the time the
	// evaluation was processed. The map is keyed by Task Group names.
	QueuedAllocations map[string]int

	// Raft Indexes
	CreateIndex uint64
	ModifyIndex uint64
}

// TerminalStatus returns if the current status is terminal and
// will no longer transition.
func (e *Evaluation) TerminalStatus() bool {
	switch e.Status {
	case EvalStatusComplete, EvalStatusFailed, EvalStatusCancelled:
		return true
	default:
		return false
	}
}

func (e *Evaluation) GoString() string {
	return fmt.Sprintf("<Eval '%s' JobID: '%s'>", e.ID, e.JobID)
}

func (e *Evaluation) Copy() *Evaluation {
	if e == nil {
		return nil
	}
	ne := new(Evaluation)
	*ne = *e

	// Copy ClassEligibility
	if e.ClassEligibility != nil {
		classes := make(map[string]bool, len(e.ClassEligibility))
		for class, elig := range e.ClassEligibility {
			classes[class] = elig
		}
		ne.ClassEligibility = classes
	}

	// Copy FailedTGAllocs
	if e.FailedTGAllocs != nil {
		failedTGs := make(map[string]*AllocMetric, len(e.FailedTGAllocs))
		for tg, metric := range e.FailedTGAllocs {
			failedTGs[tg] = metric.Copy()
		}
		ne.FailedTGAllocs = failedTGs
	}

	// Copy queued allocations
	if e.QueuedAllocations != nil {
		queuedAllocations := make(map[string]int, len(e.QueuedAllocations))
		for tg, num := range e.QueuedAllocations {
			queuedAllocations[tg] = num
		}
		ne.QueuedAllocations = queuedAllocations
	}

	return ne
}

// ShouldEnqueue checks if a given evaluation should be enqueued into the
// eval_broker
func (e *Evaluation) ShouldEnqueue() bool {
	switch e.Status {
	case EvalStatusPending:
		return true
	case EvalStatusComplete, EvalStatusFailed, EvalStatusBlocked, EvalStatusCancelled:
		return false
	default:
		panic(fmt.Sprintf("unhandled evaluation (%s) status %s", e.ID, e.Status))
	}
}

// ShouldBlock checks if a given evaluation should be entered into the blocked
// eval tracker.
func (e *Evaluation) ShouldBlock() bool {
	switch e.Status {
	case EvalStatusBlocked:
		return true
	case EvalStatusComplete, EvalStatusFailed, EvalStatusPending, EvalStatusCancelled:
		return false
	default:
		panic(fmt.Sprintf("unhandled evaluation (%s) status %s", e.ID, e.Status))
	}
}

// MakePlan is used to make a plan from the given evaluation
// for a given Job
func (e *Evaluation) MakePlan(j *Job) *Plan {
	p := &Plan{
		EvalID:         e.ID,
		Priority:       e.Priority,
		Job:            j,
		NodeUpdate:     make(map[string][]*Allocation),
		NodeAllocation: make(map[string][]*Allocation),
	}
	if j != nil {
		p.AllAtOnce = j.AllAtOnce
	}

	return p
}

// NextRollingEval creates an evaluation to followup this eval for rolling updates
func (e *Evaluation) NextRollingEval(wait time.Duration) *Evaluation {
	return &Evaluation{
		ID:             GenerateUUID(),
		Priority:       e.Priority,
		Type:           e.Type,
		TriggeredBy:    EvalTriggerRollingUpdate,
		JobID:          e.JobID,
		JobModifyIndex: e.JobModifyIndex,
		Status:         EvalStatusPending,
		Wait:           wait,
		PreviousEval:   e.ID,
	}
}

// CreateBlockedEval creates a blocked evaluation to followup this eval to place any
// failed allocations. It takes the classes marked explicitly eligible or
// ineligible and whether the job has escaped computed node classes.
func (e *Evaluation) CreateBlockedEval(classEligibility map[string]bool, escaped bool) *Evaluation {
	return &Evaluation{
		ID:                   GenerateUUID(),
		Priority:             e.Priority,
		Type:                 e.Type,
		TriggeredBy:          e.TriggeredBy,
		JobID:                e.JobID,
		JobModifyIndex:       e.JobModifyIndex,
		Status:               EvalStatusBlocked,
		PreviousEval:         e.ID,
		ClassEligibility:     classEligibility,
		EscapedComputedClass: escaped,
	}
}

// Plan is used to submit a commit plan for task allocations. These
// are submitted to the leader which verifies that resources have
// not been overcommitted before admiting the plan.
type Plan struct {
	// EvalID is the evaluation ID this plan is associated with
	EvalID string

	// EvalToken is used to prevent a split-brain processing of
	// an evaluation. There should only be a single scheduler running
	// an Eval at a time, but this could be violated after a leadership
	// transition. This unique token is used to reject plans that are
	// being submitted from a different leader.
	EvalToken string

	// Priority is the priority of the upstream job
	Priority int

	// AllAtOnce is used to control if incremental scheduling of task groups
	// is allowed or if we must do a gang scheduling of the entire job.
	// If this is false, a plan may be partially applied. Otherwise, the
	// entire plan must be able to make progress.
	AllAtOnce bool

	// Job is the parent job of all the allocations in the Plan.
	// Since a Plan only involves a single Job, we can reduce the size
	// of the plan by only including it once.
	Job *Job

	// NodeUpdate contains all the allocations for each node. For each node,
	// this is a list of the allocations to update to either stop or evict.
	NodeUpdate map[string][]*Allocation

	// NodeAllocation contains all the allocations for each node.
	// The evicts must be considered prior to the allocations.
	NodeAllocation map[string][]*Allocation

	// Annotations contains annotations by the scheduler to be used by operators
	// to understand the decisions made by the scheduler.
	Annotations *PlanAnnotations
}

// AppendUpdate marks the allocation for eviction. The clientStatus of the
// allocation may be optionally set by passing in a non-empty value.
func (p *Plan) AppendUpdate(alloc *Allocation, desiredStatus, desiredDesc, clientStatus string) {
	newAlloc := new(Allocation)
	*newAlloc = *alloc

	// If the job is not set in the plan we are deregistering a job so we
	// extract the job from the allocation.
	if p.Job == nil && newAlloc.Job != nil {
		p.Job = newAlloc.Job
	}

	// Normalize the job
	newAlloc.Job = nil

	// Strip the resources as it can be rebuilt.
	newAlloc.Resources = nil

	newAlloc.DesiredStatus = desiredStatus
	newAlloc.DesiredDescription = desiredDesc

	if clientStatus != "" {
		newAlloc.ClientStatus = clientStatus
	}

	node := alloc.NodeID
	existing := p.NodeUpdate[node]
	p.NodeUpdate[node] = append(existing, newAlloc)
}

func (p *Plan) PopUpdate(alloc *Allocation) {
	existing := p.NodeUpdate[alloc.NodeID]
	n := len(existing)
	if n > 0 && existing[n-1].ID == alloc.ID {
		existing = existing[:n-1]
		if len(existing) > 0 {
			p.NodeUpdate[alloc.NodeID] = existing
		} else {
			delete(p.NodeUpdate, alloc.NodeID)
		}
	}
}

func (p *Plan) AppendAlloc(alloc *Allocation) {
	node := alloc.NodeID
	existing := p.NodeAllocation[node]
	p.NodeAllocation[node] = append(existing, alloc)
}

// IsNoOp checks if this plan would do nothing
func (p *Plan) IsNoOp() bool {
	return len(p.NodeUpdate) == 0 && len(p.NodeAllocation) == 0
}

// PlanResult is the result of a plan submitted to the leader.
type PlanResult struct {
	// NodeUpdate contains all the updates that were committed.
	NodeUpdate map[string][]*Allocation

	// NodeAllocation contains all the allocations that were committed.
	NodeAllocation map[string][]*Allocation

	// RefreshIndex is the index the worker should refresh state up to.
	// This allows all evictions and allocations to be materialized.
	// If any allocations were rejected due to stale data (node state,
	// over committed) this can be used to force a worker refresh.
	RefreshIndex uint64

	// AllocIndex is the Raft index in which the evictions and
	// allocations took place. This is used for the write index.
	AllocIndex uint64
}

// IsNoOp checks if this plan result would do nothing
func (p *PlanResult) IsNoOp() bool {
	return len(p.NodeUpdate) == 0 && len(p.NodeAllocation) == 0
}

// FullCommit is used to check if all the allocations in a plan
// were committed as part of the result. Returns if there was
// a match, and the number of expected and actual allocations.
func (p *PlanResult) FullCommit(plan *Plan) (bool, int, int) {
	expected := 0
	actual := 0
	for name, allocList := range plan.NodeAllocation {
		didAlloc, _ := p.NodeAllocation[name]
		expected += len(allocList)
		actual += len(didAlloc)
	}
	return actual == expected, expected, actual
}

// PlanAnnotations holds annotations made by the scheduler to give further debug
// information to operators.
type PlanAnnotations struct {
	// DesiredTGUpdates is the set of desired updates per task group.
	DesiredTGUpdates map[string]*DesiredUpdates
}

// DesiredUpdates is the set of changes the scheduler would like to make given
// sufficient resources and cluster capacity.
type DesiredUpdates struct {
	Ignore            uint64
	Place             uint64
	Migrate           uint64
	Stop              uint64
	InPlaceUpdate     uint64
	DestructiveUpdate uint64
}

// msgpackHandle is a shared handle for encoding/decoding of structs
var MsgpackHandle = func() *codec.MsgpackHandle {
	h := &codec.MsgpackHandle{RawToString: true}

	// Sets the default type for decoding a map into a nil interface{}.
	// This is necessary in particular because we store the driver configs as a
	// nil interface{}.
	h.MapType = reflect.TypeOf(map[string]interface{}(nil))
	return h
}()

var HashiMsgpackHandle = func() *hcodec.MsgpackHandle {
	h := &hcodec.MsgpackHandle{RawToString: true}

	// Sets the default type for decoding a map into a nil interface{}.
	// This is necessary in particular because we store the driver configs as a
	// nil interface{}.
	h.MapType = reflect.TypeOf(map[string]interface{}(nil))
	return h
}()

// Decode is used to decode a MsgPack encoded object
func Decode(buf []byte, out interface{}) error {
	return codec.NewDecoder(bytes.NewReader(buf), MsgpackHandle).Decode(out)
}

// Encode is used to encode a MsgPack object with type prefix
func Encode(t MessageType, msg interface{}) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(uint8(t))
	err := codec.NewEncoder(&buf, MsgpackHandle).Encode(msg)
	return buf.Bytes(), err
}

// RecoverableError wraps an error and marks whether it is recoverable and could
// be retried or it is fatal.
type RecoverableError struct {
	Err         string
	Recoverable bool
}

// NewRecoverableError is used to wrap an error and mark it as recoverable or
// not.
func NewRecoverableError(e error, recoverable bool) *RecoverableError {
	if e == nil {
		return nil
	}

	return &RecoverableError{
		Err:         e.Error(),
		Recoverable: recoverable,
	}
}

func (r *RecoverableError) Error() string {
	return r.Err
}
