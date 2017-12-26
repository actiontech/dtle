package models

const (
	OrderStatusExpired = "expired"
	OrderStatusPending = "pending"
	OrderStatusDone    = "done"
	OrderStatusRunning = "running"
)

// Order is the scope of a scheduling request to Udup. It is the largest
// scoped object, and is a named collection of tasks. Each task
// is further composed of tasks. A task (T) is the unit of scheduling
// however.
type Order struct {
	// Region is the Udup region that handles scheduling this Order
	Region string

	JobID string

	// ID is a unique identifier for the Order per region. It can be
	// specified hierarchically like LineOfBiz/OrgName/Team/Project
	ID string

	SkuId string

	TrafficAgainstLimits uint64

	TotalTransferredBytes uint64

	// Order status
	Status string

	EnforceIndex bool

	// Raft Indexes
	CreateIndex      uint64
	ModifyIndex      uint64
	OrderModifyIndex uint64
}

// Copy returns a deep copy of the Order. It is expected that callers use recover.
// This Order can panic if the deep copy failed as it uses reflection.
func (j *Order) Copy() *Order {
	if j == nil {
		return nil
	}
	nj := new(Order)
	*nj = *j

	return nj
}

type OrderResponse struct {
	Success bool
	QueryMeta
}

// SingleOrderResponse is used to return a single Order
type SingleOrderResponse struct {
	Order *Order
	QueryMeta
}

// OrderListResponse is used for a list request
type OrderListResponse struct {
	Orders []*Order
	QueryMeta
}

// OrderRegisterRequest is used for Order.Register endpoint
// to register a Order as being a schedulable entity.
type OrderRegisterRequest struct {
	Order *Order

	// If EnforceIndex is set then the Order will only be registered if the passed
	// OrderModifyIndex matches the current Orders index. If the index is zero, the
	// register only occurs if the Order is new.
	EnforceIndex     bool
	OrderModifyIndex uint64

	WriteRequest
}

type OrderRenewalRequest struct {
	Order *Order

	// If EnforceIndex is set then the Order will only be registered if the passed
	// OrderModifyIndex matches the current Orders index. If the index is zero, the
	// register only occurs if the Order is new.
	EnforceIndex     bool
	OrderModifyIndex uint64

	WriteRequest
}

// OrderDeregisterRequest is used for Order.Deregister endpoint
// to deregister a Order as being a schedulable entity.
type OrderDeregisterRequest struct {
	OrderID string
	WriteRequest
}

// OrderSpecificRequest is used when we just need to specify a target Order
type OrderSpecificRequest struct {
	OrderID   string
	AllAllocs bool
	QueryOptions
}

// OrderListRequest is used to parameterize a list request
type OrderListRequest struct {
	QueryOptions
}

// OrderSummaryRequest is used when we just need to get a specific Order summary
type OrderSummaryRequest struct {
	Orders []string
	QueryOptions
}
