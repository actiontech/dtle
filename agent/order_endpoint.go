package agent

import (
	"net/http"
	"strings"

	"udup/api"
	"udup/internal/models"
)

func (s *HTTPServer) LoginRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	switch req.Method {
	case "PUT":
		return s.login(resp, req)
	default:
		return nil, CodedError(405, ErrInvalidMethod)
	}
}

func (s *HTTPServer) OrdersRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	switch req.Method {
	case "GET":
		return s.orderListRequest(resp, req)
	case "PUT", "POST":
		return s.orderUpdate(resp, req, "")
	default:
		return nil, CodedError(405, ErrInvalidMethod)
	}
}

func (s *HTTPServer) PendingOrdersRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	switch req.Method {
	case "GET":
		return s.orderListPendingRequest(resp, req)
	default:
		return nil, CodedError(405, ErrInvalidMethod)
	}
}

func (s *HTTPServer) orderListRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	args := models.OrderListRequest{}
	if args.Region == "" {
		args.Region = s.agent.config.Region
	}
	if s.parse(resp, req, &args.Region, &args.QueryOptions) {
		return nil, nil
	}

	var out models.OrderListResponse
	if err := s.agent.RPC("Order.List", &args, &out); err != nil {
		return nil, err
	}

	setMeta(resp, &out.QueryMeta)
	if out.Orders == nil {
		out.Orders = make([]*models.Order, 0)
	}
	return out.Orders, nil
}

func (s *HTTPServer) orderListPendingRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	args := models.OrderListRequest{}
	if args.Region == "" {
		args.Region = s.agent.config.Region
	}
	if s.parse(resp, req, &args.Region, &args.QueryOptions) {
		return nil, nil
	}

	var out models.OrderListResponse
	if err := s.agent.RPC("Order.ListPending", &args, &out); err != nil {
		return nil, err
	}

	setMeta(resp, &out.QueryMeta)
	if out.Orders == nil {
		out.Orders = make([]*models.Order, 0)
	}
	return out.Orders, nil
}

func (s *HTTPServer) OrderSpecificRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	path := strings.TrimPrefix(req.URL.Path, "/v1/order/")
	return s.orderCRUD(resp, req, path)
}

func (s *HTTPServer) orderCRUD(resp http.ResponseWriter, req *http.Request,
	orderName string) (interface{}, error) {
	switch req.Method {
	case "GET":
		return s.orderQuery(resp, req, orderName)
	case "PUT", "POST":
		return s.orderUpdate(resp, req, orderName)
	case "DELETE":
		return s.orderDelete(resp, req, orderName)
	default:
		return nil, CodedError(405, ErrInvalidMethod)
	}
}

func (s *HTTPServer) login(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	var order *api.Order
	if err := decodeBody(req, &order); err != nil {
		return nil, CodedError(400, err.Error())
	}

	if order.ID == nil {
		return nil, CodedError(400, "Order Id hasn't been provided")
	}
	if order.Region == nil {
		order.Region = &s.agent.config.Region
	}
	args := models.OrderSpecificRequest{
		OrderID: *order.ID,
	}
	if s.parse(resp, req, &args.Region, &args.QueryOptions) {
		return nil, nil
	}

	var out models.SingleOrderResponse
	if err := s.agent.RPC("Order.GetOrder", &args, &out); err != nil {
		return nil, err
	}

	setMeta(resp, &out.QueryMeta)
	if out.Order == nil && args.OrderID != "udup" {
		return nil, CodedError(404, "order not found")
	}
	return out, nil
}

func (s *HTTPServer) orderQuery(resp http.ResponseWriter, req *http.Request,
	orderID string) (interface{}, error) {
	if req.Method != "GET" {
		return nil, CodedError(405, ErrInvalidMethod)
	}
	args := models.OrderSpecificRequest{
		OrderID: orderID,
	}
	if s.parse(resp, req, &args.Region, &args.QueryOptions) {
		return nil, nil
	}

	var out models.SingleOrderResponse
	if err := s.agent.RPC("Order.GetOrder", &args, &out); err != nil {
		return nil, err
	}

	setMeta(resp, &out.QueryMeta)
	if out.Order == nil {
		return nil, CodedError(404, "order not found")
	}
	return out.Order, nil
}

func (s *HTTPServer) orderUpdate(resp http.ResponseWriter, req *http.Request,
	orderId string) (interface{}, error) {
	var args *api.Order
	if err := decodeBody(req, &args); err != nil {
		return nil, CodedError(400, err.Error())
	}

	if args.Name == nil {
		return nil, CodedError(400, "Order Name hasn't been provided")
	}
	if args.Region == nil {
		args.Region = &s.agent.config.Region
	}
	s.parseRegion(req, args.Region)

	sOrder := ApiOrderToStructOrder(args)

	regReq := models.OrderRegisterRequest{
		Order:            sOrder,
		EnforceIndex:     args.EnforceIndex,
		OrderModifyIndex: *args.OrderModifyIndex,
		WriteRequest: models.WriteRequest{
			Region: *args.Region,
		},
	}
	var out models.OrderResponse

	if err := s.agent.RPC("Order.Register", &regReq, &out); err != nil {
		return nil, err
	}
	setIndex(resp, out.Index)
	return out, nil
}

func (s *HTTPServer) orderDelete(resp http.ResponseWriter, req *http.Request,
	orderName string) (interface{}, error) {
	args := models.OrderDeregisterRequest{
		OrderID: orderName,
	}
	s.parseRegion(req, &args.Region)

	var out models.OrderResponse
	if err := s.agent.RPC("Order.Deregister", &args, &out); err != nil {
		return nil, err
	}
	setIndex(resp, out.Index)
	return out, nil
}

func ApiOrderToStructOrder(order *api.Order) *models.Order {
	order.Canonicalize()

	o := &models.Order{
		Region:                *order.Region,
		ID:                    *order.ID,
		Name:                  *order.Name,
		TrafficAgainstLimits:  *order.TrafficAgainstLimits,
		TotalTransferredBytes: *order.TotalTransferredBytes,
		Status:                *order.Status,
		CreateIndex:           *order.CreateIndex,
		ModifyIndex:           *order.ModifyIndex,
		OrderModifyIndex:      *order.OrderModifyIndex,
	}

	return o
}
