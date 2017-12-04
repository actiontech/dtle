package agent

import (
	"net/http"
	"strings"

	"udup/api"
	"udup/internal/models"
)

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

func (s *HTTPServer) OrderSpecificRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	path := strings.TrimPrefix(req.URL.Path, "/v1/order/")
	return s.orderCRUD(resp, req, path)
}

func (s *HTTPServer) orderCRUD(resp http.ResponseWriter, req *http.Request,
	orderName string) (interface{}, error) {
	switch req.Method {
	case "PUT", "POST":
		return s.orderUpdate(resp, req, orderName)
	case "DELETE":
		return s.orderDelete(resp, req, orderName)
	default:
		return nil, CodedError(405, ErrInvalidMethod)
	}
}

func (s *HTTPServer) orderUpdate(resp http.ResponseWriter, req *http.Request,
	orderName string) (interface{}, error) {
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
		Region:           *order.Region,
		ID:               *order.ID,
		Name:             *order.Name,
		NetworkTraffic:   *order.NetworkTraffic,
		CreateIndex:      *order.CreateIndex,
		ModifyIndex:      *order.ModifyIndex,
		OrderModifyIndex: *order.OrderModifyIndex,
	}

	return o
}
