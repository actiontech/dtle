/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package agent

import (
	"crypto/md5"
	"encoding/hex"
	"net/http"
	"net/url"
	"sort"
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

func (s *HTTPServer) OrderCloudRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	switch req.Method {
	case "GET":
		return s.orderCloudRegister(resp, req)
	default:
		return nil, CodedError(405, ErrInvalidMethod)
	}
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

	if order.ID == "" {
		return nil, CodedError(400, "Order Id hasn't been provided")
	}
	if order.Region == nil {
		order.Region = &s.agent.config.Region
	}
	args := models.OrderSpecificRequest{
		OrderID: order.ID,
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

func (s *HTTPServer) orderCloudRegister(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	if req.Method != "GET" {
		return nil, CodedError(405, ErrInvalidMethod)
	}
	queryValues := req.URL.Query()
	if queryValues.Get("token") == "" {
		return nil, CodedError(400, "Token hasn't been provided")
	}
	m_sign := map[string]string{}
	//将所有参与sign验证参数保存到map(token不参与签名计算)
	for k, v := range queryValues {
		//排除键为空或值为空
		if k != "" && k != "token" && v[0] != "" {
			m_sign[k] = v[0]
		}
	}
	//按字母序排序并拼接成键值对形式
	sPara := filterParams(m_sign)
	//获得签名结果
	mySign := createSign(sPara, "11111")
	s.logger.Printf("mySign:%v", mySign)
	if mySign != queryValues.Get("token") {
		return nil, CodedError(400, "Invalid security token")
	}

	args := &api.Order{
		Region: &s.agent.config.Region,
	}
	if queryValues.Get("skuId") == "" {
		return nil, CodedError(400, "SkuId hasn't been provided")
	}
	s.parseRegion(req, args.Region)

	args.ID = queryValues.Get("orderBizId")
	args.SkuId = queryValues.Get("skuId")

	sOrder := ApiOrderToStructOrder(args)

	regReq := models.OrderRegisterRequest{
		Order:            sOrder,
		EnforceIndex:     args.EnforceIndex,
		OrderModifyIndex: *args.OrderModifyIndex,
		WriteRequest: models.WriteRequest{
			Region: *args.Region,
		},
	}
	var rsp models.OrderResponse
	var out CloudOrderResponse

	if err := s.agent.RPC("Order.Register", &regReq, &rsp); err != nil {
		return nil, err
	}

	if rsp.Success {
		out = CloudOrderResponse{
			InstanceId: args.ID,
			AppInfo: AppInfo{
				AuthUrl:  s.agent.client.Node().HTTPAddr,
				AuthCode: args.ID,
			},
		}
	}

	return out, nil
}

func (s *HTTPServer) orderUpdate(resp http.ResponseWriter, req *http.Request,
	orderId string) (interface{}, error) {
	var args *api.Order
	if err := decodeBody(req, &args); err != nil {
		return nil, CodedError(400, err.Error())
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
		JobID:                 order.JobID,
		ID:                    order.ID,
		SkuId:                 order.SkuId,
		TrafficAgainstLimits:  *order.TrafficAgainstLimits,
		TotalTransferredBytes: *order.TotalTransferredBytes,
		Status:                *order.Status,
		CreateIndex:           *order.CreateIndex,
		ModifyIndex:           *order.ModifyIndex,
		OrderModifyIndex:      *order.OrderModifyIndex,
	}
	return o
}

type CloudOrderResponse struct {
	InstanceId string
	AppInfo    AppInfo
}

type AppInfo struct {
	FrontEndUrl string
	AdminUrl    string
	Username    string
	Password    string
	AuthUrl     string
	AuthCode    string
}

//将map以字母a到z的顺序排序,按照“参数=参数值”的模式用“&”字符拼接成字符串
func filterParams(m map[string]string) (r_s string) {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	//升序排列
	sort.Strings(keys)
	for _, k := range keys {
		// 对请求参数中的中文进行编码处理
		r_s += k + "=" + url.QueryEscape(m[k]) + "&"
	}
	return
}

//生成签名字符串
func createSign(prestr string, secret string) (mysign string) {
	prestr = prestr + "key=" + GetMd5Hash(secret)
	mysign = GetMd5Hash(prestr)
	return
}

//获取字符串的MD5值
func GetMd5Hash(text string) string {
	hasher := md5.New()
	hasher.Write([]byte(text))
	md5_str := hex.EncodeToString(hasher.Sum(nil))
	return strings.ToUpper(md5_str)
}
