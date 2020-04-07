/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package agent

import (
	"fmt"
	"net/http"
	"strings"

	umodel "github.com/actiontech/dts/internal/models"
	"io/ioutil"
	"encoding/json"
	)

var (
	clientNotRunning = fmt.Errorf("node is not running a Dtle Agent")
)

const (
	resourceNotFoundErr = "resource not found"
)

func (s *HTTPServer) AllocsRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	s.logger.Debugf("HTTPServer.AllocsRequest")
	if req.Method != "GET" {
		return nil, CodedError(405, ErrInvalidMethod)
	}

	args := umodel.AllocListRequest{}
	if s.parse(resp, req, &args.Region, &args.QueryOptions) {
		return nil, nil
	}

	var out umodel.AllocListResponse
	s.logger.Debugf("HTTPServer.AllocsRequest: call rpc")
	if err := s.agent.RPC("Alloc.List", &args, &out); err != nil {
		return nil, err
	}

	setMeta(resp, &out.QueryMeta)
	if out.Allocations == nil {
		out.Allocations = make([]*umodel.AllocListStub, 0)
	}
	return out.Allocations, nil
}

func (s *HTTPServer) AllocSpecificRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	allocID := strings.TrimPrefix(req.URL.Path, "/v1/allocation/")
	if req.Method != "GET" {
		return nil, CodedError(405, ErrInvalidMethod)
	}

	args := umodel.AllocSpecificRequest{
		AllocID: allocID,
	}
	if s.parse(resp, req, &args.Region, &args.QueryOptions) {
		return nil, nil
	}

	var out umodel.SingleAllocResponse
	if err := s.agent.RPC("Alloc.GetAlloc", &args, &out); err != nil {
		return nil, err
	}

	setMeta(resp, &out.QueryMeta)
	if out.Alloc == nil {
		return nil, CodedError(404, "alloc not found")
	}

	alloc := out.Alloc
	if alloc.Job != nil {
		alloc = alloc.Copy()
	}

	return alloc, nil
}

func (s *HTTPServer) ClientAllocRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	reqSuffix := strings.TrimPrefix(req.URL.Path, "/v1/agent/allocation/")
	// tokenize the suffix of the path to get the alloc id and find the action
	// invoked on the alloc id
	tokens := strings.Split(reqSuffix, "/")
	var forward bool
	if len(tokens) == 2{
		forward=false
	}else if len(tokens) ==3  {
		forward=true
	}else{
		return nil, CodedError(404, resourceNotFoundErr)
	}
	allocID := tokens[0]
	switch tokens[1] {
	case "stats":
		return s.allocStats(allocID,forward, resp, req)
	}
	return nil, CodedError(404, resourceNotFoundErr)
}

func (s *HTTPServer) allocStats(allocID string,forward bool, resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	//if not agent ,go to agent to get alloc
	if s.agent.client == nil {
		s.logger.Debug("node is not a agent , ,forward to agent node to find the message")
		return  s.allocStatsForward(allocID,s.addr)
	}
	////if alloc not in this agent ,go to other agent to find
	clientStats := s.agent.client.StatsReporter()
	aStats, err := clientStats.GetAllocStats(allocID)
	if err != nil && !forward{
		s.logger.Debug("not find alloc in %v,forward to other node to find ",s.addr)
		return  s.allocStatsForward(allocID,s.addr)
	}else if  err != nil{
		return nil, err
	}

	task := req.URL.Query().Get("task")
	return aStats.LatestAllocStats(task)
}
func (s *HTTPServer) allocStatsForward(allocID string,addr string) (interface{}, error) {

	//get other node ip
	args := umodel.NodeListRequest{}
	args.Region ="global"
	var out umodel.NodeListResponse
	if err := s.agent.RPC("Node.List", &args, &out); err != nil {
		return nil, err
	}
	var realErr  error
	for _ ,server:=range out.Nodes{
		serverAddr:= server.HTTPAddr
		if serverAddr!=addr && server.Status=="ready"{
			url:="http://"+ serverAddr+"/v1/agent/allocation/"+allocID+"/stats/forward"
			s.logger.Debug("go to  %v ,find the alloc",url)
			resp, err :=   http.Get(url)
			if err != nil {
				realErr= err
			}
			defer resp.Body.Close()
			if resp.StatusCode == 200 {
				body, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					realErr= err
				}
				var value interface{}
				if err:=json.Unmarshal(body,&value);err!=nil{
					return  nil,err
				}
				return  value,nil
			}
		}
	}
	return nil,realErr
}

