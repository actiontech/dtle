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

	umodel "github.com/actiontech/dtle/internal/models"
	"io/ioutil"
	"encoding/json"
	"strconv"
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
	if s.agent.client == nil {
		return nil, clientNotRunning
	}

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
	clientStats := s.agent.client.StatsReporter()
	aStats, err := clientStats.GetAllocStats(allocID)
	if err != nil && strings.Contains(err.Error(), "unknown allocation ID") && !forward{
		s.logger.Debug("not find alloc in %v,forward to other node to find ",s.addr)
		 port:=s.agent.config.Ports.HTTP
		 if port==0{
			port=8190
		}
		return  s.allocStatsForward(allocID,s.addr,strconv.Itoa(port))
	}else if  err != nil{
		return nil, err
	}

	task := req.URL.Query().Get("task")
	return aStats.LatestAllocStats(task)
}
func (s *HTTPServer) allocStatsForward(allocID string,addr string,port string) (interface{}, error) {

	//get other node ip
	args := &umodel.GenericRequest{}
	var out umodel.ServerMembersResponse
	if err := s.agent.RPC("Status.Members", args, &out); err != nil {
		return nil, err
	}
	var realErr  error
	//forward to other ip
	for _ ,server:=range out.Members{
		serverAddr:= server.Addr.String()+":"+port
		if serverAddr!=addr{
			url:="http://"+ serverAddr+"/v1/agent/allocation/"+allocID+"/stats/forward"
			resp, err :=   http.Get(url)
			if err != nil {
				realErr= err
			}
			defer resp.Body.Close()
			if resp.StatusCode == 200 {
				body, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					fmt.Println(err)
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

