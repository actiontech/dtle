package agent

import (
	"fmt"
	"net/http"
	"strings"

	umodel "udup/internal/models"
)

var (
	clientNotRunning = fmt.Errorf("node is not running a Udup Agent")
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
	if len(tokens) != 2 {
		return nil, CodedError(404, resourceNotFoundErr)
	}
	allocID := tokens[0]
	switch tokens[1] {
	case "stats":
		return s.allocStats(allocID, resp, req)
	}

	return nil, CodedError(404, resourceNotFoundErr)
}

func (s *HTTPServer) allocStats(allocID string, resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	clientStats := s.agent.client.StatsReporter()
	aStats, err := clientStats.GetAllocStats(allocID)
	if err != nil {
		return nil, err
	}

	task := req.URL.Query().Get("task")
	return aStats.LatestAllocStats(task)
}
