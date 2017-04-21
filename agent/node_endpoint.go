package agent

import (
	"net/http"
	"strings"

	"udup/internal/models"
)

func (s *HTTPServer) NodesRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	if req.Method != "GET" {
		return nil, CodedError(405, ErrInvalidMethod)
	}

	args := models.NodeListRequest{}
	if s.parse(resp, req, &args.Region, &args.QueryOptions) {
		return nil, nil
	}

	var out models.NodeListResponse
	if err := s.agent.RPC("Node.List", &args, &out); err != nil {
		return nil, err
	}

	setMeta(resp, &out.QueryMeta)
	if out.Nodes == nil {
		out.Nodes = make([]*models.NodeListStub, 0)
	}
	return out.Nodes, nil
}

func (s *HTTPServer) NodeSpecificRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	path := strings.TrimPrefix(req.URL.Path, "/v1/node/")
	switch {
	case strings.HasSuffix(path, "/evaluate"):
		nodeName := strings.TrimSuffix(path, "/evaluate")
		return s.nodeForceEvaluate(resp, req, nodeName)
	case strings.HasSuffix(path, "/allocations"):
		nodeName := strings.TrimSuffix(path, "/allocations")
		return s.nodeAllocations(resp, req, nodeName)
	default:
		return s.nodeQuery(resp, req, path)
	}
}

func (s *HTTPServer) nodeForceEvaluate(resp http.ResponseWriter, req *http.Request,
	nodeID string) (interface{}, error) {
	if req.Method != "PUT" && req.Method != "POST" {
		return nil, CodedError(405, ErrInvalidMethod)
	}
	args := models.NodeEvaluateRequest{
		NodeID: nodeID,
	}
	s.parseRegion(req, &args.Region)

	var out models.NodeUpdateResponse
	if err := s.agent.RPC("Node.Evaluate", &args, &out); err != nil {
		return nil, err
	}
	setIndex(resp, out.Index)
	return out, nil
}

func (s *HTTPServer) nodeAllocations(resp http.ResponseWriter, req *http.Request,
	nodeID string) (interface{}, error) {
	if req.Method != "GET" {
		return nil, CodedError(405, ErrInvalidMethod)
	}
	args := models.NodeSpecificRequest{
		NodeID: nodeID,
	}
	if s.parse(resp, req, &args.Region, &args.QueryOptions) {
		return nil, nil
	}

	var out models.NodeAllocsResponse
	if err := s.agent.RPC("Node.GetAllocs", &args, &out); err != nil {
		return nil, err
	}

	setMeta(resp, &out.QueryMeta)
	if out.Allocs == nil {
		out.Allocs = make([]*models.Allocation, 0)
	}
	return out.Allocs, nil
}

func (s *HTTPServer) nodeQuery(resp http.ResponseWriter, req *http.Request,
	nodeID string) (interface{}, error) {
	if req.Method != "GET" {
		return nil, CodedError(405, ErrInvalidMethod)
	}
	args := models.NodeSpecificRequest{
		NodeID: nodeID,
	}
	if s.parse(resp, req, &args.Region, &args.QueryOptions) {
		return nil, nil
	}

	var out models.SingleNodeResponse
	if err := s.agent.RPC("Node.GetNode", &args, &out); err != nil {
		return nil, err
	}

	setMeta(resp, &out.QueryMeta)
	if out.Node == nil {
		return nil, CodedError(404, "node not found")
	}
	return out.Node, nil
}
