package agent

import (
	"net/http"
	"strconv"
	"strings"

	"udup/api"
	"udup/internal/models"
)

func (s *HTTPServer) JobsRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	switch req.Method {
	case "GET":
		return s.jobListRequest(resp, req)
	case "PUT", "POST":
		return s.jobUpdate(resp, req, "")
	default:
		return nil, CodedError(405, ErrInvalidMethod)
	}
}

func (s *HTTPServer) jobListRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	args := models.JobListRequest{}
	if s.parse(resp, req, &args.Region, &args.QueryOptions) {
		return nil, nil
	}

	var out models.JobListResponse
	if err := s.agent.RPC("Job.List", &args, &out); err != nil {
		return nil, err
	}

	setMeta(resp, &out.QueryMeta)
	if out.Jobs == nil {
		out.Jobs = make([]*models.JobListStub, 0)
	}
	return out.Jobs, nil
}

func (s *HTTPServer) JobSpecificRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	path := strings.TrimPrefix(req.URL.Path, "/v1/job/")
	switch {
	case strings.HasSuffix(path, "/resume"):
		jobName := strings.TrimSuffix(path, "/resume")
		return s.jobResumeRequest(resp, req, jobName)
	case strings.HasSuffix(path, "/pause"):
		jobName := strings.TrimSuffix(path, "/pause")
		return s.jobPauseRequest(resp, req, jobName)
	case strings.HasSuffix(path, "/allocations"):
		jobName := strings.TrimSuffix(path, "/allocations")
		return s.jobAllocations(resp, req, jobName)
	case strings.HasSuffix(path, "/evaluations"):
		jobName := strings.TrimSuffix(path, "/evaluations")
		return s.jobEvaluations(resp, req, jobName)
	default:
		return s.jobCRUD(resp, req, path)
	}
}

func (s *HTTPServer) jobAllocations(resp http.ResponseWriter, req *http.Request,
	jobName string) (interface{}, error) {
	if req.Method != "GET" {
		return nil, CodedError(405, ErrInvalidMethod)
	}
	allAllocs, _ := strconv.ParseBool(req.URL.Query().Get("all"))

	args := models.JobSpecificRequest{
		JobID:     jobName,
		AllAllocs: allAllocs,
	}
	if s.parse(resp, req, &args.Region, &args.QueryOptions) {
		return nil, nil
	}

	var out models.JobAllocationsResponse
	if err := s.agent.RPC("Job.Allocations", &args, &out); err != nil {
		return nil, err
	}

	setMeta(resp, &out.QueryMeta)
	if out.Allocations == nil {
		out.Allocations = make([]*models.AllocListStub, 0)
	}
	return out.Allocations, nil
}

func (s *HTTPServer) jobEvaluations(resp http.ResponseWriter, req *http.Request,
	jobName string) (interface{}, error) {
	if req.Method != "GET" {
		return nil, CodedError(405, ErrInvalidMethod)
	}
	args := models.JobSpecificRequest{
		JobID: jobName,
	}
	if s.parse(resp, req, &args.Region, &args.QueryOptions) {
		return nil, nil
	}

	var out models.JobEvaluationsResponse
	if err := s.agent.RPC("Job.Evaluations", &args, &out); err != nil {
		return nil, err
	}

	setMeta(resp, &out.QueryMeta)
	if out.Evaluations == nil {
		out.Evaluations = make([]*models.Evaluation, 0)
	}
	return out.Evaluations, nil
}

func (s *HTTPServer) jobCRUD(resp http.ResponseWriter, req *http.Request,
	jobName string) (interface{}, error) {
	switch req.Method {
	case "GET":
		return s.jobQuery(resp, req, jobName)
	case "PUT", "POST":
		return s.jobUpdate(resp, req, jobName)
	case "DELETE":
		return s.jobDelete(resp, req, jobName)
	default:
		return nil, CodedError(405, ErrInvalidMethod)
	}
}

func (s *HTTPServer) jobQuery(resp http.ResponseWriter, req *http.Request,
	jobId string) (interface{}, error) {
	args := models.JobSpecificRequest{
		JobID: jobId,
	}
	if s.parse(resp, req, &args.Region, &args.QueryOptions) {
		return nil, nil
	}

	var out models.SingleJobResponse
	if err := s.agent.RPC("Job.GetJob", &args, &out); err != nil {
		return nil, err
	}

	setMeta(resp, &out.QueryMeta)
	if out.Job == nil {
		return nil, CodedError(404, "job not found")
	}

	job := out.Job

	return job, nil
}

func (s *HTTPServer) jobUpdate(resp http.ResponseWriter, req *http.Request,
	jobName string) (interface{}, error) {
	var args *api.Job
	if err := decodeBody(req, &args); err != nil {
		return nil, CodedError(400, err.Error())
	}

	if args.Name == nil {
		return nil, CodedError(400, "Job Name hasn't been provided")
	}
	s.parseRegion(req, args.Region)

	sJob := ApiJobToStructJob(args)

	regReq := models.JobRegisterRequest{
		Job:            sJob,
		EnforceIndex:   args.EnforceIndex,
		JobModifyIndex: *args.JobModifyIndex,
		WriteRequest: models.WriteRequest{
			Region: *args.Region,
		},
	}
	var out models.JobResponse
	if err := s.agent.RPC("Job.Register", &regReq, &out); err != nil {
		return nil, err
	}
	setIndex(resp, out.Index)
	return out, nil
}

func (s *HTTPServer) jobDelete(resp http.ResponseWriter, req *http.Request,
	jobName string) (interface{}, error) {
	args := models.JobDeregisterRequest{
		JobID: jobName,
	}
	s.parseRegion(req, &args.Region)

	var out models.JobResponse
	if err := s.agent.RPC("Job.Deregister", &args, &out); err != nil {
		return nil, err
	}
	setIndex(resp, out.Index)
	return out, nil
}

func (s *HTTPServer) jobResumeRequest(resp http.ResponseWriter, req *http.Request, name string) (interface{}, error) {
	args := models.JobUpdateStatusRequest{
		JobID:  name,
		Status: models.JobStatusRunning,
	}
	s.parseRegion(req, &args.Region)

	var out models.JobResponse
	if err := s.agent.RPC("Job.UpdateStatus", &args, &out); err != nil {
		return nil, err
	}
	setIndex(resp, out.Index)
	return out, nil
}

func (s *HTTPServer) jobPauseRequest(resp http.ResponseWriter, req *http.Request, name string) (interface{}, error) {
	args := models.JobUpdateStatusRequest{
		JobID:  name,
		Status: models.JobStatusPause,
	}
	s.parseRegion(req, &args.Region)

	var out models.JobResponse
	if err := s.agent.RPC("Job.UpdateStatus", &args, &out); err != nil {
		return nil, err
	}
	setIndex(resp, out.Index)
	return out, nil
}

func ApiJobToStructJob(job *api.Job) *models.Job {
	job.Canonicalize()

	j := &models.Job{
		Region:            *job.Region,
		ID:                *job.ID,
		Name:              *job.Name,
		Type:              *job.Type,
		Datacenters:       job.Datacenters,
		Status:            *job.Status,
		StatusDescription: *job.StatusDescription,
		CreateIndex:       *job.CreateIndex,
		ModifyIndex:       *job.ModifyIndex,
		JobModifyIndex:    *job.JobModifyIndex,
	}

	j.Tasks = make([]*models.Task, len(job.Tasks))
	for i, task := range job.Tasks {
		t := &models.Task{}
		ApiTaskToStructsTask(task, t)
		j.Tasks[i] = t
	}

	return j
}

func ApiTaskToStructsTask(apiTask *api.Task, structsTask *models.Task) {
	structsTask.Type = apiTask.Type
	structsTask.NodeId = apiTask.NodeId
	structsTask.Driver = apiTask.Driver
	structsTask.Leader = apiTask.Leader
	structsTask.Config = apiTask.Config
}
