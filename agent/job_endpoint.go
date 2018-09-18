/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package agent

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/mitchellh/mapstructure"

	"udup/api"
	"udup/internal/client/driver/mysql/sql"
	"udup/internal/config"
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

func (s *HTTPServer) JobsRenewalRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	switch req.Method {
	case "PUT":
		return s.jobRenewalRequest(resp, req)
	default:
		return nil, CodedError(405, ErrInvalidMethod)
	}
}

func (s *HTTPServer) JobsInfoRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	switch req.Method {
	case "POST":
		return s.jobInfoRequest(resp, req)
	default:
		return nil, CodedError(405, ErrInvalidMethod)
	}
}

func (s *HTTPServer) jobInfoRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	var args *api.Job
	var replicateDoDb []*ZTreeData
	if err := decodeBody(req, &args); err != nil {
		return nil, CodedError(400, err.Error())
	}

	if args.Name == nil {
		return nil, CodedError(400, "Job Name hasn't been provided")
	}
	/*if len(args.Orders) == 0 {
		return nil, CodedError(400, "Order hasn't been provided")
	}*/
	if args.Region == nil {
		args.Region = &s.agent.config.Region
	}
	s.parseRegion(req, args.Region)

	sJob := ApiJobToStructJob(args, 0)

	for _, task := range sJob.Tasks {
		if task.Driver == models.TaskDriverMySQL && task.Type == models.TaskTypeSrc {
			var driverConfig config.MySQLDriverConfig
			if err := mapstructure.WeakDecode(task.Config, &driverConfig); err != nil {
				return nil, err
			}
			if "" == driverConfig.ConnectionConfig.Charset {
				driverConfig.ConnectionConfig.Charset = "utf8"
			}
			uri := driverConfig.ConnectionConfig.GetDBUri()
			db, err := sql.CreateDB(uri)
			defer db.Close()

			if err != nil {
				return nil, err
			}
			dbs, err := sql.ShowDatabases(db)
			if err != nil {
				s.logger.Errorf("jobInfoRequest err at connect/showdatabases: %v", err.Error())
				return nil, err
			}
			for dbIdx, dbName := range dbs {
				ds := &ZTreeData{
					Code: fmt.Sprintf("%d", dbIdx),
					Name: dbName,
				}

				tbs, err := sql.ShowTables(db, dbName, true)
				if err != nil {
					return nil, err
				}

				for tbIdx, t := range tbs {
					if strings.ToLower(t.TableType) == "view" {
						continue
					}
					tb := &Node{
						Code: fmt.Sprintf("%d-%d", dbIdx, tbIdx),
						Name: t.TableName,
					}
					ds.Nodes = append(ds.Nodes, tb)
				}
				replicateDoDb = append(replicateDoDb, ds)
			}
		}
	}

	return replicateDoDb, nil
}

func (s *HTTPServer) jobListRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	args := models.JobListRequest{}
	if args.Region == "" {
		args.Region = s.agent.config.Region
	}
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
	if args.Region == "" {
		args.Region = s.agent.config.Region
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
	if args.Region == "" {
		args.Region = s.agent.config.Region
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
	if args.Region == "" {
		args.Region = s.agent.config.Region
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
	var trafficLimit int
	if err := decodeBody(req, &args); err != nil {
		return nil, CodedError(400, err.Error())
	}

	if args.Name == nil {
		return nil, CodedError(400, "Job Name hasn't been provided")
	}
	/*if len(args.Orders) == 0 {
		return nil, CodedError(400, "Order hasn't been provided")
	}*/
	if args.Region == nil {
		args.Region = &s.agent.config.Region
	}
	s.parseRegion(req, args.Region)

	for _, order := range args.Orders {
		argsOrder := models.OrderSpecificRequest{
			OrderID: order,
		}
		if s.parse(resp, req, &argsOrder.Region, &argsOrder.QueryOptions) {
			return nil, nil
		}
		var outOrder models.SingleOrderResponse
		if err := s.agent.RPC("Order.GetOrder", &argsOrder, &outOrder); err != nil {
			return nil, err
		}

		setMeta(resp, &outOrder.QueryMeta)
		if outOrder.Order == nil {
			return nil, CodedError(404, "order not found")
		}
		trafficLimit += outOrder.Order.TrafficAgainstLimits
	}

	sJob := ApiJobToStructJob(args, trafficLimit)

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

func (s *HTTPServer) jobRenewalRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	var args *api.RenewalJobRequest
	if err := decodeBody(req, &args); err != nil {
		return nil, CodedError(400, err.Error())
	}

	if args.Region == nil {
		args.Region = &s.agent.config.Region
	}
	s.parseRegion(req, args.Region)

	argsOrder := models.OrderSpecificRequest{
		OrderID: args.OrderID,
	}
	if s.parse(resp, req, &argsOrder.Region, &argsOrder.QueryOptions) {
		return nil, nil
	}
	var outOrder models.SingleOrderResponse
	if err := s.agent.RPC("Order.GetOrder", &argsOrder, &outOrder); err != nil {
		return nil, err
	}

	setMeta(resp, &outOrder.QueryMeta)
	if outOrder.Order == nil {
		return nil, CodedError(404, "order not found")
	}

	regReq := models.JobRenewalRequest{
		JobID:   args.JobID,
		OrderID: args.OrderID,
		WriteRequest: models.WriteRequest{
			Region: *args.Region,
		},
	}
	var out models.JobResponse

	if err := s.agent.RPC("Job.Renewal", &regReq, &out); err != nil {
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

func (s *HTTPServer) ValidateJobRequest(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
	// Ensure request method is POST or PUT
	if !(req.Method == "POST" || req.Method == "PUT") {
		return nil, CodedError(405, ErrInvalidMethod)
	}

	var validateRequest api.JobValidateRequest
	if err := decodeBody(req, &validateRequest.Job); err != nil {
		return nil, CodedError(400, err.Error())
	}

	job := ApiJobToStructJob(validateRequest.Job, 0)
	args := models.JobValidateRequest{
		Job: job,
		WriteRequest: models.WriteRequest{
			Region: validateRequest.Region,
		},
	}
	s.parseRegion(req, &args.Region)

	var out models.JobValidateResponse
	if err := s.agent.RPC("Job.Validate", &args, &out); err != nil {
		out.Error = err.Error()
		return nil, err
	}

	return out, nil
}

func ApiJobToStructJob(job *api.Job, trafficLimit int) *models.Job {
	job.Canonicalize()

	j := &models.Job{
		Region:            *job.Region,
		ID:                *job.ID,
		Orders:            job.Orders,
		Name:              *job.Name,
		Failover:          job.Failover,
		Type:              *job.Type,
		Datacenters:       job.Datacenters,
		Status:            *job.Status,
		StatusDescription: *job.StatusDescription,
		CreateIndex:       *job.CreateIndex,
		ModifyIndex:       *job.ModifyIndex,
		JobModifyIndex:    *job.JobModifyIndex,
	}

	j.Tasks = make([]*models.Task, len(job.Tasks))
	cfg := ""
	for _, task := range job.Tasks {
		if task.Type == models.TaskTypeSrc {
			task.Config["TrafficAgainstLimits"] = trafficLimit
			if task.Config["Gtid"] != nil {
				cfg = fmt.Sprintf("%s", task.Config["Gtid"])
			}
		}

		if task.Driver == "" {
			task.Driver = models.TaskDriverMySQL
		}
	}
	for i, task := range job.Tasks {
		if task.Type == models.TaskTypeDest {
			task.Leader = true
			task.Config["Gtid"] = cfg
		}
		t := models.NewTask()
		ApiTaskToStructsTask(task, t)
		j.Tasks[i] = t
	}

	return j
}

func ApiTaskToStructsTask(apiTask *api.Task, structsTask *models.Task) {
	structsTask.Type = apiTask.Type
	structsTask.NodeID = apiTask.NodeID
	structsTask.NodeName = apiTask.NodeName
	structsTask.Driver = apiTask.Driver
	structsTask.Leader = apiTask.Leader
	structsTask.Config = apiTask.Config
}
