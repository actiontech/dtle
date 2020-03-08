/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package agent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"net/http/pprof"

	"github.com/NYTimes/gziphandler"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/ugorji/go/codec"

	"strings"

	umodel "github.com/actiontech/dtle/internal/models"
	"github.com/dgrijalva/jwt-go"
	"github.com/sirupsen/logrus"
)

const (
	// ErrInvalidMethod is used if the HTTP method is not supported
	ErrInvalidMethod = "Invalid method"
)

var (
	// jsonHandle and jsonHandlePretty are the codec handles to JSON encode
	// models. The pretty handle will add indents for easier human consumption.
	jsonHandle = &codec.JsonHandle{
		HTMLCharsAsIs: true,
	}
	jsonHandlePretty = &codec.JsonHandle{
		HTMLCharsAsIs: true,
		Indent:        4,
	}
)

// HTTPServer is used to wrap an Agent and expose it over an HTTP interface
type HTTPServer struct {
	agent    *Agent
	mux      *http.ServeMux
	listener net.Listener
	logger   *logrus.Logger
	uiDir    string
	addr     string
}

// NewHTTPServer starts new HTTP server over the agent
func NewHTTPServer(agent *Agent, config *Config, logOutput io.Writer) (*HTTPServer, error) {
	// Start the listener
	lnAddr, err := net.ResolveTCPAddr("tcp", config.normalizedAddrs.HTTP)
	if err != nil {
		return nil, err
	}
	ln, err := config.Listener("tcp", lnAddr.IP.String(), lnAddr.Port)
	if err != nil {
		return nil, fmt.Errorf("failed to start HTTP listener: %v", err)
	}

	// Create the mux
	mux := http.NewServeMux()

	// Create the server
	srv := &HTTPServer{
		agent:    agent,
		mux:      mux,
		listener: ln,
		logger:   agent.logger,
		uiDir:    config.UiDir,
		addr:     ln.Addr().String(),
	}
	srv.registerHandlers()

	// Start the server
	go http.Serve(ln, gziphandler.GzipHandler(mux))
	return srv, nil
}

// tcpKeepAliveListener sets TCP keep-alive timeouts on accepted
// connections. It's used by NewHttpServer so
// dead TCP connections eventually go away.
type tcpKeepAliveListener struct {
	*net.TCPListener
}

func (ln tcpKeepAliveListener) Accept() (c net.Conn, err error) {
	tc, err := ln.AcceptTCP()
	if err != nil {
		return
	}
	tc.SetKeepAlive(true)
	tc.SetKeepAlivePeriod(30 * time.Second)
	return tc, nil
}

// Shutdown is used to shutdown the HTTP server
func (s *HTTPServer) Shutdown() {
	if s != nil {
		s.logger.Debugf("http: Shutting down http server")
		s.listener.Close()
	}
}

// handleFuncMetrics takes the given pattern and handler and wraps to produce
// metrics based on the pattern and request.
func (s *HTTPServer) handleFuncMetrics(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	// Get the parts of the pattern. We omit any initial empty for the
	// leading slash, and put an underscore as a "thing" placeholder if we
	// see a trailing slash, which means the part after is parsed. This lets
	// us distinguish from things like /v1/query and /v1/query/<query id>.
	var parts []string
	for i, part := range strings.Split(pattern, "/") {
		if part == "" {
			if i == 0 {
				continue
			} else {
				part = "_"
			}
		}
		parts = append(parts, part)
	}

	// Register the wrapper, which will close over the expensive-to-compute
	// parts from above.
	wrapper := func(resp http.ResponseWriter, req *http.Request) {
		handler(resp, req)
	}
	s.mux.HandleFunc(pattern, wrapper)
}

// registerHandlers is used to attach our handlers to the mux
func (s *HTTPServer) registerHandlers() {
	//s.mux.HandleFunc("/", s.Index)

	s.mux.HandleFunc("/v1/login", s.wrap(s.LoginRequest))
	s.mux.HandleFunc("/v1/login/getVerifyCode", s.wrap(s.VerifyCodeRequest))
	s.mux.HandleFunc("/v1/user/list", s.wrap(s.UserListRequest))
	s.mux.HandleFunc("/v1/user/add", s.wrap(s.UserAddRequest))
	s.mux.HandleFunc("/v1/user/edit", s.wrap(s.UserEditRequest))
	s.mux.HandleFunc("/v1/user/delete/", s.wrap(s.UserDeleteRequest))
	s.mux.HandleFunc("/v1/orders", s.wrap(s.OrdersRequest))
	s.mux.HandleFunc("/v1/orders/pending", s.wrap(s.PendingOrdersRequest))
	s.mux.HandleFunc("/v1/order/", s.wrap(s.OrderSpecificRequest))
	s.mux.HandleFunc("/v1/orders/level/", s.wrap(s.setLogLevel))
	s.mux.HandleFunc("/v1/cloud/order", s.wrap(s.OrderCloudRequest))

	s.mux.HandleFunc("/v1/jobs/", s.wrap(s.JobsRequest))
	s.mux.HandleFunc("/v1/job/renewal", s.wrap(s.JobsRenewalRequest))
	s.mux.HandleFunc("/v1/job/info", s.wrap(s.JobsInfoRequest))
	s.mux.HandleFunc("/v1/validate/job", s.wrap(s.ValidateJobRequest))
	s.mux.HandleFunc("/v1/job/", s.wrap(s.JobSpecificRequest))

	s.mux.HandleFunc("/v1/nodes", s.wrap(s.NodesRequest))
	s.mux.HandleFunc("/v1/node/", s.wrap(s.NodeSpecificRequest))

	s.mux.HandleFunc("/v1/allocations", s.wrap(s.AllocsRequest))
	s.mux.HandleFunc("/v1/allocation/", s.wrap(s.AllocSpecificRequest))

	s.mux.HandleFunc("/v1/evaluations", s.wrap(s.EvalsRequest))
	s.mux.HandleFunc("/v1/evaluation/", s.wrap(s.EvalSpecificRequest))

	s.mux.HandleFunc("/v1/agent/allocation/", s.wrap(s.ClientAllocRequest))

	s.mux.HandleFunc("/v1/self", s.wrap(s.AgentSelfRequest))
	s.mux.HandleFunc("/v1/join", s.wrap(s.AgentJoinRequest))
	s.mux.HandleFunc("/v1/agent/force-leave", s.wrap(s.AgentForceLeaveRequest))
	s.mux.HandleFunc("/v1/members", s.wrap(s.AgentMembersRequest))
	s.mux.HandleFunc("/v1/managers", s.wrap(s.AgentServersRequest))

	s.mux.HandleFunc("/v1/regions", s.wrap(s.RegionListRequest))

	s.mux.HandleFunc("/v1/leader", s.wrap(s.StatusLeaderRequest))
	s.mux.HandleFunc("/v1/peers", s.wrap(s.StatusPeersRequest))

	s.mux.HandleFunc("/v1/operator/", s.wrap(s.OperatorRequest))

	if s.agent.config.LogLevel == "DEBUG" {
		s.mux.HandleFunc("/debug/pprof/", pprof.Index)
		s.mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		s.mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		s.mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	}

	// Use the custom UI dir if provided.
	if s.uiDir != "" {
		s.mux.Handle("/", http.StripPrefix("/", http.FileServer(http.Dir(s.uiDir))))
	} else if s.agent.config.EnableUi {
		s.mux.Handle("/", http.StripPrefix("/", http.FileServer(assetFS())))
	}

	s.mux.Handle("/metrics", promhttp.Handler())
}

// HTTPCodedError is used to provide the HTTP error code
type HTTPCodedError interface {
	error
	Code() int
}

func CodedError(c int, s string) HTTPCodedError {
	return &codedError{s, c}
}

type codedError struct {
	s    string
	code int
}

func (e *codedError) Error() string {
	return e.s
}

func (e *codedError) Code() int {
	return e.code
}

var _HMAC_SECRET = []byte("yizhifu Secret")

// wrap is used to wrap functions to make them more convenient
func (s *HTTPServer) wrap(handler func(resp http.ResponseWriter, req *http.Request) (interface{}, error)) func(resp http.ResponseWriter, req *http.Request) {
	f := func(resp http.ResponseWriter, req *http.Request) {
		setHeaders(resp, s.agent.config.HTTPAPIResponseHeaders)
		// Invoke the handler
		reqURL := req.URL.String()
		if !strings.Contains(reqURL, "login") {
			if "" != req.Header.Get("authorization") {
				tokenString := req.Header.Get("authorization")
				token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
					if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
						return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
					}
					return _HMAC_SECRET, nil
				})
				if nil != err {

					resp.WriteHeader(500)
					resp.Write([]byte(err.Error()))
					return
				}

				if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid && claims["user"] != "" {

				} else {
					resp.WriteHeader(500)
					resp.Write([]byte("no user"))
					return
				}

			} else {
				resp.WriteHeader(500)
				resp.Write([]byte("no user"))
				return
			}
		}

		start := time.Now()
		defer func() {
			s.logger.Debugf("http: Request %v (%v)", reqURL, time.Now().Sub(start))
		}()
		obj, err := handler(resp, req)

		// Check for an error
	HAS_ERR:
		if err != nil {
			s.logger.Errorf("http: Request %v, error: %v", reqURL, err)
			code := 500
			if http, ok := err.(HTTPCodedError); ok {
				code = http.Code()
			}
			resp.WriteHeader(code)
			resp.Write([]byte(err.Error()))
			return
		}

		prettyPrint := false
		if v, ok := req.URL.Query()["pretty"]; ok {
			if len(v) > 0 && (len(v[0]) == 0 || v[0] != "0") {
				prettyPrint = true
			}
		}

		// Write out the JSON object
		if obj != nil {
			var buf bytes.Buffer
			if prettyPrint {
				enc := codec.NewEncoder(&buf, jsonHandlePretty)
				err = enc.Encode(obj)
				if err == nil {
					buf.Write([]byte("\n"))
				}
			} else {
				enc := codec.NewEncoder(&buf, jsonHandle)
				err = enc.Encode(obj)
			}
			if err != nil {
				goto HAS_ERR
			}
			resp.Header().Set("Content-Type", "application/json")
			resp.Write(buf.Bytes())
		}
	}
	return f
}

// decodeBody is used to decode a JSON request body
func decodeBody(req *http.Request, out interface{}) error {
	dec := json.NewDecoder(req.Body)
	return dec.Decode(&out)
}

// setIndex is used to set the index response header
func setIndex(resp http.ResponseWriter, index uint64) {
	resp.Header().Set("X-Udup-Index", strconv.FormatUint(index, 10))
}

// setKnownLeader is used to set the known leader header
func setKnownLeader(resp http.ResponseWriter, known bool) {
	s := "true"
	if !known {
		s = "false"
	}
	resp.Header().Set("X-Udup-KnownLeader", s)
}

// setLastContact is used to set the last contact header
func setLastContact(resp http.ResponseWriter, last time.Duration) {
	lastMsec := uint64(last / time.Millisecond)
	resp.Header().Set("X-Udup-LastContact", strconv.FormatUint(lastMsec, 10))
}

// setMeta is used to set the query response meta data
func setMeta(resp http.ResponseWriter, m *umodel.QueryMeta) {
	setIndex(resp, m.Index)
	setLastContact(resp, m.LastContact)
	setKnownLeader(resp, m.KnownLeader)
}

// setHeaders is used to set canonical response header fields
func setHeaders(resp http.ResponseWriter, headers map[string]string) {
	for field, value := range headers {
		resp.Header().Set(http.CanonicalHeaderKey(field), value)
	}
}

// parseWait is used to parse the ?wait and ?index query params
// Returns true on error
func parseWait(resp http.ResponseWriter, req *http.Request, b *umodel.QueryOptions) bool {
	query := req.URL.Query()
	if wait := query.Get("wait"); wait != "" {
		dur, err := time.ParseDuration(wait)
		if err != nil {
			resp.WriteHeader(400)
			resp.Write([]byte("Invalid wait time"))
			return true
		}
		b.MaxQueryTime = dur
	}
	if idx := query.Get("index"); idx != "" {
		index, err := strconv.ParseUint(idx, 10, 64)
		if err != nil {
			resp.WriteHeader(400)
			resp.Write([]byte("Invalid index"))
			return true
		}
		b.MinQueryIndex = index
	}
	return false
}

// parseConsistency is used to parse the ?stale query params.
func parseConsistency(req *http.Request, b *umodel.QueryOptions) {
	query := req.URL.Query()
	if _, ok := query["stale"]; ok {
		b.AllowStale = true
	}
}

// parsePrefix is used to parse the ?prefix query param
func parsePrefix(req *http.Request, b *umodel.QueryOptions) {
	query := req.URL.Query()
	if prefix := query.Get("prefix"); prefix != "" {
		b.Prefix = prefix
	}
}

// parseRegion is used to parse the ?region query param
func (s *HTTPServer) parseRegion(req *http.Request, r *string) {
	if other := req.URL.Query().Get("region"); other != "" {
		*r = other
	} else if *r == "" {
		*r = s.agent.config.Region
	}
}

// parse is a convenience method for endpoints that need to parse multiple flags
func (s *HTTPServer) parse(resp http.ResponseWriter, req *http.Request, r *string, b *umodel.QueryOptions) bool {
	s.parseRegion(req, r)
	parseConsistency(req, b)
	parsePrefix(req, b)
	return parseWait(resp, req, b)
}
