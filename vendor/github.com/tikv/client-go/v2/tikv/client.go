// Copyright 2021 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/client.go
//

// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tikv

import (
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/internal/client"
)

// Client is a client that sends RPC.
// It should not be used after calling Close().
type Client = client.Client

// ClientOpt defines the option to create RPC client.
type ClientOpt = client.Opt

// WithSecurity is used to set security config.
func WithSecurity(security config.Security) ClientOpt {
	return client.WithSecurity(security)
}

// Timeout durations.
const (
	ReadTimeoutMedium     = client.ReadTimeoutMedium
	ReadTimeoutShort      = client.ReadTimeoutShort
	MaxWriteExecutionTime = client.MaxWriteExecutionTime
)

// NewRPCClient creates a client that manages connections and rpc calls with tikv-servers.
func NewRPCClient(opts ...ClientOpt) *client.RPCClient {
	return client.NewRPCClient(opts...)
}
