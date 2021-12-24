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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/test_util.go
//

// Copyright 2017 PingCAP, Inc.
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
	"github.com/google/uuid"
	"github.com/tikv/client-go/v2/internal/locate"
	pd "github.com/tikv/pd/client"
)

// NewTestTiKVStore creates a test store with Option
func NewTestTiKVStore(client Client, pdClient pd.Client, clientHijack func(Client) Client, pdClientHijack func(pd.Client) pd.Client, txnLocalLatches uint) (*KVStore, error) {
	if clientHijack != nil {
		client = clientHijack(client)
	}

	pdCli := pd.Client(locate.NewCodeCPDClient(pdClient))
	if pdClientHijack != nil {
		pdCli = pdClientHijack(pdCli)
	}

	// Make sure the uuid is unique.
	uid := uuid.New().String()
	spkv := NewMockSafePointKV()
	tikvStore, err := NewKVStore(uid, pdCli, spkv, client)

	if txnLocalLatches > 0 {
		tikvStore.EnableTxnLocalLatches(txnLocalLatches)
	}

	tikvStore.mock = true
	return tikvStore, err
}
