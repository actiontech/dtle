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
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/locate/pd_codec.go
//

// Copyright 2016 PingCAP, Inc.
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

package locate

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/tikv/client-go/v2/util/codec"
	pd "github.com/tikv/pd/client"
)

// CodecPDClient wraps a PD Client to decode the encoded keys in region meta.
type CodecPDClient struct {
	pd.Client
}

// NewCodeCPDClient creates a CodecPDClient.
func NewCodeCPDClient(client pd.Client) *CodecPDClient {
	return &CodecPDClient{client}
}

// GetRegion encodes the key before send requests to pd-server and decodes the
// returned StartKey && EndKey from pd-server.
func (c *CodecPDClient) GetRegion(ctx context.Context, key []byte) (*pd.Region, error) {
	encodedKey := codec.EncodeBytes([]byte(nil), key)
	region, err := c.Client.GetRegion(ctx, encodedKey)
	return processRegionResult(region, err)
}

// GetPrevRegion encodes the key before send requests to pd-server and decodes the
// returned StartKey && EndKey from pd-server.
func (c *CodecPDClient) GetPrevRegion(ctx context.Context, key []byte) (*pd.Region, error) {
	encodedKey := codec.EncodeBytes([]byte(nil), key)
	region, err := c.Client.GetPrevRegion(ctx, encodedKey)
	return processRegionResult(region, err)
}

// GetRegionByID encodes the key before send requests to pd-server and decodes the
// returned StartKey && EndKey from pd-server.
func (c *CodecPDClient) GetRegionByID(ctx context.Context, regionID uint64) (*pd.Region, error) {
	region, err := c.Client.GetRegionByID(ctx, regionID)
	return processRegionResult(region, err)
}

// ScanRegions encodes the key before send requests to pd-server and decodes the
// returned StartKey && EndKey from pd-server.
func (c *CodecPDClient) ScanRegions(ctx context.Context, startKey []byte, endKey []byte, limit int) ([]*pd.Region, error) {
	startKey = codec.EncodeBytes([]byte(nil), startKey)
	if len(endKey) > 0 {
		endKey = codec.EncodeBytes([]byte(nil), endKey)
	}

	regions, err := c.Client.ScanRegions(ctx, startKey, endKey, limit)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	for _, region := range regions {
		if region != nil {
			err = decodeRegionMetaKeyInPlace(region.Meta)
			if err != nil {
				return nil, err
			}
		}
	}
	return regions, nil
}

func processRegionResult(region *pd.Region, err error) (*pd.Region, error) {
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if region == nil || region.Meta == nil {
		return nil, nil
	}
	err = decodeRegionMetaKeyInPlace(region.Meta)
	if err != nil {
		return nil, err
	}
	return region, nil
}

// decodeError happens if the region range key is not well-formed.
// It indicates TiKV has bugs and the client can't handle such a case,
// so it should report the error to users soon.
type decodeError struct {
	error
}

func isDecodeError(err error) bool {
	_, ok := errors.Cause(err).(*decodeError)
	if !ok {
		_, ok = errors.Cause(err).(decodeError)
	}
	return ok
}

func decodeRegionMetaKeyInPlace(r *metapb.Region) error {
	if len(r.StartKey) != 0 {
		_, decoded, err := codec.DecodeBytes(r.StartKey, nil)
		if err != nil {
			return errors.WithStack(&decodeError{err})
		}
		r.StartKey = decoded
	}
	if len(r.EndKey) != 0 {
		_, decoded, err := codec.DecodeBytes(r.EndKey, nil)
		if err != nil {
			return errors.WithStack(&decodeError{err})
		}
		r.EndKey = decoded
	}
	return nil
}

func decodeRegionMetaKeyWithShallowCopy(r *metapb.Region) (*metapb.Region, error) {
	nr := *r
	if len(r.StartKey) != 0 {
		_, decoded, err := codec.DecodeBytes(r.StartKey, nil)
		if err != nil {
			return nil, err
		}
		nr.StartKey = decoded
	}
	if len(r.EndKey) != 0 {
		_, decoded, err := codec.DecodeBytes(r.EndKey, nil)
		if err != nil {
			return nil, err
		}
		nr.EndKey = decoded
	}
	return &nr, nil
}
