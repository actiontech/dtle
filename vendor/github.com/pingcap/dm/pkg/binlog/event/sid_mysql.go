// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// binlog events generator for MySQL used to generate some binlog events for tests.
// Readability takes precedence over performance.

package event

import (
	"encoding/hex"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/pingcap/dm/pkg/terror"
)

// SID represents a SERVER_UUID in GTIDEvent/PrevGTIDEvent.
type SID [replication.SidLength]byte

// Bytes returns the byte slices representation of SID.
func (sid SID) Bytes() []byte {
	return sid[:]
}

func (sid SID) String() string {
	dst := []byte("xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")
	hex.Encode(dst, sid[:4])
	hex.Encode(dst[9:], sid[4:6])
	hex.Encode(dst[14:], sid[6:8])
	hex.Encode(dst[19:], sid[8:10])
	hex.Encode(dst[24:], sid[10:16])
	return string(dst)
}

// ParseSID parses a SID from its string representation.
// ref https://github.com/vitessio/vitess/blob/869543aa2c4c467f146ba7d77f6d090ca7f8a862/go/mysql/mysql56_gtid.go#L66.
func ParseSID(s string) (SID, error) {
	var sid SID
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return sid, terror.ErrBinlogParseSID.Generatef("SID string %s not valid", s)
	}

	// drop the dashes so we can just check the error of Decode once.
	b := make([]byte, 0, 32)
	b = append(b, s[:8]...)
	b = append(b, s[9:13]...)
	b = append(b, s[14:18]...)
	b = append(b, s[19:23]...)
	b = append(b, s[24:]...)

	if _, err := hex.Decode(sid[:], b); err != nil {
		return sid, terror.ErrBinlogParseSID.SetMessage("decode % X").Delegate(err, b)
	}
	return sid, nil
}
