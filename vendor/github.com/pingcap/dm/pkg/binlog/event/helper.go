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

package event

import (
	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/pingcap/dm/pkg/gtid"
	"github.com/pingcap/dm/pkg/terror"
)

// GTIDsFromPreviousGTIDsEvent get GTID set from a PreviousGTIDsEvent.
func GTIDsFromPreviousGTIDsEvent(e *replication.BinlogEvent) (gtid.Set, error) {
	var gSetStr string
	switch ev := e.Event.(type) {
	case *replication.PreviousGTIDsEvent:
		gSetStr = ev.GTIDSets
	default:
		return nil, terror.ErrBinlogPrevGTIDEvNotValid.Generate(e.Event)
	}

	return gtid.ParserGTID(gmysql.MySQLFlavor, gSetStr)
}

// GTIDsFromMariaDBGTIDListEvent get GTID set from a MariaDBGTIDListEvent.
func GTIDsFromMariaDBGTIDListEvent(e *replication.BinlogEvent) (gtid.Set, error) {
	var gtidListEv *replication.MariadbGTIDListEvent
	switch ev := e.Event.(type) {
	case *replication.MariadbGTIDListEvent:
		gtidListEv = ev
	default:
		return nil, terror.ErrBinlogNeedMariaDBGTIDSet.Generate(e.Event)
	}

	ggSet, err := gmysql.ParseMariadbGTIDSet("")
	if err != nil {
		return nil, terror.ErrBinlogParseMariaDBGTIDSet.Delegate(err)
	}
	mGSet := ggSet.(*gmysql.MariadbGTIDSet)
	for _, mGTID := range gtidListEv.GTIDs {
		mgClone := mGTID // use another variable so we can get different pointer (&mgClone below) when iterating
		err = mGSet.AddSet(&mgClone)
		if err != nil {
			return nil, terror.ErrBinlogMariaDBAddGTIDSet.Delegate(err, mGTID)
		}
	}

	// always MariaDB for MariaDBGTIDListEvent
	gSet, err := gtid.ParserGTID(gmysql.MariaDBFlavor, "")
	if err != nil {
		return nil, terror.Annotatef(err, "parse empty GTID set")
	}
	err = gSet.Set(ggSet)
	if err != nil {
		return nil, terror.Annotatef(err, "replace GTID set with set %v", ggSet)
	}

	return gSet, nil
}
