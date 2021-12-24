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

package gtid

import (
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"

	"github.com/pingcap/dm/pkg/terror"
)

// Set provide gtid operations for syncer.
type Set interface {
	Set(mysql.GTIDSet) error
	// compute set of self and other gtid set
	// 1. keep intersection of self and other gtid set
	// 2. keep complementary set of other gtid set except master identifications that not in self gtid
	// masters => master identification set, represents which db instances do write in one replicate group
	// example: self gtid set [xx:1-2, yy:1-3, xz:1-4], other gtid set [xx:1-4, yy:1-12, xy:1-3]. master ID set [xx]
	// => [xx:1-2, yy:1-3, xy:1-3]
	// more examples ref test cases
	Replace(other Set, masters []interface{}) error
	Clone() Set
	Origin() mysql.GTIDSet
	Equal(other Set) bool
	Contain(other Set) bool

	// Truncate truncates the current GTID sets until the `end` in-place.
	// NOTE: the original GTID sets should contain the end GTID sets, otherwise it's invalid.
	// like truncating `00c04543-f584-11e9-a765-0242ac120002:1-100` with `00c04543-f584-11e9-a765-0242ac120002:40-60`
	// should become `00c04543-f584-11e9-a765-0242ac120002:1-60`.
	Truncate(end Set) error

	String() string
}

// ParserGTID parses GTID from string.
func ParserGTID(flavor, gtidStr string) (Set, error) {
	var (
		m    Set
		err  error
		gtid mysql.GTIDSet
	)

	if len(flavor) == 0 && len(gtidStr) == 0 {
		return nil, errors.Errorf("empty flavor with empty gtid is invalid")
	}

	fla := flavor
	switch fla {
	case mysql.MySQLFlavor, mysql.MariaDBFlavor:
		gtid, err = mysql.ParseGTIDSet(fla, gtidStr)
	case "":
		fla = mysql.MySQLFlavor
		gtid, err = mysql.ParseGTIDSet(fla, gtidStr)
		if err != nil {
			fla = mysql.MariaDBFlavor
			gtid, err = mysql.ParseGTIDSet(fla, gtidStr)
		}
	default:
		err = terror.ErrNotSupportedFlavor.Generate(flavor)
	}

	if err != nil {
		return nil, err
	}

	switch fla {
	case mysql.MariaDBFlavor:
		m = &MariadbGTIDSet{}
	case mysql.MySQLFlavor:
		m = &MySQLGTIDSet{}
	default:
		return nil, terror.ErrNotSupportedFlavor.Generate(flavor)
	}
	err = m.Set(gtid)
	return m, err
}

// MinGTIDSet returns the min GTID set.
func MinGTIDSet(flavor string) Set {
	// use mysql as default
	if flavor != mysql.MariaDBFlavor && flavor != mysql.MySQLFlavor {
		flavor = mysql.MySQLFlavor
	}

	gset, err := ParserGTID(flavor, "")
	if err != nil {
		// this should not happen
		panic(err)
	}
	return gset
}

/************************ mysql gtid set ***************************/

// MySQLGTIDSet wraps mysql.MysqlGTIDSet to implement gtidSet interface
// extend some functions to retrieve and compute an intersection with other MySQL GTID Set.
type MySQLGTIDSet struct {
	set *mysql.MysqlGTIDSet
}

// Set implements Set.Set, replace g by other.
func (g *MySQLGTIDSet) Set(other mysql.GTIDSet) error {
	if other == nil {
		return nil
	}

	gs, ok := other.(*mysql.MysqlGTIDSet)
	if !ok {
		return terror.ErrNotMySQLGTID.Generate(other)
	}

	g.set = gs
	return nil
}

// Replace implements Set.Replace.
func (g *MySQLGTIDSet) Replace(other Set, masters []interface{}) error {
	if other == nil {
		return nil
	}

	otherGS, ok := other.(*MySQLGTIDSet)
	if !ok {
		return terror.ErrNotMySQLGTID.Generate(other)
	}

	for _, uuid := range masters {
		uuidStr, ok := uuid.(string)
		if !ok {
			return terror.ErrNotUUIDString.Generate(uuid)
		}

		otherGS.delete(uuidStr)
		if uuidSet, ok := g.get(uuidStr); ok {
			otherGS.set.AddSet(uuidSet)
		}
	}

	for uuid, set := range g.set.Sets {
		if _, ok := otherGS.get(uuid); ok {
			otherGS.delete(uuid)
			otherGS.set.AddSet(set)
		}
	}

	g.set = otherGS.set.Clone().(*mysql.MysqlGTIDSet)
	return nil
}

func (g *MySQLGTIDSet) delete(uuid string) {
	delete(g.set.Sets, uuid)
}

func (g *MySQLGTIDSet) get(uuid string) (*mysql.UUIDSet, bool) {
	uuidSet, ok := g.set.Sets[uuid]
	return uuidSet, ok
}

// Clone implements Set.Clone.
func (g *MySQLGTIDSet) Clone() Set {
	if g.set == nil {
		return MinGTIDSet(mysql.MySQLFlavor)
	}

	return &MySQLGTIDSet{
		set: g.set.Clone().(*mysql.MysqlGTIDSet),
	}
}

// Origin implements Set.Origin.
func (g *MySQLGTIDSet) Origin() mysql.GTIDSet {
	if g.set == nil {
		return &mysql.MysqlGTIDSet{}
	}
	return g.set.Clone().(*mysql.MysqlGTIDSet)
}

// Equal implements Set.Equal.
func (g *MySQLGTIDSet) Equal(other Set) bool {
	otherIsNil := other == nil
	if !otherIsNil {
		otherGS, ok := other.(*MySQLGTIDSet)
		if !ok {
			return false
		}
		otherIsNil = otherGS == nil
	}

	if g == nil && otherIsNil {
		return true
	} else if g == nil || otherIsNil {
		return false
	}

	return g.set.Equal(other.Origin())
}

// Contain implements Set.Contain.
func (g *MySQLGTIDSet) Contain(other Set) bool {
	otherIsNil := other == nil
	if !otherIsNil {
		otherGs, ok := other.(*MySQLGTIDSet)
		if !ok {
			return false
		}
		otherIsNil = otherGs == nil
	}
	if otherIsNil {
		return true // any set (including nil) contains nil
	} else if g == nil {
		return false // nil only contains nil
	}
	return g.set.Contain(other.Origin())
}

// Truncate implements Set.Truncate.
func (g *MySQLGTIDSet) Truncate(end Set) error {
	if end == nil {
		return nil // do nothing
	}
	if !g.Contain(end) {
		return terror.ErrGTIDTruncateInvalid.Generate(g, end)
	}
	endGs := end.(*MySQLGTIDSet) // already verify the type is `*MySQLGTIDSet` in `Contain`.
	if endGs == nil {
		return nil // do nothing
	}

	for sid, setG := range g.set.Sets {
		setE, ok := endGs.set.Sets[sid]
		if !ok {
			continue // no need to truncate for this SID
		}
		for i, interG := range setG.Intervals {
			for _, interE := range setE.Intervals {
				if interG.Start <= interE.Start && interG.Stop >= interE.Stop {
					interG.Stop = interE.Stop // truncate the stop
				}
			}
			setG.Intervals[i] = interG // overwrite the value (because it's not a pointer)
		}
	}

	return nil
}

func (g *MySQLGTIDSet) String() string {
	if g.set == nil {
		return ""
	}
	return g.set.String()
}

/************************ mariadb gtid set ***************************/

// MariadbGTIDSet wraps mysql.MariadbGTIDSet to implement gtidSet interface
// extend some functions to retrieve and compute an intersection with other Mariadb GTID Set.
type MariadbGTIDSet struct {
	set *mysql.MariadbGTIDSet
}

// Set implements Set.Set, replace g by other.
func (m *MariadbGTIDSet) Set(other mysql.GTIDSet) error {
	if other == nil {
		return nil
	}

	gs, ok := other.(*mysql.MariadbGTIDSet)
	if !ok {
		return terror.ErrNotMariaDBGTID.Generate(other)
	}

	m.set = gs
	return nil
}

// Replace implements Set.Replace.
func (m *MariadbGTIDSet) Replace(other Set, masters []interface{}) error {
	if other == nil {
		return nil
	}

	otherGS, ok := other.(*MariadbGTIDSet)
	if !ok {
		return terror.ErrNotMariaDBGTID.Generate(other)
	}

	for _, id := range masters {
		domainID, ok := id.(uint32)
		if !ok {
			return terror.ErrMariaDBDomainID.Generate(id)
		}

		otherGS.delete(domainID)
		if uuidSet, ok := m.get(domainID); ok {
			err := otherGS.set.AddSet(uuidSet)
			if err != nil {
				return terror.ErrMariaDBDomainID.AnnotateDelegate(err, "fail to add UUID set for domain %d", domainID)
			}
		}
	}

	for id, set := range m.set.Sets {
		if _, ok := otherGS.get(id); ok {
			otherGS.delete(id)
			err := otherGS.set.AddSet(set)
			if err != nil {
				return terror.ErrMariaDBDomainID.AnnotateDelegate(err, "fail to add UUID set for domain %d", id)
			}
		}
	}

	m.set = otherGS.set.Clone().(*mysql.MariadbGTIDSet)
	return nil
}

func (m *MariadbGTIDSet) delete(domainID uint32) {
	delete(m.set.Sets, domainID)
}

func (m *MariadbGTIDSet) get(domainID uint32) (*mysql.MariadbGTID, bool) {
	gtid, ok := m.set.Sets[domainID]
	return gtid, ok
}

// Clone implements Set.Clone.
func (m *MariadbGTIDSet) Clone() Set {
	if m.set == nil {
		return MinGTIDSet(mysql.MariaDBFlavor)
	}
	return &MariadbGTIDSet{
		set: m.set.Clone().(*mysql.MariadbGTIDSet),
	}
}

// Origin implements Set.Origin.
func (m *MariadbGTIDSet) Origin() mysql.GTIDSet {
	if m.set == nil {
		return &mysql.MariadbGTIDSet{}
	}
	return m.set.Clone().(*mysql.MariadbGTIDSet)
}

// Equal implements Set.Equal.
func (m *MariadbGTIDSet) Equal(other Set) bool {
	otherIsNil := other == nil
	if !otherIsNil {
		otherGS, ok := other.(*MariadbGTIDSet)
		if !ok {
			return false
		}
		otherIsNil = otherGS == nil
	}

	if m == nil && otherIsNil {
		return true
	} else if m == nil || otherIsNil {
		return false
	}

	return m.set.Equal(other.Origin())
}

// Contain implements Set.Contain.
func (m *MariadbGTIDSet) Contain(other Set) bool {
	otherIsNil := other == nil
	if !otherIsNil {
		otherGS, ok := other.(*MariadbGTIDSet)
		if !ok {
			return false
		}
		otherIsNil = otherGS == nil
	}
	if otherIsNil {
		return true // any set (including nil) contains nil
	} else if m == nil {
		return false // nil only contains nil
	}
	return m.set.Contain(other.Origin())
}

// Truncate implements Set.Truncate.
func (m *MariadbGTIDSet) Truncate(end Set) error {
	if end == nil {
		return nil // do nothing
	}
	if !m.Contain(end) {
		return terror.ErrGTIDTruncateInvalid.Generate(m, end)
	}
	endGs := end.(*MariadbGTIDSet) // already verify the type is `*MariadbGTIDSet` in `Contain`.
	if endGs == nil {
		return nil // do nothing
	}

	for did, mGTID := range m.set.Sets {
		eGTID, ok := endGs.set.Sets[did]
		if !ok {
			continue // no need to truncate for this domain ID
		}
		if mGTID.SequenceNumber > eGTID.SequenceNumber {
			mGTID.SequenceNumber = eGTID.SequenceNumber // truncate the seqNO
			mGTID.ServerID = eGTID.ServerID             // also update server-id to match the seqNO
		}
	}

	return nil
}

func (m *MariadbGTIDSet) String() string {
	if m.set == nil {
		return ""
	}
	return m.set.String()
}
