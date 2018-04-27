package gtid

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type Gtid struct {
	uuidNumbers []tUuidNumber
}

type tUuidNumber struct {
	uuid      string
	intervals []tInterval
}

type tInterval struct {
	from uint64
	to   uint64
}

type SortIntervals []tInterval

func (a SortIntervals) Len() int           { return len(a) }
func (a SortIntervals) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortIntervals) Less(i, j int) bool { return a[i].from < a[j].from }

func (g *Gtid) String() (ret string) {
	for _, uuidNumber := range g.uuidNumbers {
		s := uuidNumber.uuid
		for _, interval := range uuidNumber.intervals {
			if interval.from == interval.to {
				s = s + ":" + strconv.FormatUint(interval.from, 10)
			} else {
				s = s + ":" + strconv.FormatUint(interval.from, 10) + "-" + strconv.FormatUint(interval.to, 10)
			}
		}
		if "" != ret {
			ret = ret + ","
		}
		ret = ret + s
	}
	return ret
}

func parseGtid(gtidDesc string) (Gtid, error) {
	gtidDesc = strings.TrimSpace(gtidDesc)
	if "" == gtidDesc {
		return Gtid{}, nil
	}

	gtidDesc, err := UniformGtid(gtidDesc)
	if nil != err {
		return Gtid{}, err
	}

	ret := Gtid{}
	for _, uuidNumberDesc := range strings.Split(gtidDesc, ",") {
		uuidNumber, err := parseUuidNumber(uuidNumberDesc)
		if nil != err {
			return Gtid{}, err
		}
		ret.uuidNumbers = append(ret.uuidNumbers, uuidNumber)
	}
	return ret, nil
}

func uniformUuid(uuidDesc string) (string, error) {
	uuidDesc = strings.ToLower(strings.Replace(uuidDesc, "-", "", -1))
	if 32 != len(uuidDesc) {
		return "", fmt.Errorf("invalid uuid (%v)", uuidDesc)
	}
	uuidDesc = uuidDesc[0:8] + "-" + uuidDesc[8:12] + "-" + uuidDesc[12:16] + "-" + uuidDesc[16:20] + "-" + uuidDesc[20:]

	return uuidDesc, nil
}

func UniformGtid(gtidDesc string) (string, error) {
	gtidDesc = strings.TrimSpace(gtidDesc)
	hash := make(map[string]string)
	keys := make([]string, 0)
	for _, uuidNumberDesc := range strings.Split(gtidDesc, ",") {
		uuidNumberDesc = strings.TrimSpace(uuidNumberDesc)

		if "" == uuidNumberDesc {
			continue
		}

		splits := strings.SplitN(uuidNumberDesc, ":", 2)
		if len(splits) < 2 {
			return "", fmt.Errorf("invalid format (%v)", uuidNumberDesc)
		}

		uuid, err := uniformUuid(splits[0])
		if nil != err {
			return "", err
		}

		if "" != hash[uuid] {
			hash[uuid] = hash[uuid] + ":" + splits[1]
		} else {
			hash[uuid] = splits[1]
			keys = append(keys, uuid)
		}
	}
	sort.Strings(keys)
	ret := ""
	for _, uuid := range keys {
		if "" != ret {
			ret = ret + ","
		}
		ret = ret + uuid + ":" + hash[uuid]
	}
	return ret, nil
}

func parseUuidNumber(uuidNumberDesc string) (tUuidNumber, error) {
	uuidNumberDesc = strings.TrimSpace(uuidNumberDesc)
	splits := strings.Split(uuidNumberDesc, ":")

	ret := tUuidNumber{}
	ret.uuid = splits[0]

	intervals := make([]tInterval, 0)
	for i := 1; i < len(splits); i++ {
		numberDesc := splits[i]
		number, err := parseInterval(numberDesc)
		if nil != err {
			return tUuidNumber{}, err
		}
		intervals = append(intervals, number)
	}
	ret.intervals = uniformIntervals(intervals)
	return ret, nil
}

func parseInterval(intervalDesc string) (tInterval, error) {
	intervalDesc = strings.TrimSpace(intervalDesc)
	ret := tInterval{}
	if splitPos := strings.Index(intervalDesc, "-"); -1 != splitPos {
		firstPart := string(intervalDesc[0:splitPos])
		if i64, err := strconv.ParseUint(firstPart, 10, 64); nil == err {
			ret.from = i64
		} else {
			return tInterval{}, fmt.Errorf("invalid number %v", firstPart)
		}

		secondPart := string(intervalDesc[splitPos+1:])
		if i64, err := strconv.ParseUint(secondPart, 10, 64); nil == err {
			ret.to = i64
		} else {
			return tInterval{}, fmt.Errorf("invalid number %v", secondPart)
		}

	} else {
		if i64, err := strconv.ParseUint(intervalDesc, 10, 64); nil == err {
			ret.from = i64
			ret.to = i64
		} else {
			return tInterval{}, fmt.Errorf("invalid number %v", intervalDesc)
		}
	}
	return ret, nil
}

func uniformIntervals(intervals []tInterval) []tInterval {
	sort.Sort(SortIntervals(intervals))
	ret := make([]tInterval, 0)
	var p *tInterval = nil

	for _, intv := range intervals {
		if nil != p && intv.from <= p.to+1 {
			if intv.to >= p.to {
				p.to = intv.to
			}
			continue
		}

		ret = append(ret, intv)
		p = &ret[len(ret)-1]
	}
	return ret
}

func subIntervals(as, bs []tInterval) []tInterval {
	ret := make([]tInterval, 0)
	nexts := as
	nexti := 0
	for nexti < len(nexts) {
		current := nexts[nexti]
		nexti++
		for _, b := range bs {
			if b.to < current.from {
				continue
			}
			if b.from <= current.from {
				if b.to >= current.to {
					current = tInterval{2, 1}
					break
				} else {
					current.from = b.to + 1
					continue
				}
			}
			if b.from <= current.to {
				if b.to >= current.to {
					current.to = b.from - 1
					continue
				} else {
					nexts = append(nexts, tInterval{b.to + 1, current.to})
					current.to = b.from - 1
				}
			}
		}
		if current.from <= current.to {
			ret = append(ret, current)
		}
	}
	return uniformIntervals(ret)
}

func GtidEventCount(gtidDesc string) (uint64, error) {
	gtid, err := parseGtid(gtidDesc)
	if nil != err {
		return 0, err
	}
	var ret uint64 = 0
	for _, uuidNumber := range gtid.uuidNumbers {
		for _, interval := range uuidNumber.intervals {
			ret += interval.to - interval.from + 1
		}
	}
	return ret, nil
}
