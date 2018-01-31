package gtid

func GtidAdd(gtidDesc0, gtidDesc1 string) (string, error) {
	gtid, err := parseGtid(gtidDesc0 + "," + gtidDesc1)
	if nil != err {
		return "", err
	}
	return gtid.String(), nil
}

func GtidContain(gtidDesc0, gtidDesc1 string) (bool, error) {
	gtidMerged, err := parseGtid(gtidDesc0 + "," + gtidDesc1)
	if nil != err {
		return false, err
	}
	gtid0, err := parseGtid(gtidDesc0)
	if nil != err {
		return false, err
	}
	return gtidMerged.String() == gtid0.String(), nil
}

func GtidOverlap(gtidDesc0 string, gtidDesc1 string) (bool, error) {
	gtid0, err := parseGtid(gtidDesc0)
	if nil != err {
		return false, err
	}
	gtid1, err := parseGtid(gtidDesc1)
	if nil != err {
		return false, err
	}
	if "" == gtid0.String() && "" == gtid1.String() {
		return true, nil
	}
	sub, err := GtidSub(gtidDesc0, gtidDesc1)
	if nil != err {
		return false, err
	}
	return gtid0.String() != sub, nil
}

func GtidSub(gtidDesc0, gtidDesc1 string) (string, error) {
	gtid0, err := parseGtid(gtidDesc0)
	if nil != err {
		return "", err
	}
	gtid1, err := parseGtid(gtidDesc1)
	if nil != err {
		return "", err
	}

	ret := Gtid{}
	for _, uuidNumber0 := range gtid0.uuidNumbers {
		intv := uuidNumber0.intervals
		for _, uuidNumber1 := range gtid1.uuidNumbers {
			if uuidNumber0.uuid == uuidNumber1.uuid {
				intv = subIntervals(uuidNumber0.intervals, uuidNumber1.intervals)
			}
		}
		if len(intv) > 0 {
			ret.uuidNumbers = append(ret.uuidNumbers, tUuidNumber{uuidNumber0.uuid, intv})
		}
	}
	return ret.String(), nil
}

func GtidEqual(gtidDesc0 string, gtidDesc1 string) (bool, error) {
	gtid0, err := parseGtid(gtidDesc0)
	if nil != err {
		return false, err
	}
	gtid1, err := parseGtid(gtidDesc1)
	if nil != err {
		return false, err
	}
	return gtid0.String() == gtid1.String(), nil
}
