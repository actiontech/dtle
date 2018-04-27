package gtid

import (
	"testing"
)

func TestGtidAdd(t *testing.T) {
	gtid, err := GtidAdd(
		"ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:5-10,ff92c4da-c5d7-11e3-8cf7-5e10e6a05cfb:7",
		"ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:1-4,ff92c4da-c5d7-11e3-8cf7-5e10e6a05cfb:1-5")
	if nil != err {
		t.Fatalf("unexpected error %v", err)
	}
	if gtid != "ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:1-10,ff92c4da-c5d7-11e3-8cf7-5e10e6a05cfb:1-5:7" {
		t.Fatalf("wrong gtid %v", gtid)
	}
}

func TestGtidContain_pass(t *testing.T) {
	contain, err := GtidContain(
		"ff92c4da-c5d7-11e3-8cf7-5e10e6a05cfb:1-7,ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:1-10",
		"ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:1-4,ff92c4da-c5d7-11e3-8cf7-5e10e6a05cfb:1-5")
	if nil != err {
		t.Fatalf("unexpected error %v", err)
	}
	if !contain {
		t.Fatalf("contain should == true")
	}
}

func TestGtidContain_not_pass(t *testing.T) {
	contain, err := GtidContain(
		"ff92c4da-c5d7-11e3-8cf7-5e10e6a05cfb:1-7,ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:2-10",
		"ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:1-4,ff92c4da-c5d7-11e3-8cf7-5e10e6a05cfb:1-5")
	if nil != err {
		t.Fatalf("unexpected error %v", err)
	}
	if contain {
		t.Fatalf("contain should == false")
	}
}

func TestGtidContain_null(t *testing.T) {
	contain, err := GtidContain(
		"",
		"")
	if nil != err {
		t.Fatalf("unexpected error %v", err)
	}
	if !contain {
		t.Fatalf("contain should == true")
	}
}

func TestGtidSub_1(t *testing.T) {
	gtid, err := GtidSub(
		"ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:1-10:20-30",
		"ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:11-12")
	if nil != err {
		t.Fatalf("unexpected error %v", err)
	}
	if gtid != "ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:1-10:20-30" {
		t.Fatalf("wrong gtid %v", gtid)
	}
}

func TestGtidSub_2(t *testing.T) {
	gtid, err := GtidSub(
		"ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:1-10:20-30",
		"ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:15-40")
	if nil != err {
		t.Fatalf("unexpected error %v", err)
	}
	if gtid != "ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:1-10" {
		t.Fatalf("wrong gtid %v", gtid)
	}
}

func TestGtidSub_3(t *testing.T) {
	gtid, err := GtidSub(
		"ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:1-10:20-30",
		"ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:15-25")
	if nil != err {
		t.Fatalf("unexpected error %v", err)
	}
	if gtid != "ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:1-10:26-30" {
		t.Fatalf("wrong gtid %v", gtid)
	}
}

func TestGtidSub_4(t *testing.T) {
	gtid, err := GtidSub(
		"ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:1-10:20-30",
		"ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:15-30")
	if nil != err {
		t.Fatalf("unexpected error %v", err)
	}
	if gtid != "ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:1-10" {
		t.Fatalf("wrong gtid %v", gtid)
	}
}

func TestGtidSub_5(t *testing.T) {
	gtid, err := GtidSub(
		"ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:1-10:20-30",
		"ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:2-29")
	if nil != err {
		t.Fatalf("unexpected error %v", err)
	}
	if gtid != "ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:1:30" {
		t.Fatalf("wrong gtid %v", gtid)
	}
}

func TestGtidSub_6(t *testing.T) {
	gtid, err := GtidSub(
		"ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:1-10:20-30",
		"ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:1-30")
	if nil != err {
		t.Fatalf("unexpected error %v", err)
	}
	if gtid != "" {
		t.Fatalf("wrong gtid %v", gtid)
	}
}

func TestGtidSub_7(t *testing.T) {
	gtid, err := GtidSub(
		"ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:1-10:20-30",
		"ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:5-6:25")
	if nil != err {
		t.Fatalf("unexpected error %v", err)
	}
	if gtid != "ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:1-4:7-10:20-24:26-30" {
		t.Fatalf("wrong gtid %v", gtid)
	}
}

func TestGtidSub_8(t *testing.T) {
	gtid, err := GtidSub(
		"ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:20-30, ff92c4da-c5d7-11e3-8cf7-5e10e6a05cfb:1-7, ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:1-10",
		"ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:1-9:25, ff92c4da-c5d7-11e3-8cf7-5e10e6a05cfb:6")
	if nil != err {
		t.Fatalf("unexpected error %v", err)
	}
	if gtid != "ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:10:20-24:26-30,ff92c4da-c5d7-11e3-8cf7-5e10e6a05cfb:1-5:7" {
		t.Fatalf("wrong gtid %v", gtid)
	}
}

func TestGtidSub_empty(t *testing.T) {
	gtid, err := GtidSub(
		"",
		"")
	if nil != err {
		t.Fatalf("unexpected error %v", err)
	}
	if gtid != "" {
		t.Fatalf("wrong gtid %v", gtid)
	}
}

func TestGtidOverlap_true(t *testing.T) {
	ret, err := GtidOverlap(
		"ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:10:20-24:26-30,ff92c4da-c5d7-11e3-8cf7-5e10e6a05cfb:1-5:7",
		"ff92c4da-c5d7-11e3-8cf7-5e10e6a05cfb:4")
	if nil != err {
		t.Fatalf("unexpected error %v", err)
	}
	if !ret {
		t.Fatalf("expect true, but got false")
	}
}

func TestGtidOverlap_false(t *testing.T) {
	ret, err := GtidOverlap(
		"ca8035ea-c5d5-11e3-8ce9-e66ccf50db66:10:20-24:26-30,ff92c4da-c5d7-11e3-8cf7-5e10e6a05cfb:1-5:7",
		"ff92c4da-c5d7-11e3-8cf7-5e10e6a05cfb:9")
	if nil != err {
		t.Fatalf("unexpected error %v", err)
	}
	if ret {
		t.Fatalf("expect false, but got true")
	}
}

func TestGtidOverlap_empty(t *testing.T) {
	ret, err := GtidOverlap("", "")
	if nil != err {
		t.Fatalf("unexpected error %v", err)
	}
	if !ret {
		t.Fatalf("expect true, but got false")
	}
}

func TestGtidEqual_true(t *testing.T) {
	ret, err := GtidOverlap("ff92c4da-c5d7-11e3-8cf7-5e10e6a05cfb:1-10", "ff92c4da-c5d7-11e3-8cf7-5e10e6a05cfb:1-10")
	if nil != err {
		t.Fatalf("unexpected error %v", err)
	}
	if !ret {
		t.Fatalf("expect true, but got false")
	}
}

func TestGtidEqual_false(t *testing.T) {
	ret, err := GtidOverlap("ff92c4da-c5d7-11e3-8cf7-5e10e6a05cfb:1-10", "ff92c4da-c5d7-11e3-8cf7-5e10e6a05cfb:1-11")
	if nil != err {
		t.Fatalf("unexpected error %v", err)
	}
	if !ret {
		t.Fatalf("expect true, but got false")
	}
}
