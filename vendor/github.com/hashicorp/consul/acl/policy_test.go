package acl

import (
	"reflect"
	"strings"
	"testing"
)

func TestACLPolicy_Parse_HCL(t *testing.T) {
	inp := `
agent "foo" {
	policy = "read"
}
agent "bar" {
	policy = "write"
}
event "" {
	policy = "read"
}
event "foo" {
	policy = "write"
}
event "bar" {
	policy = "deny"
}
key "" {
	policy = "read"
}
key "foo/" {
	policy = "write"
}
key "foo/bar/" {
	policy = "read"
}
key "foo/bar/baz" {
	policy = "deny"
}
keyring = "deny"
node "" {
	policy = "read"
}
node "foo" {
	policy = "write"
}
node "bar" {
	policy = "deny"
}
operator = "deny"
service "" {
	policy = "write"
}
service "foo" {
	policy = "read"
}
session "foo" {
	policy = "write"
}
session "bar" {
	policy = "deny"
}
query "" {
	policy = "read"
}
query "foo" {
	policy = "write"
}
query "bar" {
	policy = "deny"
}
	`
	exp := &Policy{
		Agents: []*AgentPolicy{
			{
				Node:   "foo",
				Policy: PolicyRead,
			},
			{
				Node:   "bar",
				Policy: PolicyWrite,
			},
		},
		Events: []*EventPolicy{
			{
				Event:  "",
				Policy: PolicyRead,
			},
			{
				Event:  "foo",
				Policy: PolicyWrite,
			},
			{
				Event:  "bar",
				Policy: PolicyDeny,
			},
		},
		Keyring: PolicyDeny,
		Keys: []*KeyPolicy{
			{
				Prefix: "",
				Policy: PolicyRead,
			},
			{
				Prefix: "foo/",
				Policy: PolicyWrite,
			},
			{
				Prefix: "foo/bar/",
				Policy: PolicyRead,
			},
			{
				Prefix: "foo/bar/baz",
				Policy: PolicyDeny,
			},
		},
		Nodes: []*NodePolicy{
			{
				Name:   "",
				Policy: PolicyRead,
			},
			{
				Name:   "foo",
				Policy: PolicyWrite,
			},
			{
				Name:   "bar",
				Policy: PolicyDeny,
			},
		},
		Operator: PolicyDeny,
		PreparedQueries: []*PreparedQueryPolicy{
			{
				Prefix: "",
				Policy: PolicyRead,
			},
			{
				Prefix: "foo",
				Policy: PolicyWrite,
			},
			{
				Prefix: "bar",
				Policy: PolicyDeny,
			},
		},
		Services: []*ServicePolicy{
			{
				Name:   "",
				Policy: PolicyWrite,
			},
			{
				Name:   "foo",
				Policy: PolicyRead,
			},
		},
		Sessions: []*SessionPolicy{
			{
				Node:   "foo",
				Policy: PolicyWrite,
			},
			{
				Node:   "bar",
				Policy: PolicyDeny,
			},
		},
	}

	out, err := Parse(inp)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if !reflect.DeepEqual(out, exp) {
		t.Fatalf("bad: %#v %#v", out, exp)
	}
}

func TestACLPolicy_Parse_JSON(t *testing.T) {
	inp := `{
	"agent": {
		"foo": {
			"policy": "write"
		},
		"bar": {
			"policy": "deny"
		}
	},
	"event": {
		"": {
			"policy": "read"
		},
		"foo": {
			"policy": "write"
		},
		"bar": {
			"policy": "deny"
		}
	},
	"key": {
		"": {
			"policy": "read"
		},
		"foo/": {
			"policy": "write"
		},
		"foo/bar/": {
			"policy": "read"
		},
		"foo/bar/baz": {
			"policy": "deny"
		}
	},
	"keyring": "deny",
	"node": {
		"": {
			"policy": "read"
		},
		"foo": {
			"policy": "write"
		},
		"bar": {
			"policy": "deny"
		}
	},
	"operator": "deny",
	"query": {
		"": {
			"policy": "read"
		},
		"foo": {
			"policy": "write"
		},
		"bar": {
			"policy": "deny"
		}
	},
	"service": {
		"": {
			"policy": "write"
		},
		"foo": {
			"policy": "read"
		}
	},
	"session": {
		"foo": {
			"policy": "write"
		},
		"bar": {
			"policy": "deny"
		}
	}
}`
	exp := &Policy{
		Agents: []*AgentPolicy{
			{
				Node:   "foo",
				Policy: PolicyWrite,
			},
			{
				Node:   "bar",
				Policy: PolicyDeny,
			},
		},
		Events: []*EventPolicy{
			{
				Event:  "",
				Policy: PolicyRead,
			},
			{
				Event:  "foo",
				Policy: PolicyWrite,
			},
			{
				Event:  "bar",
				Policy: PolicyDeny,
			},
		},
		Keyring: PolicyDeny,
		Keys: []*KeyPolicy{
			{
				Prefix: "",
				Policy: PolicyRead,
			},
			{
				Prefix: "foo/",
				Policy: PolicyWrite,
			},
			{
				Prefix: "foo/bar/",
				Policy: PolicyRead,
			},
			{
				Prefix: "foo/bar/baz",
				Policy: PolicyDeny,
			},
		},
		Nodes: []*NodePolicy{
			{
				Name:   "",
				Policy: PolicyRead,
			},
			{
				Name:   "foo",
				Policy: PolicyWrite,
			},
			{
				Name:   "bar",
				Policy: PolicyDeny,
			},
		},
		Operator: PolicyDeny,
		PreparedQueries: []*PreparedQueryPolicy{
			{
				Prefix: "",
				Policy: PolicyRead,
			},
			{
				Prefix: "foo",
				Policy: PolicyWrite,
			},
			{
				Prefix: "bar",
				Policy: PolicyDeny,
			},
		},
		Services: []*ServicePolicy{
			{
				Name:   "",
				Policy: PolicyWrite,
			},
			{
				Name:   "foo",
				Policy: PolicyRead,
			},
		},
		Sessions: []*SessionPolicy{
			{
				Node:   "foo",
				Policy: PolicyWrite,
			},
			{
				Node:   "bar",
				Policy: PolicyDeny,
			},
		},
	}

	out, err := Parse(inp)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if !reflect.DeepEqual(out, exp) {
		t.Fatalf("bad: %#v %#v", out, exp)
	}
}

func TestACLPolicy_Keyring_Empty(t *testing.T) {
	inp := `
keyring = ""
	`
	exp := &Policy{
		Keyring: "",
	}

	out, err := Parse(inp)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if !reflect.DeepEqual(out, exp) {
		t.Fatalf("bad: %#v %#v", out, exp)
	}
}

func TestACLPolicy_Operator_Empty(t *testing.T) {
	inp := `
operator = ""
	`
	exp := &Policy{
		Operator: "",
	}

	out, err := Parse(inp)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if !reflect.DeepEqual(out, exp) {
		t.Fatalf("bad: %#v %#v", out, exp)
	}
}

func TestACLPolicy_Bad_Policy(t *testing.T) {
	cases := []string{
		`agent "" { policy = "nope" }`,
		`event "" { policy = "nope" }`,
		`key "" { policy = "nope" }`,
		`keyring = "nope"`,
		`node "" { policy = "nope" }`,
		`operator = "nope"`,
		`query "" { policy = "nope" }`,
		`service "" { policy = "nope" }`,
		`session "" { policy = "nope" }`,
	}
	for _, c := range cases {
		_, err := Parse(c)
		if err == nil || !strings.Contains(err.Error(), "Invalid") {
			t.Fatalf("expected policy error, got: %#v", err)
		}
	}
}
