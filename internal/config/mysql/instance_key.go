/*
 * Copyright (C) 2016-2018. ActionTech.
 * Based on: github.com/hashicorp/nomad, github.com/github/gh-ost .
 * License: MPL version 2: https://www.mozilla.org/en-US/MPL/2.0 .
 */

package mysql

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	DefaultInstancePort = 3306
)

// InstanceKey is an instance indicator, identifued by hostname and port
type InstanceKey struct {
	Host string
	Port int
}

const detachHint = "//"

// ParseInstanceKey will parse an InstanceKey from a string representation such as 127.0.0.1:3306
func NewRawInstanceKey(hostPort string) (*InstanceKey, error) {
	tokens := strings.SplitN(hostPort, ":", 2)
	if len(tokens) != 2 {
		return nil, fmt.Errorf("Cannot parse InstanceKey from %s. Expected format is host:port", hostPort)
	}
	instanceKey := &InstanceKey{Host: tokens[0]}
	var err error
	if instanceKey.Port, err = strconv.Atoi(tokens[1]); err != nil {
		return instanceKey, fmt.Errorf("Invalid port: %s", tokens[1])
	}

	return instanceKey, nil
}

// ParseRawInstanceKeyLoose will parse an InstanceKey from a string representation such as 127.0.0.1:3306.
// The port part is optional; there will be no name resolve
func ParseRawInstanceKeyLoose(hostPort string) (*InstanceKey, error) {
	if !strings.Contains(hostPort, ":") {
		return &InstanceKey{Host: hostPort, Port: DefaultInstancePort}, nil
	}
	return NewRawInstanceKey(hostPort)
}

// Equals tests equality between this key and another key
func (i *InstanceKey) Equals(other *InstanceKey) bool {
	if other == nil {
		return false
	}
	return i.Host == other.Host && i.Port == other.Port
}

// SmallerThan returns true if this key is dictionary-smaller than another.
// This is used for consistent sorting/ordering; there's nothing magical about it.
func (i *InstanceKey) SmallerThan(other *InstanceKey) bool {
	if i.Host < other.Host {
		return true
	}
	if i.Host == other.Host && i.Port < other.Port {
		return true
	}
	return false
}

// IsDetached returns 'true' when this hostname is logically "detached"
func (i *InstanceKey) IsDetached() bool {
	return strings.HasPrefix(i.Host, detachHint)
}

// IsValid uses simple heuristics to see whether this key represents an actual instance
func (i *InstanceKey) IsValid() bool {
	if i.Host == "_" {
		return false
	}
	if i.IsDetached() {
		return false
	}
	return len(i.Host) > 0 && i.Port > 0
}

// DetachedKey returns an instance key whose hostname is detahced: invalid, but recoverable
func (i *InstanceKey) DetachedKey() *InstanceKey {
	if i.IsDetached() {
		return i
	}
	return &InstanceKey{Host: fmt.Sprintf("%s%s", detachHint, i.Host), Port: i.Port}
}

// ReattachedKey returns an instance key whose hostname is detahced: invalid, but recoverable
func (i *InstanceKey) ReattachedKey() *InstanceKey {
	if !i.IsDetached() {
		return i
	}
	return &InstanceKey{Host: i.Host[len(detachHint):], Port: i.Port}
}

// StringCode returns an official string representation of this key
func (i *InstanceKey) StringCode() string {
	return fmt.Sprintf("%s:%d", i.Host, i.Port)
}

// DisplayString returns a user-friendly string representation of this key
func (i *InstanceKey) DisplayString() string {
	return i.StringCode()
}

// String returns a user-friendly string representation of this key
func (i InstanceKey) String() string {
	return i.StringCode()
}
