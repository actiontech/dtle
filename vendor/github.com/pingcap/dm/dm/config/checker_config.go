// Copyright 2021 PingCAP, Inc.
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

package config

import (
	"encoding/json"
	"time"
)

// Backoff related constants.
var (
	DefaultCheckInterval           = 5 * time.Second
	DefaultBackoffRollback         = 5 * time.Minute
	DefaultBackoffMin              = 1 * time.Second
	DefaultBackoffMax              = 5 * time.Minute
	DefaultBackoffJitter           = true
	DefaultBackoffFactor   float64 = 2
)

// Duration is used to hold a time.Duration field.
type Duration struct {
	time.Duration
}

// MarshalText hacks to satisfy the encoding.TextMarshaler interface
// For MarshalText, we should use (d Duration) which can be used by both pointer and instance.
func (d Duration) MarshalText() ([]byte, error) {
	return []byte(d.Duration.String()), nil
}

// UnmarshalText hacks to satisfy the encoding.TextUnmarshaler interface
// For UnmarshalText, we should use (d *Duration) to change the value of this instance instead of the copy.
func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

// MarshalJSON hacks to satisfy the json.Marshaler interface.
func (d *Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Duration string `json:"Duration"`
	}{
		d.Duration.String(),
	})
}

// CheckerConfig is configuration used for TaskStatusChecker.
type CheckerConfig struct {
	CheckEnable     bool     `yaml:"check-enable" toml:"check-enable" json:"check-enable"`
	BackoffRollback Duration `yaml:"backoff-rollback" toml:"backoff-rollback" json:"backoff-rollback"`
	BackoffMax      Duration `yaml:"backoff-max" toml:"backoff-max" json:"backoff-max"`
	// unexpose config
	CheckInterval Duration `yaml:"check-interval" toml:"check-interval" json:"-"`
	BackoffMin    Duration `yaml:"backoff-min" toml:"backoff-min" json:"-"`
	BackoffJitter bool     `yaml:"backoff-jitter" toml:"backoff-jitter" json:"-"`
	BackoffFactor float64  `yaml:"backoff-factor" toml:"backoff-factor" json:"-"`
}

// Adjust sets default value for field: CheckInterval/BackoffMin/BackoffJitter/BackoffFactor.
func (cc *CheckerConfig) Adjust() {
	cc.CheckInterval = Duration{Duration: DefaultCheckInterval}
	cc.BackoffMin = Duration{Duration: DefaultBackoffMin}
	cc.BackoffJitter = DefaultBackoffJitter
	cc.BackoffFactor = DefaultBackoffFactor
}
