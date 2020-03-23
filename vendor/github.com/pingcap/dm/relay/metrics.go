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

package relay

import (
	"time"

	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/pingcap/dm/pkg/utils"
)

var (
	relayLogPosGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "relay",
			Name:      "binlog_pos",
			Help:      "current binlog pos in current binlog file",
		}, []string{"node"})

	relayLogFileGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "relay",
			Name:      "binlog_file",
			Help:      "current binlog file index",
		}, []string{"node"})

	// split sub directory info from relayLogPosGauge / relayLogFileGauge
	// to make compare relayLogFileGauge for master / relay more easier
	relaySubDirIndex = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "relay",
			Name:      "sub_dir_index",
			Help:      "current relay sub directory index",
		}, []string{"node", "uuid"})

	// should alert if avaiable space < 10G
	relayLogSpaceGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dm",
			Subsystem: "relay",
			Name:      "space",
			Help:      "the space of storge for relay component",
		}, []string{"type"}) // type can be 'capacity' and 'available'.

	// should alert
	relayLogDataCorruptionCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "relay",
			Name:      "data_corruption",
			Help:      "counter of relay log data corruption",
		})

	relayLogWriteSizeHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "relay",
			Name:      "write_size",
			Help:      "write relay log size",
			Buckets:   prometheus.ExponentialBuckets(16, 2, 20),
		})

	relayLogWriteDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "relay",
			Name:      "write_duration",
			Help:      "bucketed histogram of write time (s) of single relay log event",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 18),
		})

	// should alert
	relayLogWriteErrorCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "relay",
			Name:      "write_error_count",
			Help:      "write relay log error count",
		})

	// should alert
	binlogReadErrorCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "relay",
			Name:      "read_error_count",
			Help:      "read binlog from master error count",
		})

	binlogReadDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "dm",
			Subsystem: "relay",
			Name:      "read_binlog_duration",
			Help:      "bucketed histogram of read time (s) of single binlog event from the master.",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 18),
		})

	// should alert
	relayExitWithErrorCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "dm",
			Subsystem: "relay",
			Name:      "exit_with_error_count",
			Help:      "counter of relay unit exits with error",
		})
)

// RegisterMetrics register metrics.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(relayLogPosGauge)
	registry.MustRegister(relayLogFileGauge)
	registry.MustRegister(relaySubDirIndex)
	registry.MustRegister(relayLogSpaceGauge)
	registry.MustRegister(relayLogDataCorruptionCounter)
	registry.MustRegister(relayLogWriteSizeHistogram)
	registry.MustRegister(relayLogWriteDurationHistogram)
	registry.MustRegister(relayLogWriteErrorCounter)
	registry.MustRegister(binlogReadErrorCounter)
	registry.MustRegister(binlogReadDurationHistogram)
	registry.MustRegister(relayExitWithErrorCounter)
}

func reportRelayLogSpaceInBackground(dirpath string, shutdown chan struct{}) error {
	if len(dirpath) == 0 {
		return errors.New("dirpath is empty")
	}

	go func() {
		ticker := time.NewTicker(time.Second * 10)
		defer ticker.Stop()

		for range ticker.C {
			select {
			case <-shutdown:
				log.Infof("reportRelayLogSpaceInBackground: shutdown")
				return
			default:
			}

			size, err := utils.GetStorageSize(dirpath)
			if err != nil {
				log.Error("update storage size err: ", err)
			} else {
				relayLogSpaceGauge.WithLabelValues("capacity").Set(float64(size.Capacity))
				relayLogSpaceGauge.WithLabelValues("available").Set(float64(size.Available))
			}
		}
	}()

	return nil
}
