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

package writer

// Writer is a binlog event writer, it may write binlog events to a binlog file, an in-memory structure or a TCP stream.
type Writer interface {
	// Start prepares the writer for writing binlog events.
	Start() error

	// Close closes the writer and release the resource.
	Close() error

	// Write writes/appends a binlog event's rawData.
	Write(rawData []byte) error

	// Flush flushes the buffered data to a stable storage or sends through the network.
	Flush() error

	// Status returns the status of the writer.
	Status() interface{}
}
