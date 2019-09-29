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

package streamer

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/watcher"

	"github.com/pingcap/dm/pkg/binlog"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/utils"
)

var (
	// ErrEmptyRelayDir means error about empty relay dir.
	ErrEmptyRelayDir = errors.New("empty relay dir")
)

// FileCmp is a compare condition used when collecting binlog files
type FileCmp uint8

// FileCmpLess represents a < FileCmp condition, others are similar
const (
	FileCmpLess FileCmp = iota + 1
	FileCmpLessEqual
	FileCmpEqual
	FileCmpBiggerEqual
	FileCmpBigger
)

// CollectAllBinlogFiles collects all valid binlog files in dir
func CollectAllBinlogFiles(dir string) ([]string, error) {
	if dir == "" {
		return nil, ErrEmptyRelayDir
	}
	files, err := ReadDir(dir)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ret := make([]string, 0, len(files))
	for _, f := range files {
		if strings.HasPrefix(f, utils.MetaFilename) {
			// skip meta file or temp meta file
			log.Debugf("[streamer] skip meta file %s", f)
			continue
		}
		if !binlog.VerifyFilename(f) {
			log.Warnf("[streamer] collecting binlog file, ignore invalid file %s", f)
			continue
		}
		ret = append(ret, f)
	}
	return ret, nil
}

// CollectBinlogFilesCmp collects valid binlog files with a compare condition
func CollectBinlogFilesCmp(dir, baseFile string, cmp FileCmp) ([]string, error) {
	if dir == "" {
		return nil, ErrEmptyRelayDir
	}

	if bp := filepath.Join(dir, baseFile); !utils.IsFileExists(bp) {
		return nil, errors.NotFoundf("base file %s in directory %s", baseFile, dir)
	}

	bf, err := binlog.ParseFilename(baseFile)
	if err != nil {
		return nil, errors.Annotatef(err, "filename %s", baseFile)
	}

	allFiles, err := CollectAllBinlogFiles(dir)
	if err != nil {
		return nil, errors.Trace(err)
	}

	results := make([]string, 0, len(allFiles))
	for _, f := range allFiles {
		// we have parse f in `CollectAllBinlogFiles`, may be we can refine this
		parsed, err := binlog.ParseFilename(f)
		if err != nil || parsed.BaseName != bf.BaseName {
			log.Warnf("[streamer] collecting binlog file, ignore invalid file %s, err %v", f, err)
			continue
		}
		switch cmp {
		case FileCmpBigger:
			if !parsed.GreaterThan(bf) {
				log.Debugf("[streamer] ignore older or equal binlog file %s in dir %s", f, dir)
				continue
			}
		case FileCmpBiggerEqual:
			if !parsed.GreaterThanOrEqualTo(bf) {
				log.Debugf("[streamer] ignore older binlog file %s in dir %s", f, dir)
				continue
			}
		case FileCmpLess:
			if !parsed.LessThan(bf) {
				log.Debugf("[streamer] ignore newer or equal binlog file %s in dir %s", f, dir)
				continue
			}
		default:
			return nil, errors.NotSupportedf("cmp condition %v", cmp)
		}

		results = append(results, f)
	}

	return results, nil
}

// getFirstBinlogName gets the first binlog file in relay sub directory
func getFirstBinlogName(baseDir, uuid string) (string, error) {
	subDir := filepath.Join(baseDir, uuid)
	files, err := ReadDir(subDir)
	if err != nil {
		return "", errors.Annotatef(err, "get binlog file for dir %s", subDir)
	}

	for _, f := range files {
		if f == utils.MetaFilename {
			log.Debugf("[streamer] skip meta file %s", f)
			continue
		}

		if !binlog.VerifyFilename(f) {
			return "", errors.NotValidf("binlog file %s", f)
		}
		return f, nil
	}

	return "", errors.NotFoundf("binlog files in dir %s", subDir)
}

// ReadDir reads and returns all file(sorted asc) and dir names from directory f
func ReadDir(dirpath string) ([]string, error) {
	dir, err := os.Open(dirpath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer dir.Close()

	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, errors.Annotatef(err, "dir %s", dirpath)
	}

	sort.Strings(names)

	return names, nil
}

// fileSizeUpdated checks whether the file's size has updated
// return
//   0: not updated
//   1: update to larger
//  -1: update to smaller, only happens in special case, for example we change
//      relay.meta manually and start task before relay log catches up.
func fileSizeUpdated(path string, latestSize int64) (int, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, errors.Annotatef(err, "get stat for relay log %s", path)
	}
	currSize := fi.Size()
	if currSize == latestSize {
		return 0, nil
	} else if currSize > latestSize {
		log.Debugf("[streamer] size of relay log file %s has changed from %d to %d", path, latestSize, currSize)
		return 1, nil
	} else {
		log.Errorf("[streamer] size of relay log file %s has changed from %d to %d", path, latestSize, currSize)
		return -1, nil
	}
}

// relaySubDirUpdated checks whether the relay sub directory updated
// including file changed, created, removed, etc.
func relaySubDirUpdated(ctx context.Context, watcherInterval time.Duration, dir string,
	latestFilePath, latestFile string, latestFileSize int64) (string, error) {
	// create polling watcher
	watcher2 := watcher.NewWatcher()

	// Add before Start
	// no need to Remove, it will be closed and release when return
	err := watcher2.Add(dir)
	if err != nil {
		return "", errors.Annotatef(err, "add watch for relay log dir %s", dir)
	}

	err = watcher2.Start(watcherInterval)
	if err != nil {
		return "", errors.Trace(err)
	}
	defer watcher2.Close()

	type watchResult struct {
		updatePath string
		err        error
	}

	result := make(chan watchResult, 1) // buffered chan to ensure not block the sender even return in the halfway
	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		for {
			select {
			case <-newCtx.Done():
				result <- watchResult{
					updatePath: "",
					err:        newCtx.Err(),
				}
				return
			case err2, ok := <-watcher2.Errors:
				if !ok {
					result <- watchResult{
						updatePath: "",
						err:        errors.Errorf("watcher's errors chan for relay log dir %s closed", dir),
					}
				} else {
					result <- watchResult{
						updatePath: "",
						err:        errors.Annotatef(err2, "relay log dir %s", dir),
					}
				}
				return
			case event, ok := <-watcher2.Events:
				if !ok {
					result <- watchResult{
						updatePath: "",
						err:        errors.Errorf("watcher's events chan for relay log dir %s closed", dir),
					}
					return
				}
				log.Debugf("[streamer] watcher receive event %+v", event)
				if event.IsDirEvent() {
					log.Debugf("[streamer] skip watcher event %+v for directory", event)
					continue
				} else if !event.HasOps(watcher.Modify, watcher.Create) {
					log.Debugf("[streamer] skip uninterested event op %s for file %s", event.Op, event.Path)
					continue
				}
				baseName := filepath.Base(event.Path)
				if !binlog.VerifyFilename(baseName) {
					log.Debugf("skip watcher event %+v for invalid relay log file", event)
					continue // not valid binlog created, updated
				}
				result <- watchResult{
					updatePath: event.Path,
					err:        nil,
				}
				return
			}
		}
	}()

	// try collect newer relay log file to check whether newer exists before watching
	newerFiles, err := CollectBinlogFilesCmp(dir, latestFile, FileCmpBigger)
	if err != nil {
		return "", errors.Annotatef(err, "collect newer files from %s in dir %s", latestFile, dir)
	}

	// check the latest relay log file whether updated when adding watching and collecting newer
	cmp, err := fileSizeUpdated(latestFilePath, latestFileSize)
	if err != nil {
		return "", errors.Trace(err)
	} else if cmp < 0 {
		return "", errors.Errorf("file size of relay log %s become smaller", latestFilePath)
	} else if cmp > 0 {
		// the latest relay log file already updated, need to parse from it again (not need to re-collect relay log files)
		return latestFilePath, nil
	} else if len(newerFiles) > 0 {
		// check whether newer relay log file exists
		nextFilePath := filepath.Join(dir, newerFiles[0])
		log.Infof("[streamer] newer relay log file %s already generated, start parse from it", nextFilePath)
		return nextFilePath, nil
	}

	res := <-result
	return res.updatePath, res.err
}

// needSwitchSubDir checks whether the reader need to switch to next relay sub directory
func needSwitchSubDir(relayDir, currentUUID, latestFilePath string, latestFileSize int64, UUIDs []string) (
	needSwitch, needReParse bool, nextUUID string, nextBinlogName string, err error) {
	nextUUID, _, err = getNextUUID(currentUUID, UUIDs)
	if err != nil {
		return false, false, "", "", errors.Annotatef(err, "current UUID %s, UUIDs %v", currentUUID, UUIDs)
	} else if len(nextUUID) == 0 {
		// no next sub dir exists, not need to switch
		return false, false, "", "", nil
	}

	// try get the first binlog file in next sub directory
	nextBinlogName, err = getFirstBinlogName(relayDir, nextUUID)
	if err != nil {
		// NOTE: current we can not handle `errors.IsNotFound(err)` easily
		// because creating sub directory and writing relay log file are not atomic
		// so we let user to pause syncing before switching relay's master server
		return false, false, "", "", errors.Trace(err)
	}

	// check the latest relay log file whether updated when checking next sub directory
	cmp, err := fileSizeUpdated(latestFilePath, latestFileSize)
	if err != nil {
		return false, false, "", "", errors.Trace(err)
	} else if cmp < 0 {
		return false, false, "", "", errors.Errorf("file size of relay log %s become smaller", latestFilePath)
	} else if cmp > 0 {
		// the latest relay log file already updated, need to parse from it again (not need to switch to sub directory)
		return false, true, "", "", nil
	}

	// need to switch to next sub directory
	return true, false, nextUUID, nextBinlogName, nil
}
