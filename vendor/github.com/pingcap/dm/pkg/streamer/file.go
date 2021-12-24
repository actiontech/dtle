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
	"path"
	"path/filepath"
	"sort"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/failpoint"
	"go.uber.org/zap"

	"github.com/pingcap/dm/pkg/binlog"
	"github.com/pingcap/dm/pkg/log"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/pingcap/dm/pkg/utils"
)

// FileCmp is a compare condition used when collecting binlog files.
type FileCmp uint8

// FileCmpLess represents a < FileCmp condition, others are similar.
const (
	FileCmpLess FileCmp = iota + 1
	FileCmpLessEqual
	FileCmpEqual
	FileCmpBiggerEqual
	FileCmpBigger
)

// SwitchPath represents next binlog file path which should be switched.
type SwitchPath struct {
	nextUUID       string
	nextBinlogName string
}

// CollectAllBinlogFiles collects all valid binlog files in dir, and returns filenames in binlog ascending order.
func CollectAllBinlogFiles(dir string) ([]string, error) {
	if dir == "" {
		return nil, terror.ErrEmptyRelayDir.Generate()
	}
	return readSortedBinlogFromDir(dir)
}

// CollectBinlogFilesCmp collects valid binlog files with a compare condition.
func CollectBinlogFilesCmp(dir, baseFile string, cmp FileCmp) ([]string, error) {
	if dir == "" {
		return nil, terror.ErrEmptyRelayDir.Generate()
	}

	if bp := filepath.Join(dir, baseFile); !utils.IsFileExists(bp) {
		return nil, terror.ErrBaseFileNotFound.Generate(baseFile, dir)
	}

	bf, err := binlog.ParseFilename(baseFile)
	if err != nil {
		return nil, terror.Annotatef(err, "filename %s", baseFile)
	}

	allFiles, err := CollectAllBinlogFiles(dir)
	if err != nil {
		return nil, err
	}

	results := make([]string, 0, len(allFiles))
	for _, f := range allFiles {
		// we have parse f in `CollectAllBinlogFiles`, may be we can refine this
		parsed, err := binlog.ParseFilename(f)
		if err != nil || parsed.BaseName != bf.BaseName {
			log.L().Warn("collecting binlog file, ignore invalid file", zap.String("file", f), log.ShortError(err))
			continue
		}
		switch cmp {
		case FileCmpBigger:
			if !parsed.GreaterThan(bf) {
				log.L().Debug("ignore older or equal binlog file", zap.String("file", f), zap.String("directory", dir))
				continue
			}
		case FileCmpBiggerEqual:
			if !parsed.GreaterThanOrEqualTo(bf) {
				log.L().Debug("ignore older binlog file", zap.String("file", f), zap.String("directory", dir))
				continue
			}
		case FileCmpLess:
			if !parsed.LessThan(bf) {
				log.L().Debug("ignore newer or equal binlog file", zap.String("file", f), zap.String("directory", dir))
				continue
			}
		default:
			return nil, terror.ErrBinFileCmpCondNotSupport.Generate(cmp)
		}

		results = append(results, f)
	}

	return results, nil
}

// getFirstBinlogName gets the first binlog file in relay sub directory.
func getFirstBinlogName(baseDir, uuid string) (string, error) {
	subDir := filepath.Join(baseDir, uuid)
	files, err := readSortedBinlogFromDir(subDir)
	if err != nil {
		return "", terror.Annotatef(err, "get binlog file for dir %s", subDir)
	}

	if len(files) == 0 {
		return "", terror.ErrBinlogFilesNotFound.Generate(subDir)
	}
	return files[0], nil
}

// readSortedBinlogFromDir reads and returns all binlog files (sorted ascending by binlog filename and sequence number).
func readSortedBinlogFromDir(dirpath string) ([]string, error) {
	dir, err := os.Open(dirpath)
	if err != nil {
		return nil, terror.ErrReadDir.Delegate(err, dirpath)
	}
	defer dir.Close()

	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, terror.ErrReadDir.Delegate(err, dirpath)
	}
	if len(names) == 0 {
		return nil, nil
	}

	// sorting bin.100000, ..., bin.1000000, ..., bin.999999
	type tuple struct {
		filename string
		parsed   binlog.Filename
	}
	tmp := make([]tuple, 0, len(names)-1)

	for _, f := range names {
		p, err2 := binlog.ParseFilename(f)
		if err2 != nil {
			// may contain some file that can't be parsed, like relay meta. ignore them
			log.L().Info("collecting binlog file, ignore invalid file", zap.String("file", f))
			continue
		}
		tmp = append(tmp, tuple{
			filename: f,
			parsed:   p,
		})
	}

	sort.Slice(tmp, func(i, j int) bool {
		if tmp[i].parsed.BaseName != tmp[j].parsed.BaseName {
			return tmp[i].parsed.BaseName < tmp[j].parsed.BaseName
		}
		return tmp[i].parsed.LessThan(tmp[j].parsed)
	})

	ret := make([]string, len(tmp))
	for i := range tmp {
		ret[i] = tmp[i].filename
	}

	return ret, nil
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
		return 0, terror.ErrGetRelayLogStat.Delegate(err, path)
	}
	curSize := fi.Size()
	switch {
	case curSize == latestSize:
		return 0, nil
	case curSize > latestSize:
		log.L().Debug("size of relay log file has been changed", zap.String("file", path),
			zap.Int64("old size", latestSize), zap.Int64("size", curSize))
		return 1, nil
	default:
		log.L().Error("size of relay log file has been changed", zap.String("file", path),
			zap.Int64("old size", latestSize), zap.Int64("size", curSize))
		return -1, nil
	}
}

// relayLogUpdatedOrNewCreated checks whether current relay log file is updated or new relay log is created.
// we check the size of the file first, if the size is the same as the latest file, we assume there is no new write
// so we need to check relay meta file to see if the new relay log is created.
// this func will be blocked until current filesize changed or meta file updated or context cancelled.
// we need to make sure that only one channel (updatePathCh or errCh) has events written to it.
func relayLogUpdatedOrNewCreated(ctx context.Context, watcherInterval time.Duration, dir string,
	latestFilePath, latestFile string, latestFileSize int64, updatePathCh chan string, errCh chan error) {
	ticker := time.NewTicker(watcherInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			errCh <- terror.Annotate(ctx.Err(), "context meet error")
			return
		case <-ticker.C:
			cmp, err := fileSizeUpdated(latestFilePath, latestFileSize)
			if err != nil {
				errCh <- terror.Annotatef(err, "latestFilePath=%s latestFileSize=%d", latestFilePath, latestFileSize)
				return
			}
			failpoint.Inject("CMPAlwaysReturn0", func() {
				cmp = 0
			})
			switch {
			case cmp < 0:
				errCh <- terror.ErrRelayLogFileSizeSmaller.Generate(latestFilePath)
				return
			case cmp > 0:
				updatePathCh <- latestFilePath
				return
			default:
				// current watched file size have no change means that no new writes have been made
				// our relay meta file will be updated immediately after receive the rotate event
				// although we cannot ensure that the binlog filename in the meta is the next file after latestFile
				// but if we return a different filename with latestFile, the outer logic (parseDirAsPossible)
				// will find the right one
				meta := &Meta{}
				_, err = toml.DecodeFile(filepath.Join(dir, utils.MetaFilename), meta)
				if err != nil {
					errCh <- terror.Annotate(err, "decode relay meta toml file failed")
					return
				}
				if meta.BinLogName != latestFile {
					// we need check file size again, as the file may have been changed during our metafile check
					cmp, err := fileSizeUpdated(latestFilePath, latestFileSize)
					if err != nil {
						errCh <- terror.Annotatef(err, "latestFilePath=%s latestFileSize=%d",
							latestFilePath, latestFileSize)
						return
					}
					switch {
					case cmp < 0:
						errCh <- terror.ErrRelayLogFileSizeSmaller.Generate(latestFilePath)
					case cmp > 0:
						updatePathCh <- latestFilePath
					default:
						nextFilePath := filepath.Join(dir, meta.BinLogName)
						log.L().Info("newer relay log file is already generated",
							zap.String("now file path", latestFilePath),
							zap.String("new file path", nextFilePath))
						updatePathCh <- nextFilePath
					}
					return
				}
			}
		}
	}
}

// needSwitchSubDir checks whether the reader need to switch to next relay sub directory.
func needSwitchSubDir(ctx context.Context, relayDir, currentUUID string, switchCh chan SwitchPath, errCh chan error) {
	var (
		err            error
		nextUUID       string
		nextBinlogName string
		uuids          []string
	)

	ticker := time.NewTicker(watcherInterval)
	defer func() {
		ticker.Stop()
		if err != nil {
			errCh <- err
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// reload uuid
			uuids, err = utils.ParseUUIDIndex(path.Join(relayDir, utils.UUIDIndexFilename))
			if err != nil {
				return
			}
			nextUUID, _, err = getNextUUID(currentUUID, uuids)
			if err != nil {
				return
			}
			if len(nextUUID) == 0 {
				continue
			}

			// try get the first binlog file in next sub directory
			nextBinlogName, err = getFirstBinlogName(relayDir, nextUUID)
			if err != nil {
				// because creating sub directory and writing relay log file are not atomic
				// just continue to observe subdir
				if terror.ErrBinlogFilesNotFound.Equal(err) {
					err = nil
					continue
				}
				return
			}

			switchCh <- SwitchPath{nextUUID, nextBinlogName}
			return
		}
	}
}
