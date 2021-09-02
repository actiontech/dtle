package pkg

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
)

type tCover struct {
	fileName string
	counter  []uint32
	pos      []uint32
	numStmts []uint16
}

type tCovers []*tCover

var covers_ = make(tCovers, 0)
var coversMu sync.Mutex

func RegisterCover(fileName string, counter []uint32, pos []uint32, numStmts []uint16) {
	coversMu.Lock()
	defer coversMu.Unlock()
	covers_ = append(covers_, &tCover{
		fileName: fileName,
		counter:  counter,
		pos:      pos,
		numStmts: numStmts,
	})
}

func makeCoverProfile(covers tCovers, mode string) ([]*Profile, error) {
	profiles := make([]*Profile, 0)
	for _, cover := range covers {
		profile := &Profile{
			FileName: cover.fileName,
			Mode:     mode,
			Blocks:   make([]ProfileBlock, 0),
		}
		for i := range cover.counter {
			block := ProfileBlock{
				StartLine: int(cover.pos[3*i+0]),
				StartCol:  int(uint16(cover.pos[3*i+2])),
				EndLine:   int(cover.pos[3*i+1]),
				EndCol:    int(uint16(cover.pos[3*i+2] >> 16)),
				NumStmt:   int(cover.numStmts[i]),
				Count:     int(atomic.LoadUint32(&cover.counter[i])),
			}
			profile.Blocks = append(profile.Blocks, block)
		}

		sort.Sort(blocksByStart(profile.Blocks))
		// Merge samples from the same location.
		j := 1
		for i := 1; i < len(profile.Blocks); i++ {
			b := profile.Blocks[i]
			last := profile.Blocks[j-1]
			if b.StartLine == last.StartLine &&
				b.StartCol == last.StartCol &&
				b.EndLine == last.EndLine &&
				b.EndCol == last.EndCol {
				if b.NumStmt != last.NumStmt {
					return nil, fmt.Errorf("inconsistent NumStmt: changed from %d to %d", last.NumStmt, b.NumStmt)
				}
				if mode == "set" {
					profile.Blocks[j-1].Count |= b.Count
				} else {
					profile.Blocks[j-1].Count += b.Count
				}
				continue
			}
			profile.Blocks[j] = b
			j++
		}
		if j <= len(profile.Blocks) {
			profile.Blocks = profile.Blocks[:j]
		}

		profiles = append(profiles, profile)
	}
	return profiles, nil
}

func GenerateHtmlReport(out io.Writer) error {
	return GenerateHtmlReport2(out, "")
}

func GenerateHtmlReport2(out io.Writer, coverageReportRawCodeDir string) error {
	coversMu.Lock()
	covers := covers_
	coversMu.Unlock()
	profiles, err := makeCoverProfile(covers, "set")
	if nil != err {
		return err
	}
	return htmlOutput(profiles, out, coverageReportRawCodeDir)
}
