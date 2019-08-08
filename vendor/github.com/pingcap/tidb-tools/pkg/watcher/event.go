package watcher

import (
	"bytes"
	"os"
)

// Op represents file operation type
type Op uint32

// Operations type current supported
const (
	Create Op = 1 << iota
	Remove
	Modify
	Rename
	Chmod
	Move
)

func (op Op) String() string {
	var buffer bytes.Buffer

	// now, only one Op will used in polling, but it can combine multi Ops if needed
	if op&Create == Create {
		buffer.WriteString("|CREATE")
	}
	if op&Remove == Remove {
		buffer.WriteString("|REMOVE")
	}
	if op&Modify == Modify {
		buffer.WriteString("|MODIFY")
	}
	if op&Rename == Rename {
		buffer.WriteString("|RENAME")
	}
	if op&Chmod == Chmod {
		buffer.WriteString("|CHMOD")
	}
	if op&Move == Move {
		buffer.WriteString("|MOVE")
	}
	if buffer.Len() == 0 {
		return ""
	}
	return buffer.String()[1:] // Strip leading pipe
}

// Event represents a single file operation event
type Event struct {
	Path     string
	Op       Op
	FileInfo os.FileInfo
}

// IsDirEvent returns whether is a event for a directory
func (e *Event) IsDirEvent() bool {
	if e == nil {
		return false
	}
	return e.FileInfo.IsDir()
}

// HasOps checks whether has any specified operation types
func (e *Event) HasOps(ops ...Op) bool {
	if e == nil {
		return false
	}
	for _, op := range ops {
		if e.Op&op != 0 {
			return true
		}
	}
	return false
}
