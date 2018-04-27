package serialization

import (
	"bufio"
	"io"
)

type serializer interface {
	Serialize(value interface{}) []byte
	Deserialize(in []byte, value interface{})
}
