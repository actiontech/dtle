package ast

type Node interface {
	Text() string
	SetText(text string)
}

// node is the struct implements Node interface
type node struct {
	text string
}

// Text implements Node interface.
func (n *node) Text() string {
	return n.text
}

// SetText implements Node interface.
func (n *node) SetText(text string) {
	n.text = text
}
