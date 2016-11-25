package fingerprint

import (
	"log"

	client "udup/client/config"
	"udup/server/structs"
)

// UdupFingerprint is used to fingerprint the Udup version
type UdupFingerprint struct {
	StaticFingerprinter
	logger *log.Logger
}

// NewUdupFingerprint is used to create a Udup fingerprint
func NewUdupFingerprint(logger *log.Logger) Fingerprint {
	f := &UdupFingerprint{logger: logger}
	return f
}

func (f *UdupFingerprint) Fingerprint(config *client.Config, node *structs.Node) (bool, error) {
	node.Attributes["udup.version"] = config.Version
	node.Attributes["udup.revision"] = config.Revision
	return true, nil
}
