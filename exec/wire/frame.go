package wire

import "github.com/xraph/dispatch/exec"

// MaxFrameBytes caps a decoded frame. A payload larger than this is
// refused rather than allocated, so a corrupt or hostile length header
// cannot exhaust the reader's memory.
const MaxFrameBytes = 64 << 20

// Kind identifies which side of the exchange a frame carries.
type Kind uint8

const (
	// KindRequest is a parent-to-child execution request.
	KindRequest Kind = 1
	// KindResult is a child-to-parent execution result.
	KindResult Kind = 2
)

// Frame is one message. Exactly one of Request or Result is set,
// matching Kind.
type Frame struct {
	Kind    Kind          `msgpack:"kind"`
	Request *exec.Request `msgpack:"request,omitempty"`
	Result  *exec.Result  `msgpack:"result,omitempty"`
}
