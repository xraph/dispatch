package dwp

// Codec defines the serialization contract for DWP frames.
// Implementations handle encoding/decoding frames to/from bytes.
type Codec interface {
	// Encode serializes a frame to bytes.
	Encode(frame *Frame) ([]byte, error)

	// Decode deserializes bytes into a frame.
	Decode(data []byte) (*Frame, error)

	// Name returns the codec identifier (e.g., "json", "msgpack", "protobuf").
	Name() string
}

// CodecName constants for format negotiation.
const (
	CodecNameJSON     = "json"
	CodecNameMsgpack  = "msgpack"
	CodecNameProtobuf = "protobuf"
)

// GetCodec returns a codec by name. Defaults to JSON.
func GetCodec(name string) Codec {
	switch name {
	case CodecNameMsgpack:
		return &MsgpackCodec{}
	case CodecNameJSON, "":
		return &JSONCodec{}
	default:
		return &JSONCodec{}
	}
}
