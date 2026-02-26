package dwp

import "github.com/vmihailenco/msgpack/v5"

// MsgpackCodec encodes/decodes DWP frames as MessagePack.
type MsgpackCodec struct{}

func (c *MsgpackCodec) Encode(frame *Frame) ([]byte, error) {
	return msgpack.Marshal(frame)
}

func (c *MsgpackCodec) Decode(data []byte) (*Frame, error) {
	var f Frame
	if err := msgpack.Unmarshal(data, &f); err != nil {
		return nil, err
	}
	return &f, nil
}

func (c *MsgpackCodec) Name() string { return CodecNameMsgpack }
