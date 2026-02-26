package dwp

import "encoding/json"

// JSONCodec encodes/decodes DWP frames as JSON.
type JSONCodec struct{}

func (c *JSONCodec) Encode(frame *Frame) ([]byte, error) {
	return json.Marshal(frame)
}

func (c *JSONCodec) Decode(data []byte) (*Frame, error) {
	var f Frame
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, err
	}
	return &f, nil
}

func (c *JSONCodec) Name() string { return CodecNameJSON }
