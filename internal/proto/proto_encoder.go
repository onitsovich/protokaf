package proto

import (
	"encoding/binary"
	"github.com/Shopify/sarama"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
)

const (
	magicByte    byte = 0x0
	defaultIndex byte = 0x0
	schema       int  = 3
)

var _ sarama.Encoder = &protoEncoder{}

type protoEncoder struct {
	data []byte
	err  error
}

// Encoder returns sarama.Encoder for protobuf.
func Encoder(m *dynamic.Message) sarama.Encoder {
	data, err := m.Marshal()

	b := make([]byte, 6, len(data)+6)
	b[0] = magicByte
	binary.BigEndian.PutUint32(b[1:], uint32(schema))
	b[5] = defaultIndex
	b = append(b, data...)

	return &protoEncoder{
		data: b,
		err:  err,
	}
}

func (s protoEncoder) Encode() ([]byte, error) {
	return s.data, s.err
}

func (s protoEncoder) Length() int {
	return len(s.data)
}

func Unmarshal(b []byte, md *desc.MessageDescriptor) (*dynamic.Message, error) {
	f := dynamic.NewMessageFactoryWithDefaults()
	m := f.NewDynamicMessage(md)

	err := m.UnmarshalJSON(b)
	if err != nil {
		return nil, err
	}

	return m, nil
}
