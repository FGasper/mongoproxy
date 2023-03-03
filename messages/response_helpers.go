package messages

import (
	"bytes"
	"fmt"
	"github.com/mongodbinc-interns/mongoproxy/buffer"
	"gopkg.in/mgo.v2/bson"
)

// A ResponseWriter is the interface that is used to convert module responses
// to wire protocol messages.
type ResponseWriter interface {
	// ToBytes encodes a ResponseWriter into a valid wire protocol message
	// corresponding to the input response header.
	ToBytes(MsgHeader) ([]byte, error)

	// ToBSON encodes a ResponseWriter into a BSON document that can be examined
	// by other modules.
	ToBSON() bson.M
}

// A struct that represents a response to a generic command.
type CommandResponse struct {
	Reply     bson.M
	Documents []bson.D
}

func (c CommandResponse) ToBytes(header MsgHeader) ([]byte, error) {
	resHeader := createResponseHeader(header, OP_REPLY)
	startingFrom := int32(0)

	flags := int32(8)

	buf := bytes.NewBuffer([]byte{})

	// write all documents
	err := buffer.WriteToBuf(buf, resHeader, int32(flags), int64(0), int32(startingFrom),
		int32(1+len(c.Documents)))
	if err != nil {
		return nil, fmt.Errorf("error writing prepared response: %v", err)
	}
	reply := c.Reply
	reply["ok"] = 1
	docBytes, err := marshalReplyDocs(reply, c.Documents)
	if err != nil {
		return nil, fmt.Errorf("error marshaling documents: %v", err)
	}

	resp := append(buf.Bytes(), docBytes...)

	resp = setMessageSize(resp)

	return resp, nil
}

func (c CommandResponse) ToBSON() bson.M {
	return c.Reply
}
