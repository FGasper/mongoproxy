// Package messages contains structs and functions to encode and decode
// wire protocol messages.
package messages

import (
	"fmt"
	"bytes"

	. "github.com/mongodbinc-interns/mongoproxy/log"
	"github.com/mongodbinc-interns/mongoproxy/buffer"
	"gopkg.in/mgo.v2/bson"
)

type RequestID int32
type OpCode int32

// constants representing the different opcodes for the wire protocol.
const (
	OP_QUERY OpCode    = 2004
	OP_REPLY          = 1
	OP_MSG			  = 2013

	OP_MSG_FLAG_CHECKSUM_PRESENT uint32 = 1 << 0
	OP_MSG_FLAG_MORE_TO_COME uint32 = 1 << 1
	OP_MSG_FLAG_EXHAUST_ALLOWED uint32 = 1 << 16
)

// constants representing the types of request structs supported by proxy core.
const (
	CommandType string = "command"
	MessageType string = "message"
)

// a struct to represent a wire protocol message header.
type MsgHeader struct {
	MessageLength int32
	RequestID     RequestID
	ResponseTo    RequestID
	OpCode        OpCode
}

// struct for a generic command, the default Requester sent from proxy
// core to modules
type Command struct {
	RequestID   RequestID
	CommandName string
	Database    string
	Args        bson.M
	Metadata    bson.M
	Docs        []bson.D
}

func (c Command) Type() string {
	return CommandType
}

func (c Command) ToBSON() bson.D {
	nameArg, ok := c.Args[c.CommandName]
	if !ok {
		nameArg = 1
	}
	args := bson.D{
		{c.CommandName, nameArg},
	}

	for arg, value := range c.Args {
		if arg != c.CommandName {
			args = append(args, bson.DocElem{arg, value})
		}
	}

	return args
}

// GetArg takes the name of an argument for the command and returns the
// value of that argument.
func (c Command) GetArg(arg string) interface{} {
	a, ok := c.Args[arg]
	if !ok {
		return nil
	}
	return a
}

// ----------------------------------------------------------------------

type MessageAuxiliary map[string][]bson.D

type Message struct {
	RequestID   RequestID
	FlagBits	uint32

	// The wire protocol docs say that we’re fine to merge the
	// “auxiliary” sections into the body section; however, those docs
	// are meant more for clients than for servers. It’s best for us
	// to preserve the distinction between sections just in case
	// proxy modules need it, now or in the future.
	Body 		bson.D   `bson:"main"` //  TODO: remove
	Auxiliary   MessageAuxiliary
}

func (_ Message) Type() string {
	return MessageType
}

func (m Message) ToBytes(header MsgHeader) ([]byte, error) {
	resHeader := createResponseHeader(header, OP_MSG)

	mainBson, err := bson.Marshal(m.Body)
	if err != nil {
		return nil, fmt.Errorf("Failed to marshal OP_MSG body document: %v", err)
	}

	buf := bytes.NewBuffer([]byte{})

	err = buffer.WriteToBuf(
		buf,
		resHeader, // size will be filled in later
		uint32(0), // no flags
		uint8(0),  // first section is type 0
		mainBson,
	)

	if err != nil {
		return nil, fmt.Errorf("Failed to initialize OP_MSG: %v", err)
	}

	for identifier, bsonDocs := range m.Auxiliary {
		err := buffer.WriteToBuf(
			buf,
			uint32(1),            // type 1 section
			identifier, uint8(0), // NUL-terminated string
		)
		if err != nil {
			return nil, fmt.Errorf("Failed to initialize “%s” in OP_MSG: %v", identifier, err)
		}

		for _, bsonDoc := range bsonDocs {
			err := buffer.WriteToBuf( buf, bsonDoc )
			if err != nil {
				return nil, fmt.Errorf("Failed to extend “%s” in OP_MSG: %v", identifier, err)
			}
		}
	}

	respBytes := buf.Bytes()
	respBytes = setMessageSize(respBytes)

	Log(DEBUG, "response length: %d", len(respBytes))

	return respBytes, nil
}

func (m Message) ToBSON() bson.M {
	panic("unimplemented")
}
