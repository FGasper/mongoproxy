// Package messages contains structs and functions to encode and decode
// wire protocol messages.
package messages

import (
	"gopkg.in/mgo.v2/bson"
)

// constants representing the different opcodes for the wire protocol.
const (
	OP_QUERY int32    = 2004
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
	RequestID     int32
	ResponseTo    int32
	OpCode        int32
}

// struct for a generic command, the default Requester sent from proxy
// core to modules
type Command struct {
	RequestID   int32
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

type Message struct {
	RequestID   int32
	FlagBits	uint32
	Main 		bson.D
	Auxiliary   map[string][]bson.D
}

func (m Message) Type() string {
	return MessageType
}
