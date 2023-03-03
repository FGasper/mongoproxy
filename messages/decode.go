package messages

import (
	"bytes"
	"encoding/binary"
	"fmt"
	. "github.com/mongodbinc-interns/mongoproxy/log"
	"gopkg.in/mgo.v2/bson"
	"io"
	"strings"
)

var allowedOpQueryCommands = []string{
	"ismaster",
	"isMaster",
}

const opQueryCollection string = "$cmd"

const MSG_HEADER_LENGTH uint8 = 16

func opQueryCommandAllowed(name string) bool {
	for _, allowed := range allowedOpQueryCommands {
		if allowed == name {
			return true
		}
	}

	return false;
}

func splitCommandOpQuery(q bson.D) (string, bson.M) {
	commandName := q[0].Name

	args := bson.M{}

	// throw the command arguments into args. This includes the command
	// name, as some of the commands have an important argument attached
	// to the command definition as well.
	for i := 0; i < len(q); i++ {
		args[q[i].Name] = q[i].Value
	}

	return commandName, args
}

// ParseNamespace splits a namespace string into the database and collection.
// The first return value is the database, the second, the collection. An error
// is returned if either the database or the collection doesn't exist.
func ParseNamespace(namespace string) (string, string, error) {
	index := strings.Index(namespace, ".")
	if index < 0 || index >= len(namespace) {
		return "", "", fmt.Errorf("not a namespace")
	}
	database, collection := namespace[0:index], namespace[index+1:]

	// Error if empty database or collection
	if len(database) == 0 {
		return "", "", fmt.Errorf("empty database field")
	}

	if len(collection) == 0 {
		return "", "", fmt.Errorf("empty collection field")
	}

	return database, collection, nil
}

func createCommand(header MsgHeader, commandName string, database string, args bson.M) Command {
	c := Command{
		RequestID:   header.RequestID,
		CommandName: commandName,
		Database:    database,
		Args:        args,
	}
	return c
}

// reads a header from the reader (16 bytes), consistent with wire protocol
func processHeader(reader io.Reader) (MsgHeader, error) {
	// read the message header
	msgHeaderBytes := make([]byte, MSG_HEADER_LENGTH)
	n, err := reader.Read(msgHeaderBytes)
	if err != nil && err != io.EOF {
		return MsgHeader{}, err
	}
	if n == 0 {
		// EOF?
		Log(INFO, "connection closed")
		return MsgHeader{}, err
	}
	mHeader := MsgHeader{}
	err = binary.Read(bytes.NewReader(msgHeaderBytes), binary.LittleEndian, &mHeader)
	if err != nil {
		Log(ERROR, "error decoding from reader: %v", err)
		return MsgHeader{}, err
	}

	// sanity check
	if mHeader.MessageLength <= 15 {
		return MsgHeader{}, fmt.Errorf("Message length not long enough for header")
	}

	return mHeader, nil
}

func decodeBSON(buffer []byte) (bson.D, uint32, error) {
	var err error
	var doc bson.D
	var bsonLen uint32

	if (len(buffer) < 4) {
		err = fmt.Errorf("Found few bytes (%d) to store BSON document length", len(buffer))
	}

	if err == nil {
		bsonLen = binary.LittleEndian.Uint32(buffer)
		if bsonLen > uint32(len(buffer)) {
			err = fmt.Errorf("Found few bytes (%d) to store BSON document length", len(buffer))
		}
	}

	if err == nil {
		err = bson.Unmarshal(buffer, &doc)
	}

	return doc, bsonLen, err
}

func decodeCString(buffer []byte) (string, error) {
	nullAt := bytes.IndexByte(buffer, 0)
	if nullAt == -1 {
		return "", fmt.Errorf("Malformed string: no terminating NUL")
	}

	return string(buffer[:nullAt]), nil
}

func decodeUint32(buffer []byte) (uint32, error) {
	if len(buffer) < 4 {
		return 0, fmt.Errorf("Too few bytes (%d) to store a uint32", len(buffer))
	}

	return binary.LittleEndian.Uint32(buffer), nil
}

func processOpMsg(msgBody []byte, header MsgHeader) (Requester, error) {
	flags, err := decodeUint32(msgBody)
	if err != nil {
		return nil, err
	}

	// We needn’t support this for now. More context:
	// https://github.com/mongodb/specifications/blob/master/source/message/OP_MSG.rst#exhaustallowed
	//
	if (flags & OP_MSG_FLAG_EXHAUST_ALLOWED) != 0 {
		return nil, fmt.Errorf("exhaustAllowed flag given but is forbidden")
	}

	// Likewise.
	if (flags & OP_MSG_FLAG_MORE_TO_COME) != 0 {
		return nil, fmt.Errorf("moreToCome flag given but is forbidden")
	}

	msgBodyLen := uint32(len(msgBody))
	cursor := uint32(4)  // sizeof uint32

	msg := Message{
		RequestID: header.RequestID,
		FlagBits: flags,
		Auxiliary: MessageAuxiliary{},
	}

	foundType0 := false

	for cursor < msgBodyLen {
		if (msgBodyLen - cursor == 4) {
			if (flags & OP_MSG_FLAG_CHECKSUM_PRESENT) != 0 {
				// TODO: Verify checksum
				Log(DEBUG, "Checksum present; skipping verification (unimplemented)")
				break
			}
		}

		sectionType := msgBody[cursor]
		cursor++

		switch sectionType {
			case 0:
				if (foundType0) {
					return nil, fmt.Errorf(">1 main section")
				}

				foundType0 = true

				doc, bsonLen, err := decodeBSON(msgBody[cursor:])
				if err != nil {
					return nil, err
				}

				msg.Main = doc

				cursor += bsonLen

			case 1:
				sectionLen, err := decodeUint32(msgBody)
				if err != nil {
					return nil, err
				}

				if sectionLen > msgBodyLen - cursor {
					return nil, fmt.Errorf("Section claims too much size (%d; only %d left)", sectionLen, msgBodyLen - cursor)
				}

				sectionCursor := 4 + cursor

				identifier, err := decodeCString(msgBody[sectionCursor:])
				if err != nil {
					return nil, err
				}

				// “Pre-advance” the cursor.
				cursor += sectionLen

				sectionCursor += 1 + uint32(len(identifier))

				docs := []bson.D{}

				for sectionCursor < cursor {
					doc, bsonLen, err := decodeBSON(msgBody[sectionCursor:])
					if err != nil {
						return nil, err
					}

					docs = append(docs, doc)
					sectionCursor += bsonLen
				}

				msg.Auxiliary[identifier] = docs

			default:
				return nil, fmt.Errorf("Unknown section type: %d", sectionType)
		}
	}

	if !foundType0 {
		return nil, fmt.Errorf("No type-0 sections in OP_MSG body")
	}

	return &msg, nil
}

func processOpQuery(msgBody []byte, header MsgHeader) (Requester, error) {

	// Skip 4 bytes for the flags, which we don't need.
	namespace, err := decodeCString(msgBody[4:])

	database, collection, err := ParseNamespace(namespace)

	if err != nil {
		return nil, fmt.Errorf("error parsing namespace: %v", err)
	}

	if collection != opQueryCollection {
		return nil, fmt.Errorf("OP_QUERY is only for the “%s” collection, not “%s”", opQueryCollection, collection)
	}

	// 4 bytes for flags
	// 4 bytes for numberToSkip
	// 4 bytes for numberToReturn
	// 1 byte for namespace's NULL

	bsonBytes := msgBody[13 + len(namespace):]

	document := bson.D{}
	err = bson.Unmarshal(bsonBytes, &document)
	if err != nil {
		return nil, err
	}

	cName, args := splitCommandOpQuery(document)
	if !opQueryCommandAllowed(cName) {
		return nil, fmt.Errorf("OP_QUERY forbids the “%s” command (only allows: %v)", cName, allowedOpQueryCommands)
	}

	return createCommand(header, cName, database, args), nil
}

type opCodeDecoderT func([]byte, MsgHeader) (Requester, error)
var opCodeDecoder = map[OpCode]opCodeDecoderT {
	OP_QUERY: processOpQuery,
	OP_MSG: processOpMsg,
}

func slurpMessageBody(ioReader io.Reader, header MsgHeader) ([]byte, error) {
	bodyLength := header.MessageLength - int32(MSG_HEADER_LENGTH)
	msgBody := make([]byte, bodyLength)

	_, err := ioReader.Read(msgBody)
	if err != nil {
		return nil, fmt.Errorf("Failed to read %d-byte message: %v", bodyLength, err)
	}

	return msgBody, nil
}

// Decodes a wire protocol message from a connection into a Requester to pass
// onto modules, a struct containing the header of the original message, and an error.
// It returns a non-nil error if reading from the connection
// fails in any way
func Decode(reader io.Reader) (Requester, MsgHeader, error) {
	mHeader, err := processHeader(reader)

	var req Requester;
	var decoderFunc opCodeDecoderT

	if err == nil {
		decoderFunc = opCodeDecoder[mHeader.OpCode]

		if decoderFunc == nil {
			err = fmt.Errorf("unimplemented operation: %#v", mHeader)
		}
	}

	var msgBody []byte

	if err == nil {
		msgBody, err = slurpMessageBody(reader, mHeader)
	}

	if err == nil {
		req, err = decoderFunc(msgBody, mHeader)
	}

	return req, mHeader, err
}
