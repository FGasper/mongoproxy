// Package mockule contains a module that can be used as a mock backend for
// proxy core, which stores inserts and queries finds in memory.
package mockule

import (
	. "github.com/mongodbinc-interns/mongoproxy/log"
	"github.com/mongodbinc-interns/mongoproxy/messages"
	"github.com/mongodbinc-interns/mongoproxy/server"
	"gopkg.in/mgo.v2/bson"
/*
	"math/rand"
	"strconv"
*/
)

var maxWireVersion = 17

// a 'database' in memory. The string keys are the collections, which
// have an array of bson documents.
var database = make(map[string][]bson.D)

// The Mockule is a mock module used for testing. It currently
// logs requests and sends valid but generally nonsense responses back to
// the client, without touching mongod.
type Mockule struct {
}

func init() {
	server.Publish(Mockule{})
}

func (m Mockule) New() server.Module {
	return m
}

func (m Mockule) Name() string {
	return "mockule"
}

func (m Mockule) Configure(bson.M) error {
	return nil
}

func (m Mockule) Process(req messages.Requester, res messages.Responder,
	next server.PipelineFunc) {

	switch req.Type() {
	case messages.MessageType:
		message, err := messages.ToMessageRequest(req)
		if err != nil {
			break
		}
		Log(INFO, "%#v", message)

	case messages.CommandType:
		command, err := messages.ToCommandRequest(req)
		if err != nil {
			break
		}
		Log(INFO, "%#v", command)

		reply := messages.CommandResponse{}

		switch command.CommandName {
			case "isMaster":
				fallthrough
			case "ismaster":
				reply.Reply = bson.M{
					"ismaster": true,
					"secondary": false,
					"localTime": bson.Now(),
					"maxWireVersion": maxWireVersion,
					"minWireVersion": 0,
					"maxWriteBatchSize": 1000,
					"maxBsonObjectSize": 16777216,
					"maxMessageSizeBytes": 48000000,
				}
				res.Write(reply)
				return
		}
		reply.Reply = bson.M{"ok": 1}
		res.Write(reply)
	}
	next(req, res)
}
