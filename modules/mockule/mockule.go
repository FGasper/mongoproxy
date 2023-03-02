// Package mockule contains a module that can be used as a mock backend for
// proxy core, which stores inserts and queries finds in memory.
package mockule

import (
	"bytes"
	"net/http"
	"fmt"
	"io"
	"time"

	. "github.com/mongodbinc-interns/mongoproxy/log"
	"github.com/mongodbinc-interns/mongoproxy/messages"
	"github.com/mongodbinc-interns/mongoproxy/server"
	"gopkg.in/mgo.v2/bson"
/*
	"math/rand"
	"strconv"
*/
)

const (
	maxTimeoutSecs = 30
	bsonContentType = "application/bson"
	httpMethod = "POST"

	urlBaseConfName = "urlBase"
	headersConfName = "headers"

	minWireVersion = 6
	maxWireVersion = 17
)

// a 'database' in memory. The string keys are the collections, which
// have an array of bson documents.
var database = make(map[string][]bson.D)

type headerType [2]string

// The Mockule is a mock module used for testing. It currently
// logs requests and sends valid but generally nonsense responses back to
// the client, without touching mongod.
type Mockule struct {
	httpClient   http.Client
	urlBase      string
	extraHeaders []headerType
}

func init() {
	server.Publish(&Mockule{})
}

func (_ *Mockule) New() server.Module {
	return &Mockule{}
}

func (_ *Mockule) Name() string {
	return "mockule"
}

func (m *Mockule) Configure(conf bson.M) error {
	urlBase, exists := conf[urlBaseConfName]
	if !exists {
		return fmt.Errorf("Missing “%s” in config!", urlBaseConfName)
	}

	urlBaseStr, ok := urlBase.(string)
	if !ok {
		return fmt.Errorf("“%s” must be a string, not %v", urlBaseConfName, urlBase)
	}

	// TODO: Validate?
	m.urlBase = urlBaseStr

	headers, exists := conf[headersConfName]
	if exists {
		array, ok := headers.([]interface{})
		if !ok {
			return fmt.Errorf("“%s” must be an array, not %v", headersConfName, array)
		}

		for _, cur := range array {
			curArray, ok := cur.([]interface{})
			if !ok || len(curArray) != 2 {
				return fmt.Errorf("“%s” must be an array of 2-member arrays. (Found: %v)", headersConfName, cur)
			}

			key, ok := curArray[0].(string)
			if !ok {
				return fmt.Errorf("%s: Found non-string header name: %v", headersConfName, curArray[0])
			}

			val, ok := curArray[1].(string)
			if !ok {
				return fmt.Errorf("%s: Found non-string header value: %v", headersConfName, curArray[1])
			}

			m.extraHeaders = append( m.extraHeaders, headerType{key, val} )
		}
	}

	return nil
}

func (m *Mockule) Process(req messages.Requester, res messages.Responder,
	next server.PipelineFunc) {

	switch req.Type() {
		case messages.MessageType:
			message, err := messages.ToMessageRequest(req)
			if err != nil {
				Log(ERROR, "ToMessageRequest: %v", err)
				break
			}

			reply, err := m.handleOpMsg(message)
			if err == nil {
				res.Write(*reply)
				return
			} else {
				Log(ERROR, "%v", err)
			}

		case messages.CommandType:
			command, err := messages.ToCommandRequest(req)
			if err != nil {
				break
			}

			switch command.CommandName {
				case "isMaster":
					fallthrough
				case "ismaster":
					reply := messages.CommandResponse{}
					reply.Reply = bson.M{
						"ismaster": true,
						"secondary": false,
						"localTime": bson.Now(),
						"maxWireVersion": maxWireVersion,
						"minWireVersion": minWireVersion,
						"maxWriteBatchSize": 1000,
						"maxBsonObjectSize": 16777216,
						"maxMessageSizeBytes": 48000000,
					}
					res.Write(reply)
					return
				default:
					Log(ERROR, "Unrecognized OP_QUERY command: %s", command.CommandName)
			}

		default:
			Log(ERROR, "Unrecognized request type: %s", req.Type())
	}
	
	next(req, res)
}

// ----------------------------------------------------------------------

func (m *Mockule) getPostUrl() string {
	urlBase := m.urlBase

	// Tolerate trailing slash:
	if urlBase[len(urlBase)-2:] == "/" {
		urlBase = urlBase[:len(urlBase)-2]
	}

	//return urlBase + "/op_msg";
	return urlBase
}

func (m *Mockule) handleOpMsg(msg *messages.Message) (*messages.Message, error) {
	Log(DEBUG, "Marshalling BSON: %v", msg)

	reqBody, err := bson.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("Failed to marshal parsed OP_MSG to BSON: %v", err)
	}

	url := m.getPostUrl()

	httpReq, err := http.NewRequest(
		httpMethod,
		url,
		bytes.NewReader(reqBody),
	)
	if err != nil {
		return nil, fmt.Errorf("Failed to initialize HTTP %s request to %s: %v", httpMethod, m.getPostUrl(), err)
	}

	for _, hdr := range m.extraHeaders {
		httpReq.Header.Add(hdr[0], hdr[1])
	}

	httpReq.Header.Set("Content-Type", bsonContentType)

	Log(DEBUG, "Sending HTTP request: %v", httpReq)

	resp, err := m.getHttpClient().Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("Failed to send HTTP POST to %s: %v", m.getPostUrl(), err)
	}

	Log(DEBUG, "HTTP response: %v", resp)

	// Every case needs the body, so we read it proactively.
	body, readErr := io.ReadAll(resp.Body)

	if !httpRespSucceeded(resp) {
		if readErr != nil {
			Log(ERROR, "Failed to read non-success HTTP response body: %v", readErr)
		}

		return nil, fmt.Errorf("Received HTTP failure response: %v %s", resp, string(body))
	}

	Log(DEBUG, "HTTP response is a success")

	if resp.Header.Get("Content-Type") != bsonContentType {
		if readErr != nil {
			Log(ERROR, "Failed to read non-BSON HTTP response body: %v", readErr)
		}

		return nil, fmt.Errorf("Received non-BSON HTTP response: %v %s", resp, string(body))
	}

	Log(DEBUG, "HTTP response is the right content type")

	if readErr != nil {
		return nil, fmt.Errorf("Failed to read HTTP response body: %v", readErr)
	}

	Log(DEBUG, "Got HTTP response body")

	respMsg := messages.Message{}
	err = bson.Unmarshal(body, &respMsg)
	if err != nil {
		return nil, fmt.Errorf("Failed parse HTTP response body as BSON: %v", err)
	}

	if 0 == len(respMsg.Main) {
		generic := bson.D{}
		err2 := bson.Unmarshal(body, &generic)
		if err2 == nil {
			return nil, fmt.Errorf("Response body (%v) schema is wrong", generic)
		}

		return nil, fmt.Errorf("Response body (failed to parse: %v) schema is wrong", err2)
	}

	Log(DEBUG, "Unmarshalled BSON: %v", respMsg)

	return &respMsg, nil
}

func httpRespSucceeded(resp *http.Response) bool {
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

func (m *Mockule) getHttpClient() *http.Client {
//	if (nil == m.httpClient.Transport) {
		m.httpClient.Transport = &http.Transport{
			IdleConnTimeout: maxTimeoutSecs * time.Second,
		}
//	}

	return &m.httpClient;
}
