package mongoproxy

import (
	"fmt"
	. "github.com/mongodbinc-interns/mongoproxy/log"
	"github.com/mongodbinc-interns/mongoproxy/messages"
	"github.com/mongodbinc-interns/mongoproxy/modules"
	"net"
)

func Start(port int, pipeline modules.PipelineFunc) {

	ln, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		Log(ERROR, "Error listening on port %v: %v\n", port, err)
		return
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			Log(ERROR, "error accepting connection: %v\n", err)
			continue
		}

		Log(NOTICE, "accepted connection from: %v\n", conn.RemoteAddr())
		go handleConnection(conn, pipeline)
	}

}

func handleConnection(conn net.Conn, pipeline modules.PipelineFunc) {
	for {

		message, msgHeader, err := messages.Decode(conn)

		if err != nil {
			Log(ERROR, "%#v", err)
			return
		}

		res := &messages.ModuleResponse{}
		pipeline(message, res)

		Log(DEBUG, "%#v\n", res)

		bytes, err := messages.Encode(msgHeader, *res)
		if msgHeader.OpCode == 2002 || msgHeader.OpCode == 2001 ||
			msgHeader.OpCode == 2006 {
			continue
		}
		if err != nil {
			Log(ERROR, "%#v", err)
			return
		}
		_, err = conn.Write(bytes)
		if err != nil {
			Log(ERROR, "%#v", err)
			return
		}

	}
}
