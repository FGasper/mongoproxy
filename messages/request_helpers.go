package messages

import (
	"fmt"
)

func ToCommandRequest(r Requester) (Command, error) {
	c, ok := r.(Command)
	if !ok {
		return Command{}, fmt.Errorf("Requester was not a command object. Requester received instead: %#v", r)
	}
	return c, nil
}

func ToMessageRequest(r Requester) (*Message, error) {
	m, ok := r.(*Message)
	if !ok {
		return nil, fmt.Errorf("Requester was not a Message object. Requester received instead: %#v", r)
	}
	return m, nil
}
