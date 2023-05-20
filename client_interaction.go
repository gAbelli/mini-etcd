package main

import (
	"encoding/json"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type ReadInput struct {
	Type string `json:"type"`
	Key  int    `json:"key"`
}

type ReadOutput struct {
	Type  string `json:"type"`
	Value int    `json:"value"`
}

func (s *Server) handleRead(msg maelstrom.Message) error {
	var inputBody ReadInput
	if err := json.Unmarshal(msg.Body, &inputBody); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	val, err := s.db.Get(inputBody.Key)
	if err != nil {
		return err
	}
	outputBody := ReadOutput{
		Type:  "read_ok",
		Value: val,
	}
	return s.n.Reply(msg, outputBody)
}

type WriteInput struct {
	Type  string `json:"type"`
	Key   int    `json:"key"`
	Value int    `json:"value"`
}

type WriteOutput struct {
	Type string `json:"type"`
}

func (s *Server) handleWrite(msg maelstrom.Message) error {
	var inputBody WriteInput
	if err := json.Unmarshal(msg.Body, &inputBody); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.db.Set(inputBody.Key, inputBody.Value)
	if err != nil {
		return err
	}
	outputBody := WriteOutput{
		Type: "write_ok",
	}
	return s.n.Reply(msg, outputBody)
}

type CasInput struct {
	Type string `json:"type"`
	Key  int    `json:"key"`
	From int    `json:"from"`
	To   int    `json:"to"`
}

type CasOutput struct {
	Type string `json:"type"`
}

func (s *Server) handleCas(msg maelstrom.Message) error {
	var inputBody CasInput
	if err := json.Unmarshal(msg.Body, &inputBody); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.db.Cas(inputBody.Key, inputBody.From, inputBody.To)
	if err != nil {
		return err
	}
	outputBody := CasOutput{
		Type: "cas_ok",
	}
	return s.n.Reply(msg, outputBody)
}
