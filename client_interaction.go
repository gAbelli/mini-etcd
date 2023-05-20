package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

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
	if s.n.ID() == "n0" && s.State != LEADER {
		s.becomeLeader()
	}
	var inputBody ReadInput
	if err := json.Unmarshal(msg.Body, &inputBody); err != nil {
		return err
	}

	if s.State != LEADER && s.n.ID() != "n0" {
		res, err := s.n.SyncRPC(context.Background(), "n0", inputBody)
		if err != nil {
			return err
		}
		var outputBody ReadOutput
		if err = json.Unmarshal(res.Body, &outputBody); err != nil {
			return err
		}
		return s.n.Reply(msg, outputBody)
	}

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
	if s.n.ID() == "n0" && s.State != LEADER {
		s.becomeLeader()
	}
	var inputBody WriteInput
	if err := json.Unmarshal(msg.Body, &inputBody); err != nil {
		return err
	}

	if s.State != LEADER && s.n.ID() != "n0" {
		res, err := s.n.SyncRPC(context.Background(), "n0", inputBody)
		if err != nil {
			return err
		}
		var outputBody WriteOutput
		if err = json.Unmarshal(res.Body, &outputBody); err != nil {
			return err
		}
		return s.n.Reply(msg, outputBody)
	}

	s.mu.Lock()
	entry := LogEntry{
		Index: len(s.Log),
		Term:  s.CurrentTerm,
		Command: map[string]any{
			"type": "write",
			"key":  inputBody.Key,
			"val":  inputBody.Value,
		},
	}
	s.Log = append(s.Log, entry)
	s.mu.Unlock()

	responses := make(chan error, len(s.n.NodeIDs()))
	responses <- nil
	for _, id := range s.n.NodeIDs() {
		id := id
		if id == s.n.ID() {
			continue
		}
		go func() {
			err := s.sendAppendEntries(id)
			responses <- err
		}()
	}

	count := 0
	for range s.n.NodeIDs() {
		err := <-responses
		if err == nil {
			count++
		}
		if count > len(s.n.NodeIDs())/2 {
			break
		}
	}
	if count <= len(s.n.NodeIDs())/2 {
		return fmt.Errorf("Not enough servers responded")
	}

	s.mu.Lock()
	for s.CommitIndex != entry.Index-1 {
		s.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		s.mu.Lock()
	}
	s.CommitIndex++
	err := s.db.Set(inputBody.Key, inputBody.Value)
	s.LastApplied++
	s.mu.Unlock()

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
	if s.n.ID() == "n0" && s.State != LEADER {
		s.becomeLeader()
	}
	var inputBody CasInput
	if err := json.Unmarshal(msg.Body, &inputBody); err != nil {
		return err
	}

	if s.State != LEADER && s.n.ID() != "n0" {
		res, err := s.n.SyncRPC(context.Background(), "n0", inputBody)
		if err != nil {
			return err
		}
		var outputBody CasOutput
		if err = json.Unmarshal(res.Body, &outputBody); err != nil {
			return err
		}
		return s.n.Reply(msg, outputBody)
	}

	s.mu.Lock()
	entry := LogEntry{
		Index: len(s.Log),
		Term:  s.CurrentTerm,
		Command: map[string]any{
			"type": "cas",
			"key":  inputBody.Key,
			"from": inputBody.From,
			"to":   inputBody.To,
		},
	}
	s.Log = append(s.Log, entry)
	s.mu.Unlock()

	responses := make(chan error, len(s.n.NodeIDs()))
	responses <- nil
	for _, id := range s.n.NodeIDs() {
		id := id
		if id == s.n.ID() {
			continue
		}
		go func() {
			err := s.sendAppendEntries(id)
			responses <- err
		}()
	}

	count := 0
	for range s.n.NodeIDs() {
		err := <-responses
		if err == nil {
			count++
		}
		if count > len(s.n.NodeIDs())/2 {
			break
		}
	}
	if count <= len(s.n.NodeIDs())/2 {
		return fmt.Errorf("Not enough servers responded")
	}

	s.mu.Lock()
	for s.CommitIndex != entry.Index-1 {
		s.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		s.mu.Lock()
	}
	s.CommitIndex++
	err := s.db.Cas(inputBody.Key, inputBody.From, inputBody.To)
	s.LastApplied++
	s.mu.Unlock()

	if err != nil {
		return err
	}
	outputBody := CasOutput{
		Type: "cas_ok",
	}
	return s.n.Reply(msg, outputBody)
}
