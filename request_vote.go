package main

import (
	"encoding/json"
	"math/rand"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type RequestVoteInput struct {
	Type         string `json:"type"`
	Term         int    `json:"term"`
	CandidateId  string `json:"candidate_id"`
	LastLogIndex int    `json:"last_log_index"`
	LastLogTerm  int    `json:"last_log_term"`
}

type RequestVoteOutput struct {
	Type        string `json:"type"`
	Term        int    `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

func (s *Server) requestVoteHandler(msg maelstrom.Message) error {
	var inputBody RequestVoteInput
	if err := json.Unmarshal(msg.Body, &inputBody); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	outputBody := RequestVoteOutput{
		Type: "request_vote_ok",
		Term: s.CurrentTerm,
	}

	if inputBody.Term < s.CurrentTerm {
		return s.n.Reply(msg, outputBody)
	}
	if inputBody.Term > s.CurrentTerm {
		s.CurrentTerm = inputBody.Term
		s.mu.Unlock()
		s.becomeFollower()
		s.mu.Lock()
	}

	if s.VotedFor == "" || s.VotedFor == inputBody.CandidateId {
		// If he is more up-to-date, we vote for him
		if inputBody.LastLogIndex >= len(s.Log) || len(s.Log) == 0 {
			outputBody.VoteGranted = true
			s.VotedFor = inputBody.CandidateId
			return s.n.Reply(msg, outputBody)
		}
		// If we are more up-to-date, we don't vote for him
		if inputBody.LastLogIndex < len(s.Log)-1 {
			return s.n.Reply(msg, outputBody)
		}
		// Otherwise, we check if the last entry corresponds
		lastEntry := s.Log[len(s.Log)-1]
		if lastEntry.Index == inputBody.LastLogIndex && lastEntry.Term == inputBody.LastLogTerm {
			outputBody.VoteGranted = true
			s.VotedFor = inputBody.CandidateId
			return s.n.Reply(msg, outputBody)
		}
	}

	// If something is wrong, we end up here
	return s.n.Reply(msg, outputBody)
}

func (s *Server) resetTimer() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.electionTimer.Reset(time.Duration(150+rand.Intn(100)) * time.Millisecond)
}
