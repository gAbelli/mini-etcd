package main

import (
	"encoding/json"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type AppendEntriesInput struct {
	Type         string     `json:"type"`
	Term         int        `json:"term"`
	LeaderId     string     `json:"leader_id"`
	PrevLogIndex int        `json:"prev_log_index"`
	PrevLogTerm  int        `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leader_commit"`
}

type AppendEntriesOutput struct {
	Type    string `json:"type"`
	Term    int    `json:"term"`
	Success bool   `json:"success"`
}

func (s *Server) appendEntriesHandler(msg maelstrom.Message) error {
	var inputBody AppendEntriesInput
	if err := json.Unmarshal(msg.Body, &inputBody); err != nil {
		return err
	}
	outputBody := AppendEntriesOutput{
		Type: "append_entries_ok",
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	outputBody.Term = s.CurrentTerm
	if inputBody.Term > s.CurrentTerm {
		s.CurrentTerm = inputBody.Term
		s.mu.Unlock()
		s.becomeFollower()
		s.mu.Lock()
	}

	// If something is wrong, set success=false
	if inputBody.Term < s.CurrentTerm {
		return s.n.Reply(msg, outputBody)
	}
	s.currentLeader = inputBody.LeaderId
	s.mu.Unlock()
	s.resetTimer()
	s.mu.Lock()
	if inputBody.PrevLogIndex > len(s.Log)-1 || s.Log[inputBody.PrevLogIndex].Index != inputBody.PrevLogIndex || s.Log[inputBody.PrevLogIndex].Term != inputBody.PrevLogTerm {
		return s.n.Reply(msg, outputBody)
	}

	// Otherwise, update the log
	for _, entry := range inputBody.Entries {
		if entry.Index < len(s.Log) {
			s.Log[entry.Index] = entry
		} else {
			s.Log = append(s.Log, entry)
		}
	}

	// Update CommitIndex
	if inputBody.LeaderCommit > s.CommitIndex {
		oldCommitIndex := s.CommitIndex
		if len(inputBody.Entries) > 0 {
			lastEntry := inputBody.Entries[len(inputBody.Entries)-1]
			if inputBody.LeaderCommit < lastEntry.Index {
				s.CommitIndex = inputBody.LeaderCommit
			} else {
				s.CommitIndex = inputBody.LeaderCommit
			}
		} else {
			s.CommitIndex = inputBody.LeaderCommit
		}
		for i := oldCommitIndex; i < s.CommitIndex; i++ {
			s.dbChan <- i
		}
	}

	// Everything went well
	outputBody.Success = true
	return s.n.Reply(msg, outputBody)
}
