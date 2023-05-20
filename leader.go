package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

func (s *Server) becomeLeader() {
	s.mu.Lock()
	s.State = LEADER
	for _, id := range s.n.NodeIDs() {
		lastEntry := s.Log[len(s.Log)-1]
		s.NextIndex[id] = lastEntry.Index + 1
		s.MatchIndex[id] = 0
	}
	s.mu.Unlock()

	for _, id := range s.n.NodeIDs() {
		id := id
		go func() {
			err := s.sendAppendEntries(id)
			if err != nil {
				fmt.Fprintf(os.Stderr, err.Error())
			}
		}()
	}

	t := time.NewTicker(50 * time.Millisecond)
	for {
		select {
		case <-t.C:
			for _, id := range s.n.NodeIDs() {
				id := id
				go func() {
					err := s.sendAppendEntries(id)
					if err != nil {
						fmt.Fprintf(os.Stderr, err.Error())
					}
				}()
			}
		}
	}
}

func (s *Server) sendAppendEntries(id string) error {
	s.mu.Lock()
	lastEntry := s.Log[len(s.Log)-1]
	entries := []LogEntry{}
	for i := s.NextIndex[id]; i < len(s.Log); i++ {
		entries = append(entries, s.Log[i])
	}
	inputBody := AppendEntriesInput{
		Type:         "append_entries",
		Term:         s.CurrentTerm,
		LeaderId:     s.n.ID(),
		PrevLogIndex: lastEntry.Index,
		PrevLogTerm:  lastEntry.Term,
		Entries:      entries,
		LeaderCommit: s.CommitIndex,
	}
	s.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	res, err := s.n.SyncRPC(ctx, id, inputBody)
	if err != nil {
		return err
	}

	var outputBody AppendEntriesOutput
	if err := json.Unmarshal(res.Body, &outputBody); err != nil {
		return err
	}

	s.mu.Lock()
	if outputBody.Success {
		s.NextIndex[id] = lastEntry.Index + 1
		s.MatchIndex[id] = lastEntry.Index
	} else {
		if s.NextIndex[id] > 0 {
			s.NextIndex[id]--
		}
		s.mu.Unlock()
		return s.sendAppendEntries(id)
	}
	s.mu.Unlock()

	return nil
}
