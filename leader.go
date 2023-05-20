package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

var TERM_OVER = fmt.Errorf("A new term has started")

func (s *Server) becomeLeader() {
	s.mu.Lock()
	s.State = LEADER
	s.currentLeader = s.n.ID()
	for _, id := range s.n.NodeIDs() {
		lastEntry := s.Log[len(s.Log)-1]
		s.NextIndex[id] = lastEntry.Index + 1
		s.MatchIndex[id] = 0
	}
	s.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())

	for _, id := range s.n.NodeIDs() {
		id := id
		go func() {
			err := s.sendAppendEntries(id)
			if err == TERM_OVER {
				cancel()
			} else if err != nil {
				fmt.Fprintf(os.Stderr, err.Error())
			}
		}()
	}

	go func() {
		t := time.NewTicker(50 * time.Millisecond)
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				for _, id := range s.n.NodeIDs() {
					id := id
					go func() {
						err := s.sendAppendEntries(id)
						if err == TERM_OVER {
							cancel()
						} else if err != nil {
							fmt.Fprintf(os.Stderr, err.Error())
						}
					}()
				}
			}
		}
	}()

	go func() {
		<-ctx.Done()
		s.becomeFollower()
	}()
}

func (s *Server) sendAppendEntries(id string) error {
	s.mu.Lock()
	lastEntry := s.Log[len(s.Log)-1]
	lastReplicatedEntry := s.Log[s.NextIndex[id]-1]
	entries := []LogEntry{}
	for i := s.NextIndex[id]; i < len(s.Log); i++ {
		entries = append(entries, s.Log[i])
	}
	inputBody := AppendEntriesInput{
		Type:         "append_entries",
		Term:         s.CurrentTerm,
		LeaderId:     s.n.ID(),
		PrevLogIndex: lastReplicatedEntry.Index,
		PrevLogTerm:  lastReplicatedEntry.Term,
		Entries:      entries,
		LeaderCommit: s.CommitIndex,
	}
	s.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	res, err := s.n.SyncRPC(ctx, id, inputBody)
	if err != nil {
		return err
	} else if inputBody.Term > s.CurrentTerm {
		s.mu.Lock()
		s.CurrentTerm = inputBody.Term
		s.mu.Unlock()
		return TERM_OVER
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
		if s.NextIndex[id] > 1 {
			s.NextIndex[id]--
		}
		s.mu.Unlock()
		return s.sendAppendEntries(id)
	}
	s.mu.Unlock()

	return nil
}
