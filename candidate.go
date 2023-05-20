package main

import (
	"context"
	"encoding/json"
)

func (s *Server) becomeCandidate() {
	s.mu.Lock()
	s.State = CANDIDATE
	s.currentLeader = ""
	s.CurrentTerm++
	term := s.CurrentTerm
	lastEntry := s.Log[len(s.Log)-1]
	lastLogIndex := lastEntry.Index
	lastLogTerm := lastEntry.Term
	s.mu.Unlock()

	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		responses := make(chan error, len(s.n.NodeIDs()))
		responses <- nil
		for _, id := range s.n.NodeIDs() {
			id := id
			if id != s.n.ID() {
				go func() {
					inputBody := RequestVoteInput{
						Type:         "request_vote",
						Term:         term,
						CandidateId:  s.n.ID(),
						LastLogIndex: lastLogIndex,
						LastLogTerm:  lastLogTerm,
					}
					res, err := s.n.SyncRPC(context.Background(), id, inputBody)
					if err != nil {
						responses <- err
					}
					var outputBody RequestVoteOutput
					if err = json.Unmarshal(res.Body, &outputBody); err != nil {
						responses <- err
					}
					if outputBody.Term > term {
						cancel()
						return
					}
					responses <- nil
				}()
			}
		}

		count := 0
		for range s.n.NodeIDs() {
			select {
			case <-ctx.Done():
				s.becomeFollower()
				return
			case r := <-responses:
				if r == nil {
					count++
				}
			}
			if count > len(s.n.NodeIDs())/2 {
				cancel()
				s.mu.Lock()
				newTerm := s.CurrentTerm
				s.mu.Unlock()
				if newTerm == term {
					s.becomeLeader()
				} else {
					s.becomeFollower()
				}
				return
			}
		}
		s.becomeFollower()
	}()

	// if s.n.ID() == "n0" {
	// 	s.becomeLeader()
	// } else {
	// 	s.becomeFollower()
	// }
}
