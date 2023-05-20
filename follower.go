package main

import "time"

func (s *Server) becomeFollower() {
	s.mu.Lock()
	s.State = FOLLOWER
	s.mu.Unlock()
	time.AfterFunc(1*time.Second, func() {
		if s.n.ID() == "n0" {
			s.becomeLeader()
		}
	})
}
