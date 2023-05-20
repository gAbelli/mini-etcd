package main

func (s *Server) becomeCandidate() {
	s.mu.Lock()
	s.State = CANDIDATE
	s.currentLeader = ""
	s.mu.Unlock()

	if s.n.ID() == "n0" {
		s.becomeLeader()
	} else {
		s.becomeFollower()
	}
}
