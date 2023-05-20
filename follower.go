package main

func (s *Server) becomeFollower() {
	s.mu.Lock()
	s.State = FOLLOWER
	s.currentLeader = ""
	s.mu.Unlock()

	s.resetTimer()
	go func() {
		<-s.electionTimer.C
		s.electionTimer.Stop()
		s.becomeCandidate()
	}()
}
