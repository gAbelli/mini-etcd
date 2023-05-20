package main

func (s *Server) becomeFollower() {
	s.mu.Lock()
	s.State = FOLLOWER
	s.mu.Unlock()
}
