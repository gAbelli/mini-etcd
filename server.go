package main

import (
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	DEAD = iota
	LEADER
	FOLLOWER
	CANDIDATE
)

type Server struct {
	n  *maelstrom.Node
	mu sync.Mutex

	db     *Db
	dbChan chan int

	State       int
	CurrentTerm int
	VotedFor    string
	Log         []LogEntry
	CommitIndex int
	LastApplied int
	NextIndex   map[string]int
	MatchIndex  map[string]int
}

func NewServer() *Server {
	return &Server{
		n:          maelstrom.NewNode(),
		db:         NewDb(),
		dbChan:     make(chan int, 10),
		State:      LEADER,
		NextIndex:  make(map[string]int),
		MatchIndex: make(map[string]int),
	}
}

func (s *Server) Run() {
	go func() {
		for {
			index := <-s.dbChan
			s.mu.Lock()
			command := s.Log[index].Command
			if command["type"] == "write" {
				key := command["key"].(string)
				val := command["val"].(int)
				if err := s.db.Set(key, val); err != nil {
					log.Fatalf("Error while writing to the db: %v\n", err)
				}
			} else if command["type"] == "cas" {
				key := command["key"].(string)
				from := command["from"].(int)
				to := command["to"].(int)
				if err := s.db.Cas(key, from, to); err != nil {
					log.Fatalf("Error while compare-and-swapping in the db: %v\n", err)
				}
			}
			s.mu.Unlock()
		}
	}()

	s.n.Handle("append_entries", s.appendEntriesHandler)

	if err := s.n.Run(); err != nil {
		log.Fatal(err)
	}
}
