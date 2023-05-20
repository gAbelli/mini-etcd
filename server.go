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
	s := &Server{
		n:          maelstrom.NewNode(),
		db:         NewDb(),
		dbChan:     make(chan int, 10),
		State:      FOLLOWER,
		NextIndex:  make(map[string]int),
		MatchIndex: make(map[string]int),
	}
	// Make sure the log is never empty
	s.Log = append(s.Log, LogEntry{
		Index: 0,
		Term:  0,
		Command: map[string]any{
			"type": "initialize",
		},
	})
	return s
}

func (s *Server) Run() {
	go func() {
		for {
			index := <-s.dbChan
			s.mu.Lock()
			command := s.Log[index].Command
			if command["type"] == "write" {
				key := command["key"].(int)
				val := command["val"].(int)
				if err := s.db.Set(key, val); err != nil {
					log.Fatalf("Error while writing to the db: %v\n", err)
				}
			} else if command["type"] == "cas" {
				key := command["key"].(int)
				from := command["from"].(int)
				to := command["to"].(int)
				if err := s.db.Cas(key, from, to); err != nil {
					log.Fatalf("Error while compare-and-swapping in the db: %v\n", err)
				}
			} else if command["type"] == "initialize" {
			} else {
				log.Fatalf("Invalid command")
			}
			s.mu.Unlock()
		}
	}()

	s.n.Handle("append_entries", s.appendEntriesHandler)
	s.n.Handle("read", s.handleRead)
	s.n.Handle("write", s.handleWrite)
	s.n.Handle("cas", s.handleCas)

	if err := s.n.Run(); err != nil {
		log.Fatal(err)
	}
}
