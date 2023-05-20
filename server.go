package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

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

	db            *Db
	dbChan        chan int
	electionTimer *time.Timer
	currentLeader string

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
		n:             maelstrom.NewNode(),
		db:            NewDb(),
		dbChan:        make(chan int, 10),
		electionTimer: time.NewTimer(100 * time.Millisecond),
		State:         DEAD,
		NextIndex:     make(map[string]int),
		MatchIndex:    make(map[string]int),
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
	s.becomeFollower()
	go func() {
		for {
			index := <-s.dbChan
			s.mu.Lock()
			command := s.Log[index].Command
			if command["type"] == "write" {
				fmt.Fprintln(os.Stderr, command)
				_key := command["key"]
				key, ok := _key.(float64)
				if !ok {
					log.Fatalf("write key")
				}
				_val := command["val"]
				val, ok := _val.(float64)
				if !ok {
					log.Fatalf("write val")
				}
				if err := s.db.Set(int(key), int(val)); err != nil {
					log.Fatalf("Error while writing to the db: %v\n", err)
				}
			} else if command["type"] == "cas" {
				_key := command["key"]
				key, ok := _key.(float64)
				if !ok {
					log.Fatalf("cas key")
				}
				_from := command["from"]
				from, ok := _from.(float64)
				if !ok {
					log.Fatalf("cas from")
				}
				_to := command["to"]
				to, ok := _to.(float64)
				if !ok {
					log.Fatalf("cas to")
				}
				_ = s.db.Cas(int(key), int(from), int(to))
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
