package main

import (
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Db struct {
	Entries map[int]int
	mu      sync.Mutex
}

func NewDb() *Db {
	return &Db{
		Entries: make(map[int]int),
	}
}

func (db *Db) Get(key int) (int, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	val, ok := db.Entries[key]
	if !ok {
		return 0, maelstrom.NewRPCError(maelstrom.KeyDoesNotExist, "Key does not exist")
	}
	return val, nil
}

func (db *Db) Set(key int, val int) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.Entries[key] = val
	return nil
}

func (db *Db) Cas(key int, from int, to int) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	val, ok := db.Entries[key]
	if !ok {
		return maelstrom.NewRPCError(maelstrom.KeyDoesNotExist, "Key does not exist")
	}
	if val != from {
		return maelstrom.NewRPCError(maelstrom.PreconditionFailed, "Precondition failed")
	}
	db.Entries[key] = to
	return nil
}
