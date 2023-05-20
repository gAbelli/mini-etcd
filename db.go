package main

import (
	"fmt"
	"sync"
)

type Db struct {
	Entries map[string]int
	mu      sync.Mutex
}

func NewDb() *Db {
	return &Db{
		Entries: make(map[string]int),
	}
}

func (db *Db) Get(key string) (int, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	val, ok := db.Entries[key]
	if !ok {
		return 0, fmt.Errorf("Key not found")
	}
	return val, nil
}

func (db *Db) Set(key string, val int) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.Entries[key] = val
	return nil
}

func (db *Db) Cas(key string, from int, to int) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	val, ok := db.Entries[key]
	if !ok {
		return fmt.Errorf("Key not found")
	}
	if val != from {
		return fmt.Errorf("Value different from the one expected")
	}
	db.Entries[key] = to
	return nil
}
