package main

import (
	"fmt"
	"sync"
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
		return 0, fmt.Errorf("Key not found")
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
		return fmt.Errorf("Key not found")
	}
	if val != from {
		return fmt.Errorf("Value different from the one expected")
	}
	db.Entries[key] = to
	return nil
}
