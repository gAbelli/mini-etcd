package main

type LogEntry struct {
	Index   int
	Term    int
	Command map[string]any
}
