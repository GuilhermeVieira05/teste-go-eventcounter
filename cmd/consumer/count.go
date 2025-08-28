package main

import (
	"strings"
	"sync"
)

type EventCounter struct{
	mutex sync.Mutex
	data map[string]map[string]int
}

func NewEventCounter() *EventCounter {
	return &EventCounter{
		data: make(map[string]map[string]int),
	}
}

func ParseUserID(routingKey []byte) (string) {
	parts := strings.Split(string(routingKey), ".")
	if len(parts) > 0 {
        return parts[0]
    }
    return ""
}

func (c *EventCounter) Increment(eventType, userID string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if _, ok := c.data[eventType]; !ok {
		c.data[eventType] = make(map[string]int)
	}
	c.data[eventType][userID]++
}

func (c *EventCounter) Snapshot() map[string]map[string]int {
    c.mutex.Lock()
    defer c.mutex.Unlock()
    // Deep copy para evitar concorrência
    copied := make(map[string]map[string]int)
    for typ, users := range c.data {
        copied[typ] = make(map[string]int)
        for user, cnt := range users {
            copied[typ][user] = cnt
        }
    }
    return copied
}