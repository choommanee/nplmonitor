package main

import (
	"bufio"
	"fmt"
	"sync"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/valyala/fasthttp"
)

type sseHub struct {
	mu      sync.RWMutex
	clients map[string]chan string
}

var hub = &sseHub{clients: make(map[string]chan string)}

func (h *sseHub) add() (string, chan string) {
	id := uuid.New().String()
	ch := make(chan string, 16)
	h.mu.Lock()
	h.clients[id] = ch
	h.mu.Unlock()
	return id, ch
}

func (h *sseHub) remove(id string) {
	h.mu.Lock()
	if ch, ok := h.clients[id]; ok {
		close(ch)
		delete(h.clients, id)
	}
	h.mu.Unlock()
}

func (h *sseHub) broadcast(msg string) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, ch := range h.clients {
		select {
		case ch <- msg:
		default:
		}
	}
}

// GET /api/events — Server-Sent Events
func handleSSE(c *fiber.Ctx) error {
	c.Set("Content-Type", "text/event-stream")
	c.Set("Cache-Control", "no-cache")
	c.Set("Connection", "keep-alive")
	c.Set("X-Accel-Buffering", "no")

	id, ch := hub.add()
	c.Context().SetBodyStreamWriter(fasthttp.StreamWriter(func(w *bufio.Writer) {
		defer hub.remove(id)
		fmt.Fprint(w, "data: connected\n\n")
		_ = w.Flush()
		for msg := range ch {
			fmt.Fprintf(w, "data: %s\n\n", msg)
			if err := w.Flush(); err != nil {
				return
			}
		}
	}))
	return nil
}
