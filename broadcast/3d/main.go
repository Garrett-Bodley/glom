package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	Node           *maelstrom.Node
	MsgMutex       sync.RWMutex
	Messages       []float64
	MsgSet         map[float64]bool
	NeighborsMutex sync.RWMutex
	Neighbors      []string
}

func main() {
	server := server{
		Node:     maelstrom.NewNode(),
		Messages: make([]float64, 0),
		MsgSet:   make(map[float64]bool),
	}

	server.Node.Handle("broadcast", server.handleBroadcast)
	server.Node.Handle("read", server.handleRead)
	server.Node.Handle("topology", server.handleTopology)

	if err := server.Node.Run(); err != nil {
		log.Fatal(err)
	}
}

type broadcastMsgBody struct {
	Type    string  `json:"type"`
	Message float64 `json:"message"`
}

type broadcastResp struct {
	Type string `json:"type"`
}

func (s *server) handleBroadcast(msg maelstrom.Message) error {
	body := broadcastMsgBody{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		log.Println(err)
		return err
	}

	s.MsgMutex.Lock()
	newMsg := body.Message
	exists := s.MsgSet[newMsg]

	if exists {
		s.MsgMutex.Unlock()
		return s.Node.Reply(msg, broadcastResp{Type: "broadcast_ok"})
	}
	s.MsgSet[newMsg] = true
	s.Messages = append(s.Messages, newMsg)
	s.MsgMutex.Unlock()

	err := s.Node.Reply(msg, broadcastResp{Type: "broadcast_ok"})
	if err != nil {
		log.Println(err)
	}

	ackSet := make(map[string]bool)
	ackMutex := sync.RWMutex{}

	s.NeighborsMutex.RLock()
	for {
		count := 0
		for _, n := range s.Neighbors {
			if msg.Src == n {
				continue
			}
			ackMutex.RLock()
			if ackSet[n] {
				ackMutex.RUnlock()
				continue
			}
			ackMutex.RUnlock()
			count++
			err := s.Node.RPC(n, body, func(msg maelstrom.Message) error {
				body := broadcastResp{}
				if err := json.Unmarshal(msg.Body, &body); err != nil {
					log.Println(err)
					return err
				}
				if body.Type == "broadcast_ok" {
					ackMutex.Lock()
					ackSet[n] = true
					ackMutex.Unlock()
					return nil
				}
				log.Printf("broadcast to neighbor \"%s\" failed (｡Ó﹏Ò｡)", n)
				return fmt.Errorf("broadcast to neighbor \"%s\" failed (｡Ó﹏Ò｡)", n)
			})
			if err != nil {
				respBody := broadcastResp{Type: "broadcast_not_okay"}
				s.Node.Reply(msg, respBody)
				return err
			}
		}
		if count == 0 {
			break
		}
		time.Sleep(time.Millisecond * 50)
	}
	s.NeighborsMutex.RUnlock()

	respBody := broadcastResp{Type: "broadcast_ok"}
	return s.Node.Reply(msg, respBody)
}

type readMsgBody struct {
	Type string `json:"type"`
}

type readResp struct {
	Type     string    `json:"type"`
	Messages []float64 `json:"messages"`
}

func (s *server) handleRead(msg maelstrom.Message) error {
	var body readMsgBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		log.Println(err)
		return err
	}
	resp := readResp{Type: "read_ok", Messages: make([]float64, 0)}
	s.MsgMutex.RLock()
	resp.Messages = append(resp.Messages, s.Messages...)
	s.MsgMutex.RUnlock()

	return s.Node.Reply(msg, resp)
}

type topologyMsgBody struct {
	Type     string              `json:"string"`
	Topology map[string][]string `jsong:"topology"`
}

type topologyResp struct {
	Type string `json:"type"`
}

func (s *server) handleTopology(msg maelstrom.Message) error {
	var body topologyMsgBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		log.Println(err)
		return err
	}
	s.NeighborsMutex.Lock()
	s.Neighbors = append([]string{}, body.Topology[s.Node.ID()]...)
	s.NeighborsMutex.Unlock()

	resp := topologyResp{Type: "topology_ok"}
	return s.Node.Reply(msg, resp)
}
