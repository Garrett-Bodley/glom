package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type server struct {
	Node     *maelstrom.Node
	Messages []float64
	Topology map[string][]string
}

func main() {
	server := server{
		Node:     maelstrom.NewNode(),
		Messages: make([]float64, 0),
		Topology: make(map[string][]string),
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
	s.Messages = append(s.Messages, body.Message)
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
	resp.Messages = append(resp.Messages, s.Messages...)

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
	serverTopology := make([]string, 0)
	serverTopology = append(serverTopology, body.Topology[s.Node.ID()]...)

	resp := topologyResp{Type: "topology_ok"}
	return s.Node.Reply(msg, resp)
}
