package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/siddarthpai/wardrobe/respgo"
	"github.com/siddarthpai/wardrobe/store"
)

func main() {
	var (
		portOpt   string
		replicaOf string
	)
	flag.StringVar(&portOpt, "port", "8000", "port to listen on")
	flag.StringVar(&replicaOf, "replicaof", "", "master address in form host:port")
	flag.Parse()

	cacheSvc := store.New()
	cacheSvc.Info.Port = portOpt

	if replicaOf != "" {
		parts := strings.Split(replicaOf, ":")
		if len(parts) != 2 {
			log.Fatalf("invalid replicaof argument: %s", replicaOf)
		}
		cacheSvc.Info.Role = "slave"
		cacheSvc.Info.MasterIP = parts[0]
		cacheSvc.Info.MasterPort = parts[1]

		log.Printf("Starting in slave mode, syncing with master at %s:%s", parts[0], parts[1])
		go cacheSvc.HandleReplication()

		time.Sleep(100 * time.Millisecond)
	}

	bindAddr := fmt.Sprintf("0.0.0.0:%s", cacheSvc.Info.Port)
	ln, err := net.Listen("tcp", bindAddr)
	if err != nil {
		log.Fatalf("failed to bind on %s: %v", bindAddr, err)
	}
	defer ln.Close()
	log.Printf("Cache server listening on %s", bindAddr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}

		session := store.Connection{
			Conn:       conn,
			TxnStarted: false,
			TxnQueue:   make([][]string, 0),
		}
		parser := respgo.NewParser(conn)

		go cacheSvc.HandleConnection(session, parser)
	}
}
