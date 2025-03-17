package main

import (
	"DFS/datakeeper/server"
	pb "DFS/dfs"
	"flag"
	"log"
	"net"
	"regexp"
	"time"

	"google.golang.org/grpc"
)

func main() {

	id := flag.String("i", "1", "Datakeeper ID")
	ip := flag.String("ip", "localhost", "Datakeeper IP")
	port := flag.String("p", "50052", "Datakeeper Port")
	MasterAddr := flag.String("m", "localhost:50051", "Master Address")

	flag.Parse()

	ipRegex := `\b((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)\.){3}(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)\b|\b(?:localhost)\b`
	matched, err := regexp.MatchString(ipRegex, *ip)
	if err != nil || !matched {
		log.Fatalf("invalid IP address: %v", *ip)
	}

	if *ip == "" || *port == "" {
		log.Fatalf("IP address and port must not be empty")
	}

	lis, err := net.Listen("tcp", *ip+":"+*port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpc_server := grpc.NewServer()

	dk := server.NewDataKeeperServer(*id, *ip, *port, *MasterAddr)

	pb.RegisterDataKeeperServer(grpc_server, dk)

	log.Println("Starting Datakeeper Server on " + *ip + ":" + *port)

	go func() {
		for {
			dk.Heartbeat()
			time.Sleep(5 * time.Second)
		}
	}()

	err = grpc_server.Serve(lis)
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
