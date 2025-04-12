package main

import (
	"DFS/master/db"
	"DFS/master/server"
	"log"
	"net"
	"time"

	pb "DFS/dfs"

	"google.golang.org/grpc"
)

func main() {
	ip := ""
	port := ":50051"
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		log.Fatalf("failed to get interface addresses: %v", err)
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ip4 := ipnet.IP.To4(); ip4 != nil {
				ip = ip4.String()
			}
		}
	}
	ip = ip + port

	log.Printf("Master server is starting at %s\n", ip)

	db.InitDb()

	lis, err := net.Listen("tcp", ip)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpc_server := grpc.NewServer()
	masterServer := server.NewMasterServer()
	pb.RegisterMasterTrackerServer(grpc_server, masterServer)

	go func() {
		for {
			masterServer.UpdateDataKeepersAliveStatus()
			time.Sleep(5 * time.Second)
		}
	}()

	go func() {
		for {
			masterServer.ReplicateFiles()
			time.Sleep(10 * time.Second)
		}
	}()

	err = grpc_server.Serve(lis)
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}
