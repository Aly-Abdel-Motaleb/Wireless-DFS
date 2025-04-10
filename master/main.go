package main

import (
	"DFS/master/db"
	"flag"
	"log"
	"net"
	"time"

	pb "DFS/dfs"
	"DFS/master/server"

	"google.golang.org/grpc"
)

func main() {
	ip := flag.String("ip", "localhost:50051", "Master IP")

	db.InitDb()

	lis, err := net.Listen("tcp", *ip)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpc_server := grpc.NewServer()
	masterServer := server.NewMasterServer()
	pb.RegisterMasterTrackerServer(grpc_server, masterServer)

	log.Println("Starting Master Server on localhost:50051")

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
