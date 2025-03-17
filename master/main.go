package main

import (
	"DFS/master/db"
	"log"
	"net"
	"time"

	pb "DFS/dfs"
	"DFS/master/server"

	"google.golang.org/grpc"
)

func main() {
	db.InitDb()

	lis, err := net.Listen("tcp", "localhost:50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpc_server := grpc.NewServer()
	masterServer := server.NewMasterServer()
	pb.RegisterMasterTrackerServer(grpc_server, masterServer)

	log.Println("Starting Master Server on localhost:50051")

	go func() {
		masterServer.UpdateDataKeepersAliveStatus()
		time.Sleep(5 * time.Second)
	}()

	err = grpc_server.Serve(lis)
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}
