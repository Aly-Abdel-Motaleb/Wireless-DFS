package main

import (
	"DFS/master/db"
	"log"
	"net"

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
	pb.RegisterMasterTrackerServer(grpc_server, server.NewMasterServer())

	log.Println("Starting Master Server on localhost:50051")

	err = grpc_server.Serve(lis)
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
