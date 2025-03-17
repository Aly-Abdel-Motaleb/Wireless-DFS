package server

import (
	pb "DFS/dfs"
	tcp "DFS/tcp"
	"context"
	"log"
	"os"

	"google.golang.org/grpc"
)

type DataKeeper struct {
	pb.UnimplementedDataKeeperServer
	id         string
	ip         string
	port       string
	masterAddr string
}

func NewDataKeeperServer(id, ip, port, masterAdr string) *DataKeeper {
	return &DataKeeper{id: id, ip: ip, masterAddr: masterAdr, port: port}
}

func (dk *DataKeeper) ReplicateFile(ctx context.Context, in *pb.ReplicateFileRequest) (*pb.Ack, error) {
	// Replicate file to destination
	// in.FileName

	error := tcp.SendFile(in.FilePath, in.DestinationIp, string(in.DestinationPort))

	if error != nil {
		return &pb.Ack{Success: false, Message: "Failed to replicate file"}, error
	}

	return &pb.Ack{Success: true, Message: "File replicated successfully"}, nil
}

func (dk *DataKeeper) requestUpload(ctx context.Context, in *pb.DatakeeperRequest) (*pb.UploadResponse, error) {

	// listen to upload request
	// save file to disk
	// send notifyfilestored to master
	fs := &tcp.FileServer{}
	serverDone := make(chan error)
	ch := make(chan string)
	// listen to the upload request
	// if upload request has been received then save the file to disk
	// send notifyfilestored to master

	go func() {
		serverDone <- fs.Start("9000", ch)
	}()

	uploadErr := <-serverDone
	if uploadErr != nil {
		return nil, uploadErr
	}

	return &pb.UploadResponse{Ip: dk.ip, Port: "9000"}, nil
}

// func (dk *DataKeeper) UploadFile(ctx context.Context) {
// 	// listen to upload request
// 	// save file to disk
// 	// send notifyfilestored to master
// 	fs := &tcp.FileServer{}
// 	serverDone := make(chan error)
// 	ch := make(chan string)
// 	// listen to the upload request
// 	// if upload request has been received then save the file to disk
// 	// send notifyfilestored to master

// 	go func() {
// 		serverDone <- fs.Start(dk.port, ch)
// 	}()

// 	uploadErr := <-serverDone
// 	if uploadErr != nil {
// 		log.Fatal("Upload file error:", uploadErr)
// 	}
// 	// send notifyfilestored to master
// 	filename := <-ch
// 	request := &pb.NotifyFileStoredRequest{
// 		DatakeeperId: dk.id,
// 		FilePath:     "./" + filename,
// 		FileName:     filename,
// 		Replication:  false,
// 	}
// 	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure(), grpc.WithBlock())
// 	if err != nil {
// 		log.Fatalf("did not connect: %v", err)
// 	}
// 	defer conn.Close()
// 	client := pb.NewMasterTrackerClient(conn)
// 	_, err = client.NotifyFileStored(context.Background(), request)

// }

func (dk *DataKeeper) Heartbeat() {
	// send heartbeat to master
	conn, err := grpc.Dial(dk.masterAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to send heartbeat: %v", err)
		os.Exit(1)
	}
	defer conn.Close()
	client := pb.NewMasterTrackerClient(conn)
	request := &pb.HeartbeatRequest{
		Id:   dk.id,
		Ip:   dk.ip,
		Port: dk.port,
	}
	_, err = client.Heartbeat(context.Background(), request)
	if err != nil {
		log.Fatalf("Failed to send heartbeat: %v", err)
		os.Exit(1)
	}
}
