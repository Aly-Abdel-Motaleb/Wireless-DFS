package main

// message ReplicateFileRequest {
//     string fileName = 1;
//     string filePath = 2;
//     string destinationIp = 3;
//     int32 destinationPort = 4;
// }

// service dataKeeper {
//     rpc replicateFile (ReplicateFileRequest) returns (Ack);
// }
import (
	pb "DFS/dfs"
	tcp "DFS/tcp"
	"context"
	"log"
	"strconv"

	"google.golang.org/grpc"
)

type DataKeeper struct {
	pb.UnimplementedDataKeeperServer
	id   string
	ip   string
	port string
}

func (dk *DataKeeper) ReplicateFile(ctx context.Context, in *pb.ReplicateFileRequest) *pb.Ack {
	// Replicate file to destination
	// in.FileName

	error := tcp.SendFile(in.FilePath, in.DestinationIp, string(in.DestinationPort))

	if error != nil {
		return &pb.Ack{Success: false, Message: "Failed to replicate file"}
	}

	return &pb.Ack{Success: true, Message: "File replicated successfully"}
}

func (dk *DataKeeper) UploadFile(ctx context.Context) {
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
		serverDone <- fs.Start(dk.port, ch)
	}()

	uploadErr := <-serverDone
	if uploadErr != nil {
		log.Fatal("Upload file error:", uploadErr)
	}
	// send notifyfilestored to master
	filename := <-ch
	request := &pb.NotifyFileStoredRequest{
		DatakeeperId: dk.id,
		FilePath:     "./" + filename,
		FileName:     filename,
		Replication:  false,
	}
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	client := pb.NewMasterTrackerClient(conn)
	_, err = client.NotifyFileStored(context.Background(), request)

}

func (dk *DataKeeper) Heartbeat(ctx context.Context) {
	// send heartbeat to master
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	client := pb.NewMasterTrackerClient(conn)
	port, err := strconv.ParseInt(dk.port, 10, 32)
	if err != nil {
		log.Fatalf("failed to convert port to int32: %v", err)
	}
	request := &pb.HeartbeatRequest{
		Id:   dk.id,
		Ip:   dk.ip,
		Port: int32(port),
	}

	_, err = client.Heartbeat(context.Background(), request)

}
