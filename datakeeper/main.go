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
	"net"
)

type DataKeeper struct {
	pb.UnimplementedDataKeeperServer
	ip   string
	port string
}

func (dk *DataKeeper) ReplicateFile(ctx context.Context, in *pb.ReplicateFileRequest) *pb.Ack {
	// Replicate file to destination
	// in.FileName

	error := tcp.SendFile(in.FilePath, in.DestinationIp, string(in.DestinationPort))

	if error != nil {
		return &pb.Ack{Success: false}
	}

	return &pb.Ack{Success: true}
}

func (dk *DataKeeper) UploadFile(ctx context.Context) {
	// listen to upload request
	// save file to disk
	// send notifyfilestored to master
	fs := &tcp.FileServer{}
	serverDone := make(chan error)

	// listen to the upload request
	// if upload request has been received then save the file to disk
	// send notifyfilestored to master

	go func() {
		serverDone <- fs.Start(dk.port)
	}()

	uploadErr := <-serverDone
	if uploadErr != nil {
		log.Fatal("Upload file error:", uploadErr)
	}
	// send notifyfilestored to master
	conn, err := net.Dial("tcp", "localhost:50051")
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}

	conn.Write(*pb.NotifyFileStoredRequest{
		dataKeeperIp: dk.ip,
		filePath: 
	})


}
