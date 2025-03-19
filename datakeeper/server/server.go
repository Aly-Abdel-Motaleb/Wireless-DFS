package server

import (
	pb "DFS/dfs"
	tcp "DFS/tcp"
	"context"
	"fmt"
	"log"
	"net"
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

	for i, _ := range in.Ids {
		conn, err := grpc.Dial(in.Ips[i]+":"+in.Ports[i], grpc.WithInsecure())
		if err != nil {
			return &pb.Ack{Success: false, Message: fmt.Sprintf("Failed to connect to %v", in.Ips[i])}, nil
		}
		defer conn.Close()
		client := pb.NewDataKeeperClient(conn)
		request := &pb.DataKeeperUploadRequest{Replication: true}
		// return the ip and port for the tcp connections
		resp, err := client.RequestUpload(context.Background(), request)
		if err != nil {
			return &pb.Ack{Success: false, Message: fmt.Sprintf("Failed to request upload from %v", in.Ips[i])}, nil
		}
		tcpConn, err := net.Dial("tcp", resp.Ip+":"+resp.Port)
		if err != nil {
			return &pb.Ack{Success: false, Message: fmt.Sprintf("Failed to connect to %v", in.Ips[i])}, nil
		}
		defer tcpConn.Close()
		err = tcp.SendFile(tcpConn, in.FilePath)
		if err != nil {
			return &pb.Ack{Success: false, Message: fmt.Sprintf("Failed to replicate file to %v", in.Ips[i])}, nil
		}
	}

	return &pb.Ack{Success: true, Message: fmt.Sprintf("File replicated to %v datakeepers", len(in.Ids))}, nil
}

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

func (dk *DataKeeper) RequestUpload(ctx context.Context, in *pb.DataKeeperUploadRequest) (*pb.UploadResponse, error) {
	fileServer := tcp.NewFileServer(dk.id)
	port, err := fileServer.Start() // port and error
	if err != nil {
		return nil, err
	}
	go fileServer.WaitOnConnections(false)

	fileServer.SetOnReceive(func(filedetails tcp.FileDetails) {
		request := &pb.NotifyFileStoredRequest{
			DatakeeperId: dk.id,
			FilePath:     filedetails.Path,
			FileName:     filedetails.FileName,
			FileHash:     filedetails.Hash,
			FileSize:     filedetails.Size,
			Replication:  in.Replication,
		}
		conn, err := grpc.Dial(dk.masterAddr, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("did not connect: %v", err)
		}
		defer conn.Close()
		client := pb.NewMasterTrackerClient(conn)

		resp, err := client.NotifyFileStored(context.Background(), request)
		if resp != nil && !resp.Success {
			switch resp.ErrorCode {
			case pb.ErrorCode_FILE_ALREADY_EXISTS:
				log.Printf("File already exists")
			case pb.ErrorCode_FILE_NOT_FOUND:
				log.Printf("File not found")
			case pb.ErrorCode_FETAL_ERROR:
				log.Fatalf("Fatal error: %v", resp.Message)
			default:
				log.Fatalf("Unknown error: %v", resp.Message)
			}
		}
	})

	return &pb.UploadResponse{Ip: dk.ip, Port: port}, nil
}

func (dk *DataKeeper) RequestDownload(ctx context.Context, in *pb.DownloadRequest) (*pb.DataKeeperDownloadResponse, error) {
	fileServer := tcp.NewFileServer(dk.id)
	fileServer.SetFileDetails(*tcp.NewFileDetails(in.FileName, nil, nil, nil))
	port, err := fileServer.Start() // port and error
	if err != nil {
		return nil, err
	}

	go fileServer.WaitOnConnections(true)

	return &pb.DataKeeperDownloadResponse{Ip: dk.ip, Port: port}, nil
}
