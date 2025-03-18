package main

import (
	"context"
	"fmt"
	"net"

	pb "DFS/dfs" // Replace with actual proto package path
	"DFS/tcp"

	"google.golang.org/grpc"
)

// Master Tracker gRPC Address
const masterTrackerAddr = "localhost:50051"

func main() {
	// if len(os.Args) < 3 {
	// 	fmt.Println("Usage: client upload/download <file.mp4>")
	// 	return
	// }

	// command := os.Args[1]
	// filePath := os.Args[2]

	// switch command {
	// case "upload":
	// default:
	// 	fmt.Println("Unknown command. Use 'upload' or 'download'.")
	// }

	uploadFile("video.mp4")
	// downloadFile("video.mp4")
}

func uploadFile(filePath string) {
	conn, err := grpc.Dial(masterTrackerAddr, grpc.WithInsecure())
	if err != nil {
		fmt.Println("Failed to connect to Master Tracker:", err)
		return
	}
	defer conn.Close()

	client := pb.NewMasterTrackerClient(conn)

	// Request an available Data Keeper node
	resp, err := client.RequestUpload(context.Background(), &pb.UploadRequest{FileName: filePath})

	if err != nil {
		fmt.Println("Failed to get Data Keeper node:", err)
		return
	}

	fmt.Println("Uploading to Data Keeper at:", resp.Ip, resp.Port)

	tcpConn, err := net.Dial("tcp", resp.Ip+":"+resp.Port)
	if err != nil {
		fmt.Println("Failed to connect to Data Keeper:", err)
		return
	}
	defer conn.Close()

	err = tcp.SendFile(tcpConn, filePath)
	if err != nil {
		fmt.Println("File upload failed:", err)
		return
	}
}

func downloadFile(fileName string) {
	conn, err := grpc.Dial(masterTrackerAddr, grpc.WithInsecure())
	if err != nil {
		fmt.Println("Failed to connect to Master Tracker:", err)
		return
	}
	defer conn.Close()

	client := pb.NewMasterTrackerClient(conn)

	// Request available Data Keeper nodes for the file
	resp, err := client.RequestDownload(context.Background(), &pb.DownloadRequest{FileName: fileName})
	if err != nil {
		fmt.Println("Failed to get Data Keeper nodes:", err)
		return
	}

	// Try downloading from the first available node
	ip_1, port_1 := resp.Ips[0], resp.Ports[0]

	fmt.Println("Downloading from Data Keeper at:", ip_1, port_1)

	tcpConn, err := net.Dial("tcp", ip_1+":"+port_1)
	if err != nil {
		fmt.Println("Error connecting:", err)
		return
	}
	defer tcpConn.Close()

	ch := make(chan tcp.FileDetails)
	exit := make(chan bool)
	go tcp.ReceiveFile(tcpConn, exit, ch)

	for {
		select {
		case fileDetails := <-ch:
			fmt.Println("File received:", fileDetails.FileName)
			return
		}
	}
}
