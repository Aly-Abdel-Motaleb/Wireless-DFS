package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"

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
	// uploadFile("main.exe")
	// default:
	// 	fmt.Println("Unknown command. Use 'upload' or 'download'.")
	// }

	downloadFile("main.exe")
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

	err = tcp.SendFile(filePath, resp.Ip, resp.Port)
	if err != nil {
		fmt.Println("File upload failed:", err)
		return
	}

}

// Send file to Data Keeper via TCP
func sendFileToDataKeeper(ip string, port int32, fileName string) error {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", ip, port))
	if err != nil {
		return fmt.Errorf("failed to connect to Data Keeper: %v", err)
	}
	defer conn.Close()

	file, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	_, err = io.Copy(conn, file)
	if err != nil {
		return fmt.Errorf("failed to send file: %v", err)
	}

	fmt.Println("File uploaded successfully!")
	return nil
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
	fs := tcp.NewFileServer()
	fs.StartOnPort(ip_1, port_1)
	err = fs.WaitOnConnections(false)

	if err != nil {
		fmt.Println("Failed to download file:", err)
		return
	}
	fmt.Println("File downloaded successfully!")
}

// Receive file from Data Keeper via TCP
func receiveFileFromDataKeeper(ip string, port int32, fileName string) error {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", ip, port))
	if err != nil {
		return fmt.Errorf("failed to connect to Data Keeper: %v", err)
	}
	defer conn.Close()

	file, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("failed to create file: %v", err)
	}
	defer file.Close()

	_, err = io.Copy(file, conn)
	if err != nil {
		return fmt.Errorf("failed to receive file: %v", err)
	}

	return nil
}
