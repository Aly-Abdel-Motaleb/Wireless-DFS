package main

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"strings"

	pb "DFS/dfs" // Replace with actual proto package path
	"DFS/tcp"

	"google.golang.org/grpc"
)

const masterTrackerAddr = "localhost:50051"

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "upload":
			if len(os.Args) < 3 {
				fmt.Println("Usage: client upload <file_path>")
				return
			}
			uploadFile(os.Args[2])
		case "download":
			if len(os.Args) < 3 {
				fmt.Println("Usage: client download <file_name>")
				return
			}
			downloadFile(os.Args[2])
		default:
			fmt.Println("Unknown command. Use 'upload' or 'download'.")
		}
		return
	}

	interactiveCLI()
}

func interactiveCLI() {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println("Choose an option:")
		fmt.Println("1. Upload a file")
		fmt.Println("2. Download a file")
		fmt.Println("3. Exit")
		fmt.Print("Enter choice: ")
		choice, _ := reader.ReadString('\n')
		choice = strings.TrimSpace(choice)

		switch choice {
		case "1":
			fmt.Print("Enter file path: ")
			filePath, _ := reader.ReadString('\n')
			uploadFile(strings.TrimSpace(filePath))
		case "2":
			listFiles()
		case "3":
			fmt.Println("Exiting...")
			return
		default:
			fmt.Println("Invalid choice, try again.")
		}
	}
}

func uploadFile(filePath string) {
	conn, err := grpc.Dial(masterTrackerAddr, grpc.WithInsecure())
	if err != nil {
		fmt.Println("Failed to connect to Master Tracker:", err)
		return
	}
	defer conn.Close()

	client := pb.NewMasterTrackerClient(conn)
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
	defer tcpConn.Close()

	err = tcp.SendFile(tcpConn, filePath)
	if err != nil {
		fmt.Println("File upload failed:", err)
		return
	}
	fmt.Println("Upload successful.")
}

func downloadFile(fileName string) {
	conn, err := grpc.Dial(masterTrackerAddr, grpc.WithInsecure())
	if err != nil {
		fmt.Println("Failed to connect to Master Tracker:", err)
		return
	}
	defer conn.Close()

	client := pb.NewMasterTrackerClient(conn)
	resp, err := client.RequestDownload(context.Background(), &pb.DownloadRequest{FileName: fileName})
	if err != nil {
		fmt.Println("Failed to get Data Keeper nodes:", err)
		return
	}

	id, ip, port := resp.Ids[0], resp.Ips[0], resp.Ports[0]
	fmt.Println("Downloading from Data Keeper at:", ip, port)
	tcpConn, err := net.Dial("tcp", ip+":"+port)
	if err != nil {
		fmt.Println("Error connecting:", err)
		return
	}
	defer tcpConn.Close()

	ch := make(chan tcp.FileDetails)
	exit := make(chan bool)
	go tcp.ReceiveFile(tcpConn, exit, ch, id, ".")

	select {
	case fileDetails := <-ch:
		fmt.Println("File received:", fileDetails.FileName)
	case <-exit:
		fmt.Println("Download failed.")
	}
}

func listFiles() {
	conn, err := grpc.Dial(masterTrackerAddr, grpc.WithInsecure())
	if err != nil {
		fmt.Println("Failed to connect to Master Tracker:", err)
		return
	}
	defer conn.Close()

	client := pb.NewMasterTrackerClient(conn)
	resp, err := client.ListFiles(context.Background(), &pb.EmptyRequest{})
	if err != nil {
		fmt.Println("Failed to list files:", err)
		return
	}

	fmt.Println("Available files:")
	for _, file := range resp.FileDetails {
		fmt.Printf("- %s (ID: %d, Size: %d bytes)\n", file.Name, file.Id, file.Size)
	}

	fmt.Print("Enter file name to download: ")
	reader := bufio.NewReader(os.Stdin)
	fileName, _ := reader.ReadString('\n')
	downloadFile(strings.TrimSpace(fileName))
}
