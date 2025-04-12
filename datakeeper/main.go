package main

import (
	"DFS/datakeeper/server"
	pb "DFS/dfs"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"regexp"
	"time"

	"google.golang.org/grpc"
)

func main() {
	ip := ""
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		log.Fatalf("failed to get interface addresses: %v", err)
	}

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ip4 := ipnet.IP.To4(); ip4 != nil {
				ip = ip4.String()
				if ip[0:3] == "192" {
					break
				}
			}
		}
	}

	id := flag.String("i", "1", "Datakeeper ID")
	port := *flag.String("p", "50052", "Datakeeper Port")
	MasterAddr := *flag.String("m", "192.168.1.15:50051", "Master Address")

	flag.Parse()

	ipRegex := `\b((25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)\.){3}(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)\b|\b(?:localhost)\b`
	matched, err := regexp.MatchString(ipRegex, ip)
	if err != nil || !matched {
		log.Fatalf("invalid IP address: %v", ip)
	}

	if ip == "" || port == "" {
		log.Fatalf("IP address and port must not be empty")
	}

	err = os.Mkdir(fmt.Sprintf("datakeeper_%s", *id), 0755)
	if err != nil && !os.IsExist(err) {
		log.Printf("Failed to create directory: %v", err)
	}

	lis, err := net.Listen("tcp", ip+":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpc_server := grpc.NewServer()

	dk := server.NewDataKeeperServer(*id, ip, port, MasterAddr)

	pb.RegisterDataKeeperServer(grpc_server, dk)

	log.Println("Starting Datakeeper Server on " + ip + ":" + port)

	go func() {
		for {
			dk.Heartbeat()
			time.Sleep(1 * time.Second)
		}
	}()

	err = grpc_server.Serve(lis)
	if err != nil {
		log.Printf("Failed to serve: %v", err)
	}
}
