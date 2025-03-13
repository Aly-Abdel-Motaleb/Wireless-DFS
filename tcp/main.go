package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
)

type FileServer struct{}

func (fs *FileServer) start() {
	ln, err := net.Listen("tcp", ":3000")
	if err != nil {
		log.Fatal(err)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go fs.readLoop(conn)
	}

}

func (fs *FileServer) readLoop(conn net.Conn) {
	buf := new(bytes.Buffer)
	defer conn.Close()
	for {
		var size int64
		err := binary.Read(conn, binary.LittleEndian, &size)
		if err != nil {
			log.Fatal(err)
		}
		n, err := io.CopyN(buf, conn, size)
		if err != nil {
			log.Fatal(err)
		}
		file, err := os.OpenFile("received_file", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Fatal(err)
		}
		_, err = file.Write(buf.Bytes())
		if err != nil {
			log.Fatal(err)
		}
		file.Close()
		fmt.Printf("received %d bytes over the network and wrote to file\n", n)
	}
}

func sendFile(path string) error {
	file, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	size := len(file)

	conn, err := net.Dial("tcp", "localhost:3000")
	if err != nil {
		return err
	}

	binary.Write(conn, binary.LittleEndian, int64(size))
	n, err := io.CopyN(conn, bytes.NewReader(file), int64(size))
	if err != nil {
		return err
	}

	fmt.Printf("sent %d bytes over the network", n)
	defer conn.Close()
	return nil
}

func main() {
	go func() {
		var path string
		fmt.Println("Enter the path of the file to send")
		fmt.Scan(&path)
		sendFile(path)
	}()
	fs := FileServer{}
	fs.start()
}
