package tcp

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
)

type FileServer struct {
	ln net.Listener   // TCP listener
	wg sync.WaitGroup // WaitGroup to wait for all goroutines to finish
}

// Start starts the file server on the specified port.
// It listens for incoming TCP connections and reads files from them.
// Upon successful completion of reading a file, it sends the filename to the provided channel.
//
// Args:
//
//	port (string): The port number to listen on.
//	ch (chan<- string): The channel to send the filename to upon successful completion.
//
// Returns:
//
//	error: An error if any occurs during the file server operation, otherwise nil.
func (fs *FileServer) Start(port string, ch chan<- string) error {
	ln, err := net.Listen("tcp", port)

	if err != nil {
		return err
	}
	fs.ln = ln
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			var opErr *net.OpError
			if errors.As(err, &opErr) && opErr.Err.Error() == "use of closed network connection" {
				fs.wg.Wait()
				return nil
			}
			return fmt.Errorf("accept error: %w", err)
		}
		fs.wg.Add(1)
		go func(c net.Conn) {
			defer fs.wg.Done()
			fs.readLoop(c, ch)
		}(conn)
	}
}

// stops the file server by closing the listener and waiting for all
func (fs *FileServer) stop() {
	if fs.ln != nil {
		fs.ln.Close()
	}
}

// readLoop reads a file from a TCP connection and saves it to the local filesystem.
// It also sends a confirmation message back to the client and sends the filename
// to the provided channel upon successful completion.
//
// Args:
//
//	conn (net.Conn): The TCP connection from which to read the file.
//	ch (chan<- string): The channel to send the filename to upon successful completion.
//
// The function performs the following steps:
// 1. Reads the length of the filename from the connection.
// 2. Reads the filename from the connection.
// 3. Reads the size of the file from the connection.
// 4. Creates a new file with the received filename.
// 5. Reads the file data from the connection and writes it to the created file.
// 6. Sends a confirmation message ("OK") back to the client.
// 7. Sends the filename to the provided channel.
//
// If any error occurs during these steps, the function logs the error and returns early.
func (fs *FileServer) readLoop(conn net.Conn, ch chan<- string) {
	defer conn.Close()

	var filenameLen int64
	if err := binary.Read(conn, binary.LittleEndian, &filenameLen); err != nil {
		log.Println("Error reading filename length:", err)
		return
	}

	filenameBuf := make([]byte, filenameLen)
	if _, err := io.ReadFull(conn, filenameBuf); err != nil {
		log.Println("Error reading filename:", err)
		return
	}
	filename := string(filenameBuf)

	var fileSize int64
	if err := binary.Read(conn, binary.LittleEndian, &fileSize); err != nil {
		log.Println("Error reading file size:", err)
		return
	}

	file, err := os.Create(filename)
	if err != nil {
		log.Println("Error creating file:", err)
		return
	}
	defer file.Close()

	buf := make([]byte, 4*1024*1024)
	var received int64
	for received < fileSize {
		n, err := conn.Read(buf)
		if err != nil && err != io.EOF {
			log.Println("Error reading data:", err)
			return
		}
		if n == 0 {
			break
		}

		_, err = file.Write(buf[:n])
		if err != nil {
			log.Println("Error writing to file:", err)
			return
		}
		received += int64(n)
	}

	if received != fileSize {
		log.Printf("Received %d bytes, expected %d bytes\n", received, fileSize)
		return
	}

	// Send confirmation to client
	_, err = conn.Write([]byte("OK"))
	if err != nil {
		log.Println("Failed to send confirmation:", err)
	}

	fmt.Printf("Received %d/%d bytes into %s\n", received, fileSize, filename)
	ch <- filename
}

// SendFile sends a file over a TCP connection to the specified IP and port.
// It sends the filename, file size, and file content, and waits for a confirmation from the server.
//
// Parameters:
//   - path: The path to the file to be sent.
//   - ip: The IP address of the server to send the file to.
//   - port: The port number of the server to send the file to.
//
// Returns:
//   - error: An error if any occurs during the file sending process, otherwise nil.
func SendFile(path, ip, port string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	filename := filepath.Base(path)
	filenameBytes := []byte(filename)
	filenameLen := int64(len(filenameBytes))

	fi, err := file.Stat()
	if err != nil {
		return err
	}
	fileSize := fi.Size()

	conn, err := net.Dial("tcp", ip+":"+port)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := binary.Write(conn, binary.LittleEndian, filenameLen); err != nil {
		return err
	}

	if _, err := conn.Write(filenameBytes); err != nil {
		return err
	}

	if err := binary.Write(conn, binary.LittleEndian, fileSize); err != nil {
		return err
	}

	buf := make([]byte, 4*1024*1024)
	var sent int64
	for {
		n, err := file.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		wn, err := conn.Write(buf[:n])
		if err != nil {
			return err
		}
		sent += int64(wn)
	}

	if sent != fileSize {
		return fmt.Errorf("sent %d bytes, expected %d", sent, fileSize)
	}

	// Wait for confirmation from server
	confirmation := make([]byte, 2)
	_, err = conn.Read(confirmation)
	if err != nil {
		return fmt.Errorf("failed to read confirmation: %w", err)
	}
	if string(confirmation) != "OK" {
		return fmt.Errorf("server did not confirm receipt")
	}

	fmt.Printf("Sent %d/%d bytes of %s\n", sent, fileSize, filename)
	return nil
}

// func tcp() {
// 	startTime := time.Now()
// 	fs := &FileServer{}
// 	serverDone := make(chan error)

// 	// Start server in a goroutine
// 	go func() {
// 		serverDone <- fs.Start("3000")
// 	}()

// 	// Give the server a moment to start
// 	time.Sleep(100 * time.Millisecond)

// 	// Client part
// 	go func() {
// 		var path string
// 		fmt.Print("Enter file path to send: ")
// 		fmt.Scan(&path)
// 		if err := SendFile(path, "localhost", "3000"); err != nil {
// 			log.Fatal("Send file error:", err)
// 		}
// 		// Stop the server after sending
// 		fs.stop()
// 	}()

// 	// Wait for the server to finish
// 	if err := <-serverDone; err != nil {
// 		log.Fatal("Server error:", err)
// 	}

// 	fmt.Printf("Time taken: %v\n", time.Since(startTime))
// }
