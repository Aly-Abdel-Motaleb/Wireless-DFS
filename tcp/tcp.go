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
	"strconv"
)

type FileDetails struct {
	FileName string
}

func NewFileDetails(filename string) *FileDetails {
	return &FileDetails{FileName: filename}
}

type FileServer struct {
	ln net.Listener // TCP listener
	// wg sync.WaitGroup // WaitGroup to wait for all goroutines to finish
	ch             chan FileDetails
	OnFileReceived func(FileDetails)
	FileDetails    FileDetails
}

func NewFileServer() *FileServer {
	return &FileServer{}
}

func (fs *FileServer) SetFileDetails(filedetails FileDetails) {
	fs.FileDetails = filedetails
}

func (fs *FileServer) SetOnReceive(f func(FileDetails)) {
	fs.OnFileReceived = f
}

// Start starts the file server by listening on a random port.
// It returns the port number and an error if any occurs during the process.
// Args:
//   - None
//
// Returns:
//   - port (string): The port number the server is listening on.
//   - error: An error if any occurs during the process, otherwise nil.
func (fs *FileServer) Start() (port string, err error) {
	ln, err := net.Listen("tcp", ":0")
	port = strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)

	if err != nil {
		return "", fmt.Errorf("listen error: %w", err)
	}
	fs.ln = ln
	fs.ch = make(chan FileDetails, 1)
	// defer ln.Close()

	return port, nil
}

// stops the file server by closing the listener and waiting for all
func (fs *FileServer) stop() {
	if fs.ln != nil {
		fs.ln.Close()
	}
}

// WaitOnConnections waits for incoming connections on the server.
// It takes a boolean argument to determine whether the server should handle
// file downloads or uploads.
//
// Args:
//   - download (bool): A boolean value to determine whether the server should handle downloads or uploads.
//
// Returns:
//   - error: An error if any occurs during the process, otherwise nil.
func (fs *FileServer) WaitOnConnections(download bool) (err error) {
	exitChannel := make(chan bool, 1)
	for {
		conn, err := fs.ln.Accept()
		if err != nil {
			var opErr *net.OpError
			if errors.As(err, &opErr) && opErr.Err.Error() == "use of closed network connection" {
				// fs.wg.Wait()
				return nil
			}
			return fmt.Errorf("accept error: %w", err)
		} else {
			// fs.wg.Add(1)
			// go func(c net.Conn) {
			// 	defer fs.wg.Done()
			if download {
				// fs.handleClientDownload(conn, exitChannel)
				SendFile(conn, fs.FileDetails.FileName)
			} else {
				ReceiveFile(conn, exitChannel, fs.ch) // upload reads from client
			}
			val, _ := <-exitChannel
			if val {
				log.Printf("Closing server\n")
				for filedetail := range fs.ch {
					fs.OnFileReceived(filedetail)
				}
				fs.stop()
				return nil
			}
			// }(conn)
		}
	}
}

// receiveFileData reads a file from a TCP connection and saves it to the local filesystem.
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
func ReceiveFile(conn net.Conn, exit chan bool, ch chan FileDetails) {
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
	fileDetails := FileDetails{FileName: filename}

	ch <- fileDetails
	exit <- true
	close(exit)
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
func SendFile(conn net.Conn, path string) error {
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
