package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"

	pb "DFS/dfs"
	"DFS/tcp"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"google.golang.org/grpc"
)

const masterTrackerAddr = "192.168.1.15:50051"

var bgColor = tcell.NewHexColor(0x1d1f21)
var defaultStyle = tcell.StyleDefault.Foreground(tcell.ColorWhite).Background(bgColor)

func main() {
	log.Printf("Args: %v", os.Args)
	if len(os.Args) == 1 {
		app := tview.NewApplication()
		menu := mainMenu(app)
		if err := app.SetRoot(menu, true).Run(); err != nil {
			log.Fatalf("Error running TUI: %v", err)
		}
	}

	switch os.Args[1] {
	case "upload":
		if len(os.Args) <= 3 {
			uploadFile(os.Args[2], func(message string) {
				log.Println(message)
			})
		} else {
			log.Println("Usage: go run main.go upload <file_path>")
		}
	case "download":
		if len(os.Args) <= 3 {
			downloadFile(os.Args[2], func() {})
		} else {
			log.Println("Usage: go run main.go upload <file_path>")
		}
	case "list":
		files, err := listFiles()
		fmt.Printf("%-20s %-10s\n", "Name", "Size")
		for _, file := range files {
			if err != nil {
				log.Printf("Error listing files: %v", err)
				continue
			}
			fmt.Printf("%-20s %-.2f MB\n", file.Name, float64(file.Size)/(1024*1024))
		}
	default:
		log.Println("Invalid command.")
	}
}

func promptFileUpload(app *tview.Application) {
	input := tview.NewInputField()

	input.SetBackgroundColor(bgColor)
	input.SetFieldBackgroundColor(bgColor)
	input.SetFieldTextColor(tcell.ColorWhite)
	input.SetLabelStyle(defaultStyle)

	input.SetLabel("Enter file path: ").SetDoneFunc(func(key tcell.Key) {
		if key == tcell.KeyEnter {
			filePath := input.GetText()
			func() {
				go uploadFile(filePath, func(message string) {
					app.QueueUpdateDraw(func() {
						modal := tview.NewModal().
							SetText(message).
							AddButtons([]string{"OK"}).
							SetDoneFunc(func(buttonIndex int, buttonLabel string) {
								app.SetRoot(mainMenu(app), true)
							})
						app.SetRoot(modal, true)
					})
				})
			}()

		}
	})
	app.SetRoot(input, true).SetFocus(input)
}

func uploadFile(filePath string, finish func(string)) {

	conn, err := grpc.Dial(masterTrackerAddr, grpc.WithInsecure())
	if err != nil {
		message := fmt.Sprintf("Failed to connect to Master Tracker: %v", err)
		finish(message)
	}
	defer conn.Close()

	client := pb.NewMasterTrackerClient(conn)
	resp, err := client.RequestUpload(context.Background(), &pb.UploadRequest{FileName: filePath})
	if err != nil {
		message := fmt.Sprintf("Failed to get Data Keeper node: %v", err)
		finish(message)
	}

	tcpConn, err := net.Dial("tcp", resp.Ip+":"+resp.Port)
	if err != nil {
		message := fmt.Sprintf("Failed to connect to Data Keeper: %v", err)
		finish(message)
	}
	defer tcpConn.Close()

	err = tcp.SendFile(tcpConn, filePath)
	if err != nil {
		message := fmt.Sprintf("File upload failed: %v", err)
		finish(message)
	}

	ack, err := client.DoesFileExist(context.Background(), &pb.DownloadRequest{FileName: filePath})
	if err != nil {
		message := fmt.Sprintf("File upload failed: %v", err)
		finish(message)
	}

	if ack != nil && ack.Success {
		message := "File uploaded successfully."
		finish(message)
	}
}

func listFiles() ([]*pb.FileDetails, error) {
	conn, err := grpc.Dial(masterTrackerAddr, grpc.WithInsecure())
	if err != nil {
		log.Println("Failed to connect to Master Tracker:", err)
		return nil, err
	}
	defer conn.Close()

	client := pb.NewMasterTrackerClient(conn)
	resp, err := client.ListFiles(context.Background(), &pb.EmptyRequest{})
	if err != nil {
		log.Println("Failed to list files:", err)
		return nil, err
	}

	return resp.FileDetails, nil
}
func listFilesTui(app *tview.Application) {
	files, err := listFiles()
	if err != nil {
		log.Println("Failed to list files:", err)
		return
	}

	listView := tview.NewList().ShowSecondaryText(false)
	listView.SetBackgroundColor(bgColor)
	listView.SetMainTextStyle(defaultStyle)
	listView.SetShortcutStyle(defaultStyle)

	for _, file := range files {
		fileName := file.Name
		listView.AddItem(fileName, "", 0, func() {
			go downloadFile(fileName, func() {
				app.QueueUpdateDraw(func() {
					modal := tview.NewModal().
						SetText("Download completed successfully!").
						AddButtons([]string{"OK"}).
						SetDoneFunc(func(buttonIndex int, buttonLabel string) {
							app.SetRoot(listView, true) // Return to the list view after closing the modal
						})
					app.SetRoot(modal, true)
				})
			})
		})
	}
	listView.AddItem("Back", "", 'b', func() { app.SetRoot(mainMenu(app), true) })
	app.SetRoot(listView, true)
}

func downloadFile(fileName string, reportSuccess func()) {
	conn, err := grpc.Dial(masterTrackerAddr, grpc.WithInsecure())
	if err != nil {
		log.Println("Failed to connect to Master Tracker:", err)
		return
	}
	defer conn.Close()

	client := pb.NewMasterTrackerClient(conn)
	resp, err := client.RequestDownload(context.Background(), &pb.DownloadRequest{FileName: fileName})
	if err != nil {
		log.Println("Failed to get Data Keeper nodes:", err)
		return
	}

	id, ip, port := resp.Ids[0], resp.Ips[0], resp.Ports[0]
	tcpConn, err := net.Dial("tcp", ip+":"+port)
	if err != nil {
		log.Println("Error connecting:", err)
		return
	}
	defer tcpConn.Close()

	ch := make(chan tcp.FileDetails)
	exit := make(chan bool)
	go tcp.ReceiveFile(tcpConn, exit, ch, id, ".")

	select {
	case _ = <-ch:
		// log.Println("File received:", fileDetails.FileName)
		reportSuccess()
	case <-exit:
		// log.Println("Download failed.")
	}
}

func mainMenu(app *tview.Application) *tview.List {
	menu := tview.NewList().ShowSecondaryText(false)

	menu.SetMainTextStyle(defaultStyle)
	menu.SetBackgroundColor(bgColor)
	menu.SetShortcutStyle(defaultStyle)

	menu.AddItem("Upload File", "", 'u', func() { promptFileUpload(app) })
	menu.AddItem("Download File", "", 'd', func() { listFilesTui(app) })
	menu.AddItem("Exit", "", 'q', func() { app.Stop() })
	return menu
}
