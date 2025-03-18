package main

import (
	"context"
	"log"
	"net"
	"os"

	pb "DFS/dfs"
	"DFS/tcp"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"google.golang.org/grpc"
)

const masterTrackerAddr = "localhost:50051"

var bgColor = tcell.NewHexColor(0x1d1f21)
var defaultStyle = tcell.StyleDefault.Foreground(tcell.ColorWhite).Background(bgColor)

func main() {
	log.Printf("Args: %v", os.Args)
	if len(os.Args) == 1 {
		app := tview.NewApplication()
		menu := mainMenu(app)
		// menu := tview.NewList()

		// menu.SetMainTextStyle(defaultStyle)
		// menu.SetBackgroundColor(bgColor)
		// menu.SetShortcutStyle(defaultStyle)

		// menu.AddItem("Upload File", "", 'u', func() { promptFileUpload(app) })
		// menu.AddItem("Download File", "", 'd', func() { listFiles(app) })
		// menu.AddItem("Exit", "", 'q', func() { app.Stop() })

		if err := app.SetRoot(menu, true).Run(); err != nil {
			log.Fatalf("Error running TUI: %v", err)
		}
	}

	if len(os.Args) == 3 {
		if os.Args[1] == "upload" {
			uploadFile(os.Args[2])
		} else if os.Args[1] == "download" {
			downloadFile(os.Args[2], func() {})
		}
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
			go uploadFile(filePath)
			app.SetRoot(mainMenu(app), true)
		}
	})
	app.SetRoot(input, true).SetFocus(input)
}

func uploadFile(filePath string) {
	conn, err := grpc.Dial(masterTrackerAddr, grpc.WithInsecure())
	if err != nil {
		log.Println("Failed to connect to Master Tracker:", err)
		return
	}
	defer conn.Close()

	client := pb.NewMasterTrackerClient(conn)
	resp, err := client.RequestUpload(context.Background(), &pb.UploadRequest{FileName: filePath})
	if err != nil {
		log.Println("Failed to get Data Keeper node:", err)
		return
	}

	tcpConn, err := net.Dial("tcp", resp.Ip+":"+resp.Port)
	if err != nil {
		log.Println("Failed to connect to Data Keeper:", err)
		return
	}
	defer tcpConn.Close()

	err = tcp.SendFile(tcpConn, filePath)
	if err != nil {
		log.Println("File upload failed:", err)
		return
	}
	log.Println("Upload successful.")
}

func listFiles(app *tview.Application) {
	conn, err := grpc.Dial(masterTrackerAddr, grpc.WithInsecure())
	if err != nil {
		log.Println("Failed to connect to Master Tracker:", err)
		return
	}
	defer conn.Close()

	client := pb.NewMasterTrackerClient(conn)
	resp, err := client.ListFiles(context.Background(), &pb.EmptyRequest{})
	if err != nil {
		log.Println("Failed to list files:", err)
		return
	}

	listView := tview.NewList().ShowSecondaryText(false)
	listView.SetBackgroundColor(bgColor)
	listView.SetMainTextStyle(defaultStyle)
	listView.SetShortcutStyle(defaultStyle)

	for _, file := range resp.FileDetails {
		fileName := file.Name
		listView.AddItem(fileName, "", 0, func() {
			go downloadFile(fileName, func() {
				app.QueueUpdateDraw(func() {
					modal := tview.NewModal().
						SetText("Download completed successfully!").
						AddButtons([]string{"OK"}).
						SetDoneFunc(func(buttonIndex int, buttonLabel string) {
							app.SetRoot(mainMenu(app), true)
						})
					app.SetRoot(modal, true)
				})
			})
			app.SetRoot(mainMenu(app), true)
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
	menu.AddItem("Download File", "", 'd', func() { listFiles(app) })
	menu.AddItem("Exit", "", 'q', func() { app.Stop() })
	return menu
}
