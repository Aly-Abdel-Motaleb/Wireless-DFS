package server

import (
	pb "DFS/dfs"
	"DFS/master/db"
	"context"
	"errors"
	"fmt"
	"log"

	"google.golang.org/grpc"
)

type MasterServer struct {
	pb.UnimplementedMasterTrackerServer
}

func NewMasterServer() *MasterServer {
	return &MasterServer{}
}

func (s *MasterServer) RequestUpload(ctx context.Context, req *pb.UploadRequest) (*pb.UploadResponse, error) {
	query := `
    SELECT dk.ip, dk.port, COUNT(fl.file_id) AS file_count
    FROM datakeepers dk
    LEFT JOIN file_locations fl ON dk.id = fl.data_keeper_id
	WHERE dk.is_alive = 1
    GROUP BY dk.id
    ORDER BY file_count ASC
    LIMIT 1;`

	var ip string
	var port string
	var fileCount int
	err := db.DB.QueryRow(query).Scan(&ip, &port, &fileCount)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.Dial(ip+":"+port, grpc.WithInsecure())
	if err != nil {
		log.Printf("Failed to dial datakeeper: %v", err)
	}
	defer conn.Close()
	client := pb.NewDataKeeperClient(conn)
	request := &pb.DataKeeperUploadRequest{Replication: false}
	dkResponse, err := client.RequestUpload(context.Background(), request)
	if err != nil {
		return nil, err
	}

	return &pb.UploadResponse{Ip: dkResponse.Ip, Port: dkResponse.Port}, nil
}

func (s *MasterServer) RequestDownload(ctx context.Context, req *pb.DownloadRequest) (*pb.DownloadResponse, error) {
	query := `
	SELECT dk.id, dk.ip, dk.port
	FROM datakeepers dk
	JOIN file_locations fl ON dk.id = fl.data_keeper_id
	JOIN files f ON fl.file_id = f.id
	WHERE f.filename = ? AND dk.is_alive = 1;
	`

	rows, err := db.DB.Query(query, req.FileName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	var ips []string
	var ports []string
	for rows.Next() {
		var id string
		var ip string
		var port string
		err := rows.Scan(&id, &ip, &port)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
		ips = append(ips, ip)
		ports = append(ports, port)
	}
	if len(ips) == 0 {
		return nil, errors.New("file not found")
	}
	var downloadPorts []string
	for i := 0; i < len(ips); i++ {
		conn, err := grpc.Dial(ips[i]+":"+ports[i], grpc.WithInsecure())
		if err != nil {
			log.Printf("Failed to dial datakeeper: %v", err)
		}
		defer conn.Close()
		client := pb.NewDataKeeperClient(conn)
		request := &pb.DownloadRequest{FileName: req.FileName}
		resp, err := client.RequestDownload(context.Background(), request)
		if err != nil {
			return nil, err
		}
		downloadPorts = append(downloadPorts, resp.Port)
	}
	return &pb.DownloadResponse{Ids: ids, Ips: ips, Ports: downloadPorts}, nil
}

func (s *MasterServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.Ack, error) {
	query := `
    INSERT OR REPLACE INTO datakeepers (id, ip, port, last_heartbeat, is_alive)
    VALUES (?, ?, ?, CURRENT_TIMESTAMP, 1);
    `

	_, err := db.DB.Exec(query, req.Id, req.Ip, req.Port)
	if err != nil {
		return nil, err
	}
	log.Printf("Heartbeat recieved from datakeeper %s", req.Id)

	return &pb.Ack{Success: true, Message: "Heartbeat received"}, nil
}

func (s *MasterServer) NotifyFileStored(ctx context.Context, req *pb.NotifyFileStoredRequest) (*pb.Ack, error) {
	// based on whether it's a replication or upload request
	// if replication request, search for file id
	// if upload request, insert file into files table

	if db.DoesFileExistByHash(req.FileHash, req.DatakeeperId) {
		response := &pb.Ack{Success: false, Message: "File already exists", ErrorCode: pb.ErrorCode_FILE_ALREADY_EXISTS}
		return response, nil
	}

	var id int
	if req.Replication {
		query := `SELECT id FROM files WHERE filename = ?;`
		err := db.DB.QueryRow(query, req.FileName).Scan(&id)
		if err != nil {
			return &pb.Ack{Success: false, Message: fmt.Sprintf("File not found: %v", err), ErrorCode: pb.ErrorCode_FILE_NOT_FOUND},
				nil
		}
	} else {
		query := `INSERT INTO files (filename, hash, size) VALUES (?, ?, ?);`
		res, err := db.DB.Exec(query, req.FileName, req.FileHash, req.FileSize)
		if err != nil {
			return nil, err
		}
		id64, err := res.LastInsertId()
		if err != nil {
			return &pb.Ack{Success: false, Message: fmt.Sprintf("Failed to get file id: %v", err), ErrorCode: pb.ErrorCode_FETAL_ERROR},
				nil
		}
		id = int(id64)
	}

	query := `INSERT INTO file_locations (file_id, data_keeper_id, filepath) VALUES (?, ?, ?);`

	log.Printf("path %s", req.FilePath)
	_, err := db.DB.Exec(query, id, req.DatakeeperId, req.FilePath)
	if err != nil {
		return &pb.Ack{Success: false, Message: fmt.Sprintf("Failed to store file location: %v", err), ErrorCode: pb.ErrorCode_FETAL_ERROR},
			nil
	}

	return &pb.Ack{Success: true, Message: "File stored"}, nil
}

func (s *MasterServer) ListFiles(ctx context.Context, req *pb.EmptyRequest) (*pb.FileDetailsResponse, error) {
	query := `
	SELECT f.id, f.filename, f.hash, f.size, f.created_at
	FROM files f;
	`

	rows, err := db.DB.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var files []*pb.FileDetails
	for rows.Next() {
		var file pb.FileDetails
		err := rows.Scan(&file.Id, &file.Name, &file.Hash, &file.Size, &file.CreatedAt)
		if err != nil {
			return nil, err
		}
		files = append(files, &file)
	}

	return &pb.FileDetailsResponse{FileDetails: files}, nil
}

func (s *MasterServer) DoesFileExist(ctx context.Context, req *pb.DownloadRequest) (*pb.Ack, error) {

	query := `
	SELECT COUNT(f.id)
	FROM files f
	WHERE f.filename = ?;
	`
	var count int
	err := db.DB.QueryRow(query, req.FileName).Scan(&count)
	if err != nil {
		return &pb.Ack{Success: false, Message: fmt.Sprintf("Failed to check file existence: %v", err), ErrorCode: pb.ErrorCode_FETAL_ERROR}, nil
	}

	if count == 0 {
		return &pb.Ack{Success: false, Message: "File does not exist", ErrorCode: pb.ErrorCode_FILE_NOT_FOUND}, nil
	}

	return &pb.Ack{Success: true, Message: "File exists"}, nil
}

func (s *MasterServer) UpdateDataKeepersAliveStatus() {
	_, err := db.DB.Exec("UPDATE datakeepers SET is_alive = 0 where last_heartbeat < datetime('now', '-10 seconds');")
	if err != nil {
		log.Fatalf("Cannot update datakeepers alive status: %v", err)
	}
}

func (s *MasterServer) ReplicateFiles() (err error) {
	//// 1. select all files that have less than 3 copies in an alive datakeeper
	//// 2. loop through each file and select a proper number of datakeepers to replicate the file to
	// 3. start copying file from source datakeeper to destination datakeeper
	// 4. update the database after replication is done0

	type File struct {
		Id             int
		FileName       string
		FileCount      int
		DataKeeper     string
		DataKeeperIp   string
		DataKeeperPort string
		FilePath       string
	}

	query := `
		SELECT f.id, f.filename, COUNT(fl.file_id) as file_count , dk.id , dk.ip, dk.port , fl.filepath
		FROM files f
		LEFT JOIN file_locations fl ON f.id = fl.file_id
		LEFT JOIN datakeepers dk ON fl.data_keeper_id = dk.id
		WHERE dk.is_alive = 1
		GROUP BY f.id
		HAVING file_count < 3;
	`
	rows, err := db.DB.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	var files []File
	for rows.Next() {
		var file File
		err := rows.Scan(&file.Id, &file.FileName, &file.FileCount, &file.DataKeeper, &file.DataKeeperIp, &file.DataKeeperPort, &file.FilePath)
		if err != nil {
			return err
		}
		files = append(files, file)
	}

	for _, file := range files {
		// select a proper number of datakeepers to replicate the file to
		numberToReplicate := 3 - file.FileCount
		// select datakeepers to replicate the file to randomly where the file doesn't exist
		query := `
		SELECT dk.id, dk.ip, dk.port
		FROM datakeepers dk
		WHERE dk.is_alive = 1 AND dk.id != ? AND dk.id NOT IN (
			SELECT fl.data_keeper_id
			FROM file_locations fl
			WHERE fl.file_id = ?
		)
		ORDER BY RANDOM()
		LIMIT ?;
		`
		rows, err := db.DB.Query(query, file.DataKeeper, file.Id, numberToReplicate)
		if err != nil {
			return err
		}
		defer rows.Close()
		var ips []string
		var ports []string
		var ids []string
		for rows.Next() {
			var id string
			var ip string
			var port string
			err := rows.Scan(&id, &ip, &port)
			if err != nil {
				return err
			}
			ips = append(ips, ip)
			ports = append(ports, port)
			ids = append(ids, id)
		}
		// start copying file from source datakeeper to destination datakeeper
		conn, err := grpc.Dial(file.DataKeeperIp+":"+file.DataKeeperPort, grpc.WithInsecure())
		if err != nil {
			log.Printf("Failed to dial datakeeper: %v", err)
		}
		defer conn.Close()
		client := pb.NewDataKeeperClient(conn)
		resp, _ := client.ReplicateFile(context.Background(), &pb.ReplicateFileRequest{FileId: int64(file.Id), Ids: ids, Ips: ips, Ports: ports, FileName: file.FileName, FilePath: file.FilePath})
		if resp != nil && !resp.Success {
			log.Printf("Failed to replicate file: %v", resp.Message)
		}

	}

	return nil
}
