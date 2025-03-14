package server

import (
	pb "DFS/dfs"
	"DFS/master/db"
	"context"
	"errors"
)

type MasterServer struct {
	pb.UnimplementedMasterTrackerServer
}

func NewMasterServer() *MasterServer {
	return &MasterServer{}
}

func (s *MasterServer) requestUpload(ctx context.Context, req *pb.UploadRequest) (*pb.UploadResponse, error) {
	query := `
	SELECT dk.ip, dk.port, COUNT(fl.file_id) AS file_count
	FROM datakeepers dk
	LEFT JOIN file_locations fl ON dk.id = fl.data_keeper_id
	GROUP BY dk.id
	ORDER BY file_count ASC
	LIMIT 1;`

	var ip string
	var port int32
	err := db.DB.QueryRow(query).Scan(&ip, &port)
	if err != nil {
		return nil, err
	}

	return &pb.UploadResponse{Ip: ip, Port: port}, nil
}

func (s *MasterServer) requestDownload(ctx context.Context, req *pb.DownloadRequest) (*pb.DownloadResponse, error) {
	query := `
	SELECT dk.ip, dk.port
	FROM datakeepers dk
	JOIN file_locations fl ON dk.id = fl.data_keeper_id
	JOIN files f ON fl.file_id = f.id
	WHERE f.filename = ?;
	`

	rows, err := db.DB.Query(query, req.FileName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ips []string
	var ports []int32
	for rows.Next() {
		var ip string
		var port int32
		err := rows.Scan(&ip, &port)
		if err != nil {
			return nil, err
		}
		ips = append(ips, ip)
		ports = append(ports, port)
	}
	if len(ips) > 0 {
		return &pb.DownloadResponse{Ips: ips, Ports: ports}, nil
	}
	return nil, errors.New("File not found")
}

func (s *MasterServer) heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.Ack, error) {
	query := `
	INSERT OR REPLACE INTO data_keepers (ip, port, last_heartbeat, is_alive)
	VALUES (?, ?, CURRENT_TIMESTAMP, 1);
	`

	_, err := db.DB.Exec(query, req.Ip, req.Port)
	if err != nil {
		return nil, err
	}

	return &pb.Ack{Success: true, Message: "Heartbeat received"}, nil
}

func (s *MasterServer) notifyFileStored(ctx context.Context, req *pb.NotifyFileStoredRequest) (*pb.Ack, error) {
	// based on wether it's a replication or upload request
	// if replication request, search for file id
	// if upload request, insert file into files table

	var id int
	if req.Replication {
		query := `SELECT id FROM files WHERE filename = ?;`
		err := db.DB.QueryRow(query, req.FileName).Scan(&id)
		if err != nil {
			return nil, err
		}
	} else {
		query := `INSERT INTO files (filename) VALUES (?);`
		res, err := db.DB.Exec(query, req.FileName)
		if err != nil {
			return nil, err
		}
		id64, err := res.LastInsertId()
		if err != nil {
			return nil, err
		}
		id = int(id64)
	}

	query := `INSERT INTO file_locations (file_id, data_keeper_id, filepath) VALUES (?, ?, ?);`

	_, err := db.DB.Exec(query, id, req.DatakeeperId, req.FilePath)
	if err != nil {
		return nil, err
	}

	return nil, nil
}
