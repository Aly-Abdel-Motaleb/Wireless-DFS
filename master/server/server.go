package server

import (
	pb "DFS/dfs"
)

type MasterServer struct {
	pb.UnimplementedMasterTrackerServer
}

func (s *MasterServer) requestUpload(req *pb.UploadRequest) (*pb.UploadResponse, error) {
	return nil, nil
}

func (s *MasterServer) requestDownload(req *pb.DownloadRequest) (*pb.DownloadResponse, error) {
	return nil, nil
}

func (s *MasterServer) heartbeat(req *pb.HeartbeatRequest) (*pb.Ack, error) {
	return nil, nil
}

func (s *MasterServer) notifyFileStored(req *pb.NotifyFileStoredRequest) (*pb.Ack, error) {
	return nil, nil
}
