package main

import (
	"flag"
	"log"
	"net"

	serverpkg "github.com/Manthan0999/apaxos-project/internal/server"
	pb "github.com/Manthan0999/apaxos-project/pkg/banking"
	"google.golang.org/grpc"
)

func main() {
	// Parse command-line arguments
	serverID := flag.String("serverID", "", "Server ID (e.g., S1)")
	numClusters := flag.Int("clusters", 3, "Number of clusters (default 3)")
	serversPerCluster := flag.Int("serversPerCluster", 3, "Number of servers per cluster (default 3)")
	flag.Parse()

	if *serverID == "" {
		log.Fatalf("Server ID is required. Usage: -serverID=S1")
	}

	// Initialize the server with the specified or default number of clusters and servers per cluster
	server := serverpkg.NewServer(*serverID, *numClusters, *serversPerCluster)

	// Start the gRPC server
	listener, err := net.Listen("tcp", server.ServerAddress())
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", server.ServerAddress(), err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterBankingServiceServer(grpcServer, server)

	log.Printf("Server %s listening on %s", *serverID, server.ServerAddress())

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve gRPC server: %v", err)
	}
}
