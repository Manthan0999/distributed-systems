// cmd/server/main.go

package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"github.com/Manthan0999/apaxos-project/internal/server"
	pb "github.com/Manthan0999/apaxos-project/pkg/banking"
	"google.golang.org/grpc"
)

var (
	serverID          = flag.Int("serverID", 1, "Server ID")
	numClusters       = flag.Int("clusters", 3, "Number of clusters (shards)")
	serversPerCluster = flag.Int("serversPerCluster", 3, "Number of servers per cluster")
)

func main() {
	flag.Parse()
	if len(os.Args) < 5 {
		log.Fatalf("Usage: go run main.go [ServerID] [ShardID] [Port] [ClusterServers(comma-separated)]")
	}

	serverID := os.Args[1] // e.g., "S1"
	shardID := os.Args[2]  // e.g., "D1"
	port := os.Args[3]
	clusterServers := strings.Split(os.Args[4], ",") // e.g., "S2,S3"

	address := fmt.Sprintf("localhost:%s", port)

	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", address, err)
	}

	grpcServer := grpc.NewServer()
	dbPath := fmt.Sprintf("server_%s.db", serverID)
	srv := server.NewServer(shardID, serverID, dbPath, clusterServers)
	pb.RegisterBankingServiceServer(grpcServer, srv)

	log.Printf("Server %s for shard %s listening on %s", serverID, shardID, address)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve gRPC server: %v", err)
	}
}
