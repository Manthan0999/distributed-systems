package main

import (
	"flag"
	"log"

	clientpkg "github.com/Manthan0999/apaxos-project/internal/client"
	"github.com/Manthan0999/apaxos-project/pkg/utils"
)

func main() {
	// Parse command-line arguments
	inputFile := flag.String("input", "", "Path to the input CSV file containing transaction sets")
	numClusters := flag.Int("clusters", 3, "Number of clusters (default 3)")
	serversPerCluster := flag.Int("serversPerCluster", 3, "Number of servers per cluster (default 3)")
	flag.Parse()

	if *inputFile == "" {
		log.Fatalf("Input file is required. Usage: ./client -input=path/to/transactions.csv")
	}

	// Initialize the client with the specified or default number of clusters and servers per cluster
	client := clientpkg.NewClient(*numClusters, *serversPerCluster)

	// Parse the input file
	sets, err := utils.ParseInputFile(*inputFile)
	if err != nil {
		log.Fatalf("Failed to parse input file: %v", err)
	}

	// Process the transaction sets
	client.ProcessSets(sets)
}
