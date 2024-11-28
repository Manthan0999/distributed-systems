package main

import (
	"log"
	"os"

	clientpkg "github.com/Manthan0999/apaxos-project/internal/client"
	"github.com/Manthan0999/apaxos-project/pkg/utils"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("Usage: go run main.go [InputFile]")
	}

	inputFile := os.Args[1]

	// Parse the input file
	sets, err := utils.ParseInputFile(inputFile)
	if err != nil {
		log.Fatalf("Failed to parse input file: %v", err)
	}

	// Initialize the client
	client := clientpkg.NewClient()

	// Process the transaction sets
	client.ProcessSets(sets)
}
