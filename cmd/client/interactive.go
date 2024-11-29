// cmd/client/interactive.go

package main

import (
	"bufio"
	"flag"
	"fmt"
	clientpkg "github.com/Manthan0999/apaxos-project/internal/client"
	"github.com/Manthan0999/apaxos-project/pkg/utils"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	//"sync"
	"text/tabwriter"
	"time"
)

type PerformanceMetrics struct {
	TotalTransactions int
	TotalLatency      time.Duration
	mu                sync.Mutex
}

func main() {
	// Parse command-line argument for input file
	inputFile := flag.String("input", "", "Path to the input CSV file containing transaction sets")
	flag.Parse()

	if *inputFile == "" {
		log.Fatalf("Input file is required. Usage: ./interactive -input=path/to/transactions.csv")
	}

	// Initialize the client
	client := clientpkg.NewClient()
	defer client.CloseDBConnections()

	perfMetrics := &PerformanceMetrics{}

	// Parse the input file
	sets, err := utils.ParseInputFile(*inputFile)
	if err != nil {
		log.Fatalf("Failed to parse input file: %v", err)
	}

	reader := bufio.NewReader(os.Stdin)

	for idx, set := range sets {
		fmt.Printf("\n=== Processing Transaction Set %d ===\n", set.SetNumber)
		fmt.Printf("Number of Transactions: %d\n", len(set.Transactions))
		fmt.Printf("Live Servers: %v\n", set.LiveServers)
		fmt.Printf("Contact Servers: %v\n", set.ContactServers)

		// Process the transaction set and measure latency
		startTime := time.Now()
		client.ProcessSets([]*utils.TransactionSet{set})
		elapsed := time.Since(startTime)

		// Update performance metrics
		perfMetrics.mu.Lock()
		perfMetrics.TotalTransactions += len(set.Transactions)
		perfMetrics.TotalLatency += elapsed
		perfMetrics.mu.Unlock()

		fmt.Printf("Transaction Set %d processed in %v\n", set.SetNumber, elapsed)

		// After processing each set, provide options to the user
		for {
			fmt.Println("\n--- Post-Set Menu ---")
			fmt.Println("1. Print Account Balances")
			fmt.Println("2. Display Datastore Contents")
			fmt.Println("3. Show Performance Metrics")
			if idx < len(sets)-1 {
				fmt.Println("4. Proceed to Next Transaction Set")
			} else {
				fmt.Println("4. Finish Processing All Sets")
			}
			fmt.Print("Select an option: ")

			option, err := reader.ReadString('\n')
			if err != nil {
				log.Printf("Error reading input: %v", err)
				continue
			}
			option = strings.TrimSpace(option)

			switch option {
			case "1":
				// Print Account Balances
				fmt.Print("Enter the Account ID to view balance (1-3000): ")
				accountInput, _ := reader.ReadString('\n')
				accountInput = strings.TrimSpace(accountInput)
				accountID, err := strconv.Atoi(accountInput)
				if err != nil || accountID < 1 || accountID > 3000 {
					fmt.Println("Invalid Account ID. Please enter a number between 1 and 3000.")
					continue
				}

				balances := client.GetBalancesforClient(int32(accountID))
				shardID := getShard(int32(accountID))
				fmt.Printf("\n--- Balances for Account ID %d in Shard %s ---\n", accountID, shardID)
				fmt.Println("-------------------------------------------------")
				fmt.Printf("| %-10s | %-10s | %-10s |\n", "Account ID", "Balance", "Server")
				fmt.Println("-------------------------------------------------")
				for serverID, balance := range balances {
					balanceStr := strconv.Itoa(int(balance))
					if balance == -1 {
						balanceStr = "Unavailable"
					}
					fmt.Printf("| %-10d | %-10s | %-10s |\n", accountID, balanceStr, serverID)
				}
				fmt.Println("-------------------------------------------------")

			case "2":
				// Display Datastore Contents
				fmt.Print("Enter the Server ID to display datastore (e.g., S1): ")
				serverInput, _ := reader.ReadString('\n')
				serverInput = strings.TrimSpace(serverInput)
				if serverInput == "" {
					fmt.Println("Invalid Server ID.")
					continue
				}

				resp, err := client.GetDatastore(serverInput)
				if err != nil {
					log.Printf("Error getting datastore from server %s: %v", serverInput, err)
					continue
				}

				// Initialize tabwriter for Datastore
				datastoreWriter := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
				fmt.Printf("\n--- Server Datastore for %s ---\n", serverInput)
				fmt.Fprintln(datastoreWriter, "Server\tDatastore")
				fmt.Fprintln(datastoreWriter, "------\t---------")
				datastoreStr := strings.Join(resp.Datastore, " → ")
				fmt.Fprintf(datastoreWriter, "%s\t%s\n", serverInput, datastoreStr)
				datastoreWriter.Flush()

				// Initialize tabwriter for WAL Logs
				walWriter := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
				fmt.Println("\n----- WAL Logs -----")
				fmt.Fprintln(walWriter, "TxnID\tStatus\tSender\tReceiver\tAmount")
				fmt.Fprintln(walWriter, "-----\t------\t------\t--------\t------")
				for _, wal := range resp.Wal {
					// Example wal format: [TxnID: S1-1, Status: Prepared, Sender: 100, Receiver: 501, Amount: 8]
					wal = strings.Trim(wal, "[]")
					parts := strings.Split(wal, ", ")
					if len(parts) != 5 {
						continue // Skip malformed entries
					}
					txnID := strings.TrimPrefix(parts[0], "TxnID: ")
					status := strings.TrimPrefix(parts[1], "Status: ")
					sender := strings.TrimPrefix(parts[2], "Sender: ")
					receiver := strings.TrimPrefix(parts[3], "Receiver: ")
					amount := strings.TrimPrefix(parts[4], "Amount: ")
					fmt.Fprintf(walWriter, "%s\t%s\t%s\t%s\t%s\n", txnID, status, sender, receiver, amount)
				}
				walWriter.Flush()
				fmt.Println("----- End of WAL -----")

			case "3":
				// Show Performance Metrics
				perfMetrics.mu.Lock()
				if perfMetrics.TotalTransactions == 0 {
					fmt.Println("No transactions processed yet.")
					perfMetrics.mu.Unlock()
					continue
				}
				throughput := float64(perfMetrics.TotalTransactions) / perfMetrics.TotalLatency.Seconds()
				averageLatency := perfMetrics.TotalLatency.Seconds() / float64(perfMetrics.TotalTransactions)
				fmt.Printf("\n--- Performance Metrics ---\n")
				fmt.Printf("Total Transactions: %d\n", perfMetrics.TotalTransactions)
				fmt.Printf("Total Latency: %v\n", perfMetrics.TotalLatency)
				fmt.Printf("Throughput: %.2f transactions/second\n", throughput)
				fmt.Printf("Average Latency: %.6f seconds/transaction\n", averageLatency)
				perfMetrics.mu.Unlock()

			case "4":
				// Proceed to Next Transaction Set or Finish
				if idx < len(sets)-1 {
					fmt.Println("Proceeding to the next transaction set...")
				} else {
					fmt.Println("All transaction sets have been processed.")
					return
				}
				break
			default:
				fmt.Println("Invalid option. Please select a valid number.")
			}
			// Break to move to the next set after handling the option
			if option == "4" {
				break
			}
		}
	}

	fmt.Println("\n=== All Transaction Sets Processed ===")
	perfMetrics.mu.Lock()
	if perfMetrics.TotalTransactions > 0 {
		throughput := float64(perfMetrics.TotalTransactions) / perfMetrics.TotalLatency.Seconds()
		averageLatency := perfMetrics.TotalLatency.Seconds() / float64(perfMetrics.TotalTransactions)
		fmt.Printf("\n--- Final Performance Metrics ---\n")
		fmt.Printf("Total Transactions: %d\n", perfMetrics.TotalTransactions)
		fmt.Printf("Total Latency: %v\n", perfMetrics.TotalLatency)
		fmt.Printf("Throughput: %.2f transactions/second\n", throughput)
		fmt.Printf("Average Latency: %.6f seconds/transaction\n", averageLatency)
	}
	perfMetrics.mu.Unlock()
}

// getShard determines the shard for an account ID based on the fixed mapping
func getShard(accountID int32) string {
	if accountID >= 1 && accountID <= 1000 {
		return "D1"
	} else if accountID >= 1001 && accountID <= 2000 {
		return "D2"
	} else if accountID >= 2001 && accountID <= 3000 {
		return "D3"
	}
	return "Unknown"
}
