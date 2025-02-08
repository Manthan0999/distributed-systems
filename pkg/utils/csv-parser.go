// csv-parser.go
package utils

import (
	"encoding/csv"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// Transaction represents a basic transaction structure
type Transaction struct {
	Sender   int32
	Receiver int32
	Amount   int32
	//Index  int32
}

// TransactionSet represents a set of transactions, live servers, and contact servers
type TransactionSet struct {
	SetNumber      int
	Transactions   []*Transaction
	LiveServers    []string
	ContactServers []string
}

// ParseInputFile parses the custom input file format and returns a slice of TransactionSets
func ParseInputFile(filename string) ([]*TransactionSet, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open input file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1 // Allow varying field counts

	var sets []*TransactionSet
	var currentSet *TransactionSet

	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("error reading CSV file: %v", err)
	}

	for _, record := range records {
		// Trim whitespace and ignore empty lines
		for i := range record {
			record[i] = strings.TrimSpace(record[i])
		}
		if len(record) == 0 || (len(record) == 1 && record[0] == "") {
			continue
		}

		// Check if it's the start of a new set
		if record[0] != "" {
			// Start of a new set
			setNumber, err := strconv.Atoi(record[0])
			if err != nil {
				return nil, fmt.Errorf("invalid Set Number: %v", err)
			}

			// Parse the first transaction
			transactions := parseTransactions(record[1])

			// Parse Live Servers (third field)
			var liveServers []string
			if len(record) >= 3 {
				liveServers = parseServers(record[2])
			}

			// Parse Contact Servers (fourth field)
			var contactServers []string
			if len(record) >= 4 {
				contactServers = parseServers(record[3])
			}

			// Create a new TransactionSet
			currentSet = &TransactionSet{
				SetNumber:      setNumber,
				Transactions:   transactions,
				LiveServers:    liveServers,
				ContactServers: contactServers,
			}
			sets = append(sets, currentSet)
		} else {
			// Additional transactions in the current set
			if currentSet == nil {
				return nil, fmt.Errorf("transaction found without a set number")
			}
			transactions := parseTransactions(record[1])
			currentSet.Transactions = append(currentSet.Transactions, transactions...)
		}
	}

	return sets, nil
}

// parseTransactions parses the transactions field
func parseTransactions(transactionsField string) []*Transaction {
	var transactions []*Transaction
	reTransaction := regexp.MustCompile(`\((\d+),\s*(\d+),\s*(\d+)\)`)
	matches := reTransaction.FindAllStringSubmatch(transactionsField, -1)

	for _, match := range matches {
		sender, _ := strconv.Atoi(match[1])
		receiver, _ := strconv.Atoi(match[2])
		amount, _ := strconv.Atoi(match[3])

		transactions = append(transactions, &Transaction{
			Sender:   int32(sender),
			Receiver: int32(receiver),
			Amount:   int32(amount),
		})
	}

	return transactions
}

// parseServers parses the servers field
func parseServers(serversField string) []string {
	serversField = strings.Trim(serversField, "[]")
	servers := strings.Split(serversField, ",")
	for i := range servers {
		servers[i] = strings.TrimSpace(servers[i])
	}
	return servers
}
