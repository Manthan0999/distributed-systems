package client

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	pb "github.com/Manthan0999/apaxos-project/pkg/banking"
	"github.com/Manthan0999/apaxos-project/pkg/utils"
	"google.golang.org/grpc"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// Client represents a banking client
type Client struct {
	ServerMap         map[string]string  // Map server IDs to addresses
	serverShardMap    map[string]string  // Map server IDs to shard IDs
	liveServers       map[string]bool    // Map of live server IDs
	serverDBPaths     map[string]string  // Map server IDs to database file paths
	dbConnections     map[string]*sql.DB // Map server IDs to database connections
	dbConnectionsMux  sync.Mutex         // Mutex for accessing dbConnections
	numClusters       int
	serversPerCluster int
}

// NewClient initializes a new client instance
func NewClient(numClusters int, serversPerCluster int) *Client {
	client := &Client{
		ServerMap:         make(map[string]string),
		serverShardMap:    make(map[string]string),
		liveServers:       make(map[string]bool),
		serverDBPaths:     make(map[string]string),
		dbConnections:     make(map[string]*sql.DB),
		numClusters:       numClusters,
		serversPerCluster: serversPerCluster,
	}

	basePort := 50050
	serverCounter := 1

	for clusterID := 1; clusterID <= numClusters; clusterID++ {
		shardID := fmt.Sprintf("D%d", clusterID)
		for i := 1; i <= serversPerCluster; i++ {
			serverID := fmt.Sprintf("S%d", serverCounter)
			serverAddress := fmt.Sprintf("localhost:%d", basePort+serverCounter)
			client.ServerMap[serverID] = serverAddress
			client.serverShardMap[serverID] = shardID
			client.serverDBPaths[serverID] = fmt.Sprintf("server_%s.db", serverID)
			serverCounter++
		}
	}

	return client
}

// CloseDBConnections closes all database connections
func (c *Client) CloseDBConnections() {
	c.dbConnectionsMux.Lock()
	defer c.dbConnectionsMux.Unlock()
	for _, db := range c.dbConnections {
		db.Close()
	}
}

// initializeServerDBPaths initializes the database file paths for each server
func (c *Client) initializeServerDBPaths() {
	for serverID := range c.ServerMap {
		// Assuming the database files are named as "server_<serverID>.db" and are located in the same directory
		dbPath := fmt.Sprintf("server_%s.db", serverID)
		c.serverDBPaths[serverID] = dbPath
	}
}

// CloseDBConnections closes all database connections
//func (c *Client) CloseDBConnections() {
//	c.dbConnectionsMux.Lock()
//	defer c.dbConnectionsMux.Unlock()
//	for _, db := range c.dbConnections {
//		db.Close()
//	}
//}

// UpdateServersLiveServers updates the live servers across all servers in the cluster
func (c *Client) UpdateServersLiveServers(contactServers []string) {
	liveServersList := c.getLiveServersList()
	log.Printf("Client updating servers with live servers: %v", liveServersList)

	// Send UpdateLiveServers RPC to all servers, not just contact servers
	for serverID := range c.ServerMap {
		if !c.liveServers[serverID] {
			continue // Skip if the server is not live
		}
		serverAddress := c.ServerMap[serverID]
		conn, err := grpc.Dial(serverAddress, grpc.WithInsecure())
		if err != nil {
			log.Printf("Failed to connect to server %s: %v", serverID, err)
			continue
		}

		client := pb.NewBankingServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		req := &pb.LiveServersRequest{
			LiveServers:    liveServersList,
			ContactServers: contactServers,
		}
		_, err = client.UpdateLiveServers(ctx, req)
		if err != nil {
			log.Printf("Failed to update live servers on server %s: %v", serverID, err)
		}
		cancel()
		conn.Close()
	}
}

// getLiveServersList returns a slice of currently live server IDs
func (c *Client) getLiveServersList() []string {
	liveServers := make([]string, 0, len(c.liveServers))
	for serverID := range c.liveServers {
		liveServers = append(liveServers, serverID)
	}
	return liveServers
}

var (
	Counter    = 0
	CounterMux sync.Mutex
)

// Generate a unique counter safely
func generateCounter() int {
	CounterMux.Lock()
	defer CounterMux.Unlock()
	Counter++
	return Counter
}

// ProcessSets processes each transaction set
// ProcessSets processes each transaction set
func (c *Client) ProcessSets(sets []*utils.TransactionSet) {
	reader := bufio.NewReader(os.Stdin)
	for _, set := range sets {
		log.Printf("Processing Set %d", set.SetNumber)

		// Update live servers for this set
		c.updateLiveServers(set.LiveServers)
		c.UpdateServersLiveServers(set.ContactServers)
		transactions := set.Transactions

		// Collect all involved account IDs in this set
		accountsInvolved := make(map[int32]bool)
		var accountsMutex sync.Mutex

		// Process transactions concurrently
		var wg sync.WaitGroup
		for _, txn := range transactions {
			currentCounter := generateCounter()
			wg.Add(1)
			go func(txn *utils.Transaction, counter int) {
				defer wg.Done()
				c.processTransaction(txn, counter, set.ContactServers)
				accountsMutex.Lock()
				accountsInvolved[txn.Sender] = true
				accountsInvolved[txn.Receiver] = true
				accountsMutex.Unlock()
			}(txn, currentCounter)
		}
		wg.Wait()

		// Display balances of all involved accounts in a table format
		fmt.Printf("\nAccount balances after processing Set %d:\n", set.SetNumber)
		fmt.Println("-------------------------------------------------------------------")
		fmt.Printf("| %-10s | %-12s | %-10s |\n", "Account ID", "Balance", "Server")
		fmt.Println("-------------------------------------------------------------------")

		// Get balances for all involved accounts via RPC
		accountIDs := make([]int32, 0, len(accountsInvolved))
		for accountID := range accountsInvolved {
			accountIDs = append(accountIDs, accountID)
		}
		// Sort account IDs for consistent output
		sort.Slice(accountIDs, func(i, j int) bool { return accountIDs[i] < accountIDs[j] })

		for _, accountID := range accountIDs {
			balances := c.GetBalancesforClient(accountID)
			for serverID, balance := range balances {
				if balance == -1 {
					fmt.Printf("| %-10d | %-12s | %-10s |\n", accountID, "Unavailable", serverID)
				} else {
					fmt.Printf("| %-10d | %-12d | %-10s |\n", accountID, balance, serverID)
				}
			}
		}
		fmt.Println("-------------------------------------------------------------------")

		// Prompt user before processing the next set
		fmt.Println("\nPress Enter to continue to the next set...")
		reader.ReadString('\n')
	}
	// Close all database connections after processing
	c.CloseDBConnections()
}

// GetBalances retrieves and returns the balances of a specific account from all servers in its shard
func (c *Client) GetBalances(txn *utils.Transaction, accountID int32) map[string]int32 {
	// Map from serverID to balance
	balances := make(map[string]int32)
	shardID := getShard(txn.Sender, c.numClusters)

	for serverID, shard := range c.serverShardMap {
		if shard != shardID {
			continue
		}
		balance := c.getBalanceFromDatabase(serverID, accountID)
		balances[serverID] = balance
	}

	return balances
}

// getBalanceFromDatabase reads the balance of an account directly from a server's database file
func (c *Client) getBalanceFromDatabase(serverID string, accountID int32) int32 {
	c.dbConnectionsMux.Lock()
	db, exists := c.dbConnections[serverID]
	if !exists {
		// Open the database connection
		dbPath, pathExists := c.serverDBPaths[serverID]
		if !pathExists {
			log.Printf("Database path for server %s not found", serverID)
			c.dbConnectionsMux.Unlock()
			return -1
		}
		var err error
		db, err = sql.Open("sqlite3", dbPath)
		if err != nil {
			log.Printf("Failed to open database for server %s: %v", serverID, err)
			c.dbConnectionsMux.Unlock()
			return -1
		}
		c.dbConnections[serverID] = db
	}
	c.dbConnectionsMux.Unlock()

	var balance int32
	err := db.QueryRow("SELECT balance FROM accounts WHERE account_id = ?;", accountID).Scan(&balance)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Printf("Account %d not found in database of server %s", accountID, serverID)
		} else {
			log.Printf("Error retrieving balance from server %s: %v", serverID, err)
		}
		return -1
	}
	return balance
}

// updateLiveServers updates the liveServers map based on the provided list
func (c *Client) updateLiveServers(liveServers []string) {
	c.liveServers = make(map[string]bool)
	for _, serverID := range liveServers {
		c.liveServers[serverID] = true
	}
}

// processTransaction processes a single transaction (either intra-shard or cross-shard)
func (c *Client) processTransaction(txn *utils.Transaction, Counter int, contactServers []string) {
	senderShard := getShard(txn.Sender, c.numClusters)
	receiverShard := getShard(txn.Receiver, c.numClusters)

	contactServerSender := c.getContactServer(senderShard, contactServers)
	contactServerReceiver := c.getContactServer(receiverShard, contactServers)

	if contactServerSender == "" || contactServerReceiver == "" {
		log.Printf("No contact server available for shards involved in the transaction (%d, %d, %d)", txn.Sender, txn.Receiver, txn.Amount)
		return
	}

	log.Printf("[Client] Processing transaction (%d, %d, %d)", txn.Sender, txn.Receiver, txn.Amount)

	if senderShard == receiverShard {
		// Intra-shard transaction
		c.handleIntraShardTransaction(contactServerSender, Counter, txn)
	} else {
		// Cross-shard transaction
		c.handleCrossShardTransaction(contactServerSender, contactServerReceiver, Counter, txn)
	}
}

// getContactServer retrieves the contact server for a given shard
func (c *Client) getContactServer(shardID string, contactServers []string) string {
	// First, try to find a contact server in the contactServers list
	for _, serverID := range contactServers {
		if c.serverShardMap[serverID] == shardID && c.liveServers[serverID] {
			return serverID
		}
	}

	// If no contact server specified for the shard, fall back to any live server in the shard
	for serverID, shard := range c.serverShardMap {
		if shard == shardID && c.liveServers[serverID] {
			return serverID
		}
	}
	return ""
}

// handleIntraShardTransaction handles an intra-shard transaction
func (c *Client) handleIntraShardTransaction(serverID string, Counter int, txn *utils.Transaction) {
	serverAddress, exists := c.ServerMap[serverID]
	if !exists {
		log.Printf("Invalid server ID %s for intra-shard transaction", serverID)
		return
	}

	log.Printf("[Client] Initiating intra-shard transaction on server %s: (%d, %d, %d)", serverID, txn.Sender, txn.Receiver, txn.Amount)

	conn, err := grpc.Dial(serverAddress, grpc.WithInsecure())
	if err != nil {
		log.Printf("Failed to connect to server %s: %v", serverID, err)
		return
	}
	defer conn.Close()

	client := pb.NewBankingServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	req := &pb.TransactionRequest{
		Sender:   txn.Sender,
		Receiver: txn.Receiver,
		Amount:   txn.Amount,
		TxnId:    strconv.Itoa(Counter),
	}

	resp, err := client.IntraShardTransaction(ctx, req)
	if err != nil {
		log.Printf("[Client] Intra-shard transaction failed on server %s: %v", serverID, err)
		return
	}

	log.Printf("[Client] Intra-shard transaction response from server %s: %v", serverID, resp.Message)
}

// generateTxnID generates a unique transaction ID based on the current timestamp
var txnIDCounter int32 = 0
var txnIDMutex sync.Mutex

func generateTxnID() int32 {
	txnIDMutex.Lock()
	defer txnIDMutex.Unlock()
	txnIDCounter++
	return txnIDCounter
}

// sendCrossShardPrepare sends a prepare request to a server for a cross-shard transaction
func (c *Client) sendCrossShardPrepare(serverID string, txn *utils.Transaction, txnID int32) bool {
	serverAddress, exists := c.ServerMap[serverID]
	if !exists {
		log.Printf("[Client] Invalid server ID %s for cross-shard prepare", serverID)
		return false
	}

	log.Printf("[Client] Sending CrossShardPrepare to server %s for txn %d", serverID, txnID)

	conn, err := grpc.Dial(serverAddress, grpc.WithInsecure())
	if err != nil {
		log.Printf("[Client] Failed to connect to server %s: %v", serverID, err)
		return false
	}
	defer conn.Close()

	client := pb.NewBankingServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &pb.CrossShardPrepareRequest{
		Sender:   txn.Sender,
		Receiver: txn.Receiver,
		Amount:   txn.Amount,
		TxnId:    strconv.Itoa(int(txnID)),
	}

	resp, err := client.CrossShardPrepare(ctx, req)
	if err != nil {
		log.Printf("[Client] Cross-shard prepare failed on server %s: %v: %v", serverID, txn, err)
		return false
	}

	if resp.Success {
		log.Printf("[Client] Cross-shard prepare successful on server %s: %v:", serverID, txn)
		return true
	}
	return false
}

// getShard determines the shard for an account ID
func getShard(accountID int32, numClusters int) string {
	totalAccounts := int32(3000)
	accountsPerShard := totalAccounts / int32(numClusters)
	shardNum := ((accountID - 1) / accountsPerShard) + 1
	if shardNum > int32(numClusters) {
		shardNum = int32(numClusters) // Handle edge cases
	}
	//log.Printf("getSGard D%d", shardNum)
	return fmt.Sprintf("D%d", shardNum)
}

// GetServerAddress returns the address of the given server ID.
func (c *Client) GetServerAddress(serverID string) string {
	address, exists := c.ServerMap[serverID]
	if !exists {
		log.Printf("Server ID %s not found in serverMap.", serverID)
		return ""
	}
	return address
}

func (c *Client) GetBalancesforClient(accountID int32) map[string]int32 {
	balances := make(map[string]int32)
	var wg sync.WaitGroup
	var mu = sync.Mutex{}

	shardID := getShard(accountID, c.numClusters)
	for serverID, addr := range c.ServerMap {
		if c.serverShardMap[serverID] != shardID {
			continue // Skip servers not in the shard
		}
		wg.Add(1)
		go func(sID, address string) {
			defer wg.Done()
			conn, err := grpc.Dial(address, grpc.WithInsecure())
			if err != nil {
				log.Printf("Failed to connect to server %s: %v", sID, err)
				mu.Lock()
				balances[sID] = -1
				mu.Unlock()
				return
			}
			defer conn.Close()

			client := pb.NewBankingServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			req := &pb.AccountRequest{
				AccountId: accountID,
			}

			resp, err := client.GetBalance(ctx, req)
			if err != nil {
				log.Printf("Error getting balance from server %s: %v", sID, err)
				mu.Lock()
				balances[sID] = -1
				mu.Unlock()
				return
			}

			mu.Lock()
			balances[sID] = resp.Balance
			mu.Unlock()
		}(serverID, addr)
	}

	wg.Wait()
	return balances
}

// GetDatastore fetches the datastore contents from the specified server.
func (c *Client) GetDatastore(serverID string) (*pb.GetDatastoreResponse, error) {
	serverAddress, exists := c.ServerMap[serverID]
	if !exists {
		return nil, fmt.Errorf("server address for %s not found", serverID)
	}

	conn, err := grpc.Dial(serverAddress, grpc.WithInsecure())
	if err != nil {
		log.Printf("Failed to connect to server %s at %s: %v", serverID, serverAddress, err)
		return nil, err
	}
	defer conn.Close()

	client := pb.NewBankingServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb.GetDatastoreRequest{
		ServerId: serverID,
	}

	resp, err := client.GetDatastore(ctx, req)
	if err != nil {
		log.Printf("GetDatastore RPC to server %s failed: %v", serverID, err)
		return nil, err
	}

	if resp.Message != "Datastore retrieved successfully" {
		log.Printf("GetDatastore RPC to server %s returned message: %s", serverID, resp.Message)
	}

	return resp, nil
}

func (c *Client) UpdateLiveServers(liveServers []string, contactServers []string) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	successCount := 0

	for _, serverID := range liveServers {
		serverAddress, exists := c.ServerMap[serverID]
		if !exists {
			log.Printf("Server ID %s not found in serverMap. Skipping.", serverID)
			continue
		}

		wg.Add(1)
		go func(id, addr string) {
			defer wg.Done()

			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				log.Printf("Failed to connect to server %s at %s: %v", id, addr, err)
				return
			}
			defer conn.Close()

			client := pb.NewBankingServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			req := &pb.LiveServersRequest{
				LiveServers:    liveServers,
				ContactServers: contactServers,
			}

			resp, err := client.UpdateLiveServers(ctx, req)
			if err != nil {
				log.Printf("UpdateLiveServers RPC to server %s failed: %v", id, err)
				return
			}

			if resp.Message != "Live servers updated" {
				log.Printf("Server %s responded with message: %s", id, resp.Message)
				return
			}

			mu.Lock()
			successCount++
			mu.Unlock()

			log.Printf("Successfully updated live servers on server %s", id)
		}(serverID, serverAddress)
	}

	wg.Wait()

	log.Printf("UpdateLiveServers: %d/%d servers updated successfully.", successCount, len(liveServers))
}
