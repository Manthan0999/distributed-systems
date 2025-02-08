// internal/server/server.go

package server

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	//"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/Manthan0999/apaxos-project/pkg/banking"
	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/grpc"
)

var serverIDMap = map[string]int32{
	"S1": 1,
	"S2": 2,
	"S3": 3,
	"S4": 4,
	"S5": 5,
	"S6": 6,
	"S7": 7,
	"S8": 8,
	"S9": 9,
}

type Server struct {
	pb.UnimplementedBankingServiceServer
	mu sync.Mutex

	// Server and cluster information
	serverID          string
	serverIDAsInt     int32
	shardID           string
	serverAddress     string
	clusterServers    []string
	liveServers       map[string]bool
	isContactServer   bool
	acceptedBallots   map[int32]int32 // Map from account ID to acceptedBallot
	acceptedValues    map[int32]*pb.Transaction
	txnIDGenerator    *TransactionIDGenerator
	numClusters       int
	serversPerCluster int

	// Write-Ahead Log (WAL) for Cross-Shard Transactions
	wal      map[string]*pb.Transaction // Change key to string for txn.ID
	walMutex sync.Mutex

	// Paxos-related fields
	currentBallot int32

	// Datastore: sequence of committed transactions
	committedTxns []*DatastoreEntry

	// Database
	db *sql.DB

	// Locks for accounts
	locks      map[int32]*TryMutex
	locksMutex sync.Mutex // Use sync.Mutex instead of TryMutex
	txnMutex   sync.Mutex
}

type DatastoreEntry struct {
	TxnID string // Change to string
	Phase string // "P" for Prepare, "C" for Commit, "" for intra-shard transactions
	Txn   *pb.Transaction
}

type TransactionIDGenerator struct {
	mu       sync.Mutex
	counter  uint64
	serverID string
}

// NewTransactionIDGenerator creates a new TransactionIDGenerator
func NewTransactionIDGenerator(serverID string) *TransactionIDGenerator {
	return &TransactionIDGenerator{
		counter:  0,
		serverID: serverID,
	}
}

// NextID generates the next unique transaction ID
func (gen *TransactionIDGenerator) NextID() string {
	gen.mu.Lock()
	defer gen.mu.Unlock()
	gen.counter++
	return fmt.Sprintf("%s-%d", gen.serverID, gen.counter)
}

var serverAddressMap = map[string]string{
	"S1": "localhost:50051",
	"S2": "localhost:50052",
	"S3": "localhost:50053",
	"S4": "localhost:50054",
	"S5": "localhost:50055",
	"S6": "localhost:50056",
	"S7": "localhost:50057",
	"S8": "localhost:50058",
	"S9": "localhost:50059",
}

type TryMutex struct {
	ch     chan struct{}
	holder string // Add this field to track which transaction holds the lock
}

func NewTryMutex() *TryMutex {
	m := &TryMutex{ch: make(chan struct{}, 1)}
	m.ch <- struct{}{} // Initialize the channel with a token
	return m
}

func (tm *TryMutex) Lock() {
	<-tm.ch // Block until we can receive a token
}

func (tm *TryMutex) Unlock() {
	select {
	case tm.ch <- struct{}{}:
		// Successfully released the lock
		tm.holder = ""
	default:
		log.Printf("unlock of unlocked mutex")
	}
}

func (tm *TryMutex) TryLock() bool {
	select {
	case <-tm.ch:
		// Successfully acquired the lock
		return true
	default:
		// Failed to acquire the lock
		return false
	}
}

// appendToDatastore appends a transaction to the committed transactions
func (s *Server) appendToDatastore(txnID string, phase string, txn *pb.Transaction) {
	entry := &DatastoreEntry{
		TxnID: txnID,
		Phase: phase,
		Txn:   txn,
	}
	s.committedTxns = append(s.committedTxns, entry)
}

// NewServer initializes a new server instance
func NewServer(serverID string, numClusters int, serversPerCluster int) *Server {
	// Determine the shard ID for the server
	sIDNum, err := strconv.Atoi(strings.TrimPrefix(serverID, "S"))
	if err != nil {
		log.Fatalf("Invalid server ID: %s", serverID)
	}

	shardNum := ((sIDNum - 1) / serversPerCluster) + 1
	shardID := fmt.Sprintf("D%d", shardNum)
	serverAddress := fmt.Sprintf("localhost:%d", 50050+sIDNum)

	dbPath := fmt.Sprintf("server_%s.db", serverID)
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	s := &Server{
		shardID:           shardID,
		serverID:          serverID,
		serverIDAsInt:     int32(sIDNum),
		serverAddress:     serverAddress,
		liveServers:       make(map[string]bool),
		committedTxns:     make([]*DatastoreEntry, 0),
		locks:             make(map[int32]*TryMutex),
		wal:               make(map[string]*pb.Transaction),
		acceptedBallots:   make(map[int32]int32),
		acceptedValues:    make(map[int32]*pb.Transaction),
		txnIDGenerator:    NewTransactionIDGenerator(serverID),
		numClusters:       numClusters,
		serversPerCluster: serversPerCluster,
		db:                db,
	}

	// Initialize accounts and load committed transactions
	s.createTables()
	s.initializeAccounts()
	s.loadCommittedTransactions()

	return s
}

func (s *Server) GetDatastore(ctx context.Context, req *pb.GetDatastoreRequest) (*pb.GetDatastoreResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("[GetDatastore] Server %s received GetDatastore request for server %s", s.serverID, req.ServerId)

	datastoreContents := make([]string, 0, len(s.committedTxns))
	for _, entry := range s.committedTxns {
		if entry.Phase != "" {
			txStr := fmt.Sprintf("[<%s, %s>, (%d, %d, %d)]", entry.TxnID, entry.Phase, entry.Txn.Sender, entry.Txn.Receiver, entry.Txn.Amount)
			datastoreContents = append(datastoreContents, txStr)
		} else {
			txStr := fmt.Sprintf("[<%s>, (%d, %d, %d)]", entry.TxnID, entry.Txn.Sender, entry.Txn.Receiver, entry.Txn.Amount)
			datastoreContents = append(datastoreContents, txStr)
		}
	}

	// Include WAL entries
	walContents := make([]string, 0, len(s.wal))
	for txnID, txn := range s.wal {
		walStr := fmt.Sprintf("TxnID: %s, Status: %s, Sender: %d, Receiver: %d, Amount: %d",
			txnID, txn.Status, txn.Sender, txn.Receiver, txn.Amount)
		walContents = append(walContents, walStr)
	}

	return &pb.GetDatastoreResponse{
		Datastore: datastoreContents,
		Wal:       walContents, // Add WAL field in proto
		Message:   "Datastore and WAL retrieved successfully",
	}, nil
}
func (s *Server) UpdateLiveServers(ctx context.Context, req *pb.LiveServersRequest) (*pb.LiveServersResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Update liveServers without overwriting the entire map
	for serverID := range s.liveServers {
		s.liveServers[serverID] = false
	}
	for _, serverID := range req.LiveServers {
		s.liveServers[serverID] = true
	}
	s.liveServers[s.serverID] = true // Ensure the server includes itself

	s.isContactServer = false
	for _, serverID := range req.ContactServers {
		if serverID == s.serverID {
			s.isContactServer = true
			break
		}
	}

	log.Printf("Server %s updated live servers: %v, isContactServer: %v", s.serverID, s.liveServers, s.isContactServer)
	return &pb.LiveServersResponse{Message: "Live servers updated"}, nil
}

func (s *Server) ServerAddress() string {
	return s.serverAddress
}

// createTables creates the necessary tables in the database
func (s *Server) createTables() {
	// Drop existing tables if necessary
	_, err := s.db.Exec(`DROP TABLE IF EXISTS accounts;`)
	if err != nil {
		log.Fatalf("Failed to drop accounts table: %v", err)
	}
	_, err = s.db.Exec(`DROP TABLE IF EXISTS transactions;`)
	if err != nil {
		log.Fatalf("Failed to drop transactions table: %v", err)
	}

	// Create new tables
	_, err = s.db.Exec(`
        CREATE TABLE accounts (
            account_id INTEGER PRIMARY KEY,
            balance INTEGER
        );
    `)
	if err != nil {
		log.Fatalf("Failed to create accounts table: %v", err)
	}
	_, err = s.db.Exec(`
        CREATE TABLE transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            txn_id TEXT,    -- Changed from INTEGER to TEXT
            phase TEXT,
            sender INTEGER,
            receiver INTEGER,
            amount INTEGER,
            UNIQUE(txn_id, phase)
        );
    `)
	if err != nil {
		log.Fatalf("Failed to create transactions table: %v", err)
	}
}

// initializeAccounts initializes account balances for the shard
func (s *Server) initializeAccounts() {
	// Check if accounts are already initialized
	var count int
	err := s.db.QueryRow("SELECT COUNT(*) FROM accounts;").Scan(&count)
	if err != nil {
		log.Fatalf("Failed to count accounts: %v", err)
	}
	if count > 0 {
		// Accounts already initialized
		return
	}

	tx, err := s.db.Begin()
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}
	stmt, err := tx.Prepare("INSERT INTO accounts (account_id, balance) VALUES (?, ?);")
	if err != nil {
		log.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()
	startID, endID := s.getShardRange()
	for i := startID; i <= endID; i++ {
		_, err = stmt.Exec(i, 10)
		if err != nil {
			log.Fatalf("Failed to insert account %d: %v", i, err)
		}
	}
	err = tx.Commit()
	if err != nil {
		log.Fatalf("Failed to commit transaction: %v", err)
	}
}

func (s *Server) GetBalance(ctx context.Context, req *pb.AccountRequest) (*pb.AccountResponse, error) {
	log.Printf("Server %s received GetBalance request for account %d", s.serverID, req.AccountId)
	var balance int32
	err := s.db.QueryRow("SELECT balance FROM accounts WHERE account_id = ?;", req.AccountId).Scan(&balance)
	if err != nil {
		return &pb.AccountResponse{
			AccountId: req.AccountId,
			Balance:   0,
			Message:   fmt.Sprintf("Error retrieving balance: %v", err),
		}, nil
	}
	return &pb.AccountResponse{
		AccountId: req.AccountId,
		Balance:   balance,
		Message:   "Balance retrieved successfully",
	}, nil
}

// getShardRange returns the account ID range for the shard
func (s *Server) getShardRange() (startID, endID int32) {
	totalAccounts := int32(3000)
	accountsPerShard := totalAccounts / int32(s.numClusters)
	shardNum, err := strconv.Atoi(strings.TrimPrefix(s.shardID, "D"))
	if err != nil {
		log.Fatalf("Invalid shard ID: %s", s.shardID)
	}
	startID = accountsPerShard*(int32(shardNum)-1) + 1
	endID = accountsPerShard * int32(shardNum)
	if int32(shardNum) == int32(s.numClusters) {
		endID = totalAccounts // Ensure the last shard covers any remaining accounts
	}
	return startID, endID
}

// IntraShardTransaction handles intra-shard transactions using the modified Paxos protocol
func (s *Server) IntraShardTransaction(ctx context.Context, req *pb.TransactionRequest) (*pb.TransactionResponse, error) {
	if !s.isContactServer {
		msg := "Server is not the contact server for its shard"
		return &pb.TransactionResponse{
			Success: false,
			Message: msg,
		}, nil
	}

	s.txnMutex.Lock()
	defer s.txnMutex.Unlock() // Hold the mutex for the entire transaction processing

	txnID := s.txnIDGenerator.NextID()

	txn := &pb.Transaction{
		Sender:   req.Sender,
		Receiver: req.Receiver,
		Amount:   req.Amount,
		ID:       txnID,
	}

	log.Printf("[IntraShardTransaction] Server %s received transaction: (%d, %d, %d)", s.serverID, txn.Sender, txn.Receiver, txn.Amount)

	// Validate that the transaction is intra-shard
	if getShard(txn.Sender, s.numClusters) != s.shardID || getShard(txn.Sender, s.numClusters) != s.shardID {
		msg := "Transaction is not intra-shard"
		return &pb.TransactionResponse{
			Success: false,
			Message: msg,
		}, nil
	}

	// Initiate the consensus protocol for this transaction
	success, err := s.InitiateConsensus(txn, "")
	if err != nil {
		log.Printf("[IntraShardTransaction] Consensus failed: %v", err)
		return &pb.TransactionResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	if success {
		defer s.releaseLocks(txn, -1)
		msg := "Transaction committed successfully"
		return &pb.TransactionResponse{
			Success: true,
			Message: msg,
		}, nil
	} else {
		msg := "Transaction aborted"
		return &pb.TransactionResponse{
			Success: false,
			Message: msg,
		}, nil
	}
}

// PrintDatastore prints the committed transactions in the datastore in the specified format
func (s *Server) PrintDatastore() {
	s.mu.Lock()
	defer s.mu.Unlock()

	fmt.Printf("Server Datastore\n%s ", s.serverID)
	for i, entry := range s.committedTxns {
		if i > 0 {
			fmt.Printf("→ ")
		}
		if entry.Phase != "" {
			// If Phase is present, include it in the output
			fmt.Printf("[<%s, %s>, (%d, %d, %d)]", entry.TxnID, entry.Phase, entry.Txn.Sender, entry.Txn.Receiver, entry.Txn.Amount)
		} else {
			// If Phase is empty, omit it
			fmt.Printf("[<%s>, (%d, %d, %d)]", entry.TxnID, entry.Txn.Sender, entry.Txn.Receiver, entry.Txn.Amount)
		}
	}
	fmt.Println()
}

func (s *Server) InitiateConsensus(txn *pb.Transaction, phase string) (bool, error) {
	s.mu.Lock()
	s.currentBallot++
	ballot := s.currentBallot*10 + s.serverIDAsInt
	s.mu.Unlock()

	isIntraShard := phase == ""

	log.Printf("[Consensus] Server %s initiating consensus for transaction (%d, %d, %d) with ballot %d", s.serverID, txn.Sender, txn.Receiver, txn.Amount, ballot)

	// Prepare phase
	promises, err := s.preparePhase(ballot)
	log.Printf("[Consensus] Server %s received %d promises", s.serverID, promises)
	if err != nil {
		return false, fmt.Errorf("Prepare phase failed: %v", err)
	}

	if promises < s.getMajority() {
		// Abort the transaction on all involved shards
		//s.abortPhase(ballot, txn)
		return false, fmt.Errorf("Not enough promises received")
	}

	// Attempt to acquire locks
	waitForLocks := isIntraShard
	if !s.acquireLocks(txn, waitForLocks) {
		// Abort the transaction on all involved shards
		//s.abortPhase(ballot, txn)
		return false, fmt.Errorf("Data items are locked")
	}
	//s.acquireLocks(txn, waitForLocks)

	// Accept phase
	accepted, err := s.acceptPhase(ballot, txn, isIntraShard)
	log.Printf("[Consensus] Server %s received %d acceptances", s.serverID, accepted)
	if err != nil {
		s.releaseLocks(txn, ballot)
		//s.abortPhase(ballot, txn)
		return false, fmt.Errorf("Accept phase failed: %v", err)
	}

	if accepted < s.getMajority() {
		s.releaseLocks(txn, ballot)
		//s.abortPhase(ballot, txn)
		return false, fmt.Errorf("Not enough acceptances received")
	}

	// Commit phase
	err = s.commitPhase(ballot, txn)
	if err != nil {
		s.releaseLocks(txn, ballot)
		//s.abortPhase(ballot, txn)
		return false, fmt.Errorf("Commit phase failed: %v", err)
	}
	//s.releaseLocks(txn)
	return true, nil
}

func (s *Server) preparePhase(ballot int32) (int, error) {
	promises := 1 // Include self
	var wg sync.WaitGroup
	var mu sync.Mutex
	success := true

	serverAddresses := s.getClusterServerAddresses()
	prepareResponses := make([]*pb.PrepareResponse, 0)

	for _, addr := range serverAddresses {
		wg.Add(1)
		go func(address string) {
			defer wg.Done()
			conn, err := grpc.Dial(address, grpc.WithInsecure())
			if err != nil {
				log.Printf("[Prepare Phase] Failed to connect to %s: %v", address, err)
				success = false
				return
			}
			defer conn.Close()

			client := pb.NewBankingServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			req := &pb.PrepareRequest{
				BallotNumber:     ballot,
				DatastoreVersion: int32(len(s.committedTxns)),
			}

			resp, err := client.Prepare(ctx, req)
			if err != nil {
				log.Printf("[Prepare Phase] Prepare RPC to %s failed: %v", address, err)
				success = false
				return
			}

			if resp.Promise {
				log.Printf("[Prepare Phase] Received promise from %s", address)
				mu.Lock()
				promises++
				prepareResponses = append(prepareResponses, resp)
				mu.Unlock()
			} else {
				log.Printf("[Prepare Phase] Promise rejected by %s", address)
			}
		}(addr)
	}

	wg.Wait()
	if !success {
		return promises, fmt.Errorf("Prepare phase failed")
	}

	// Synchronize datastores
	for _, resp := range prepareResponses {
		if len(resp.MissingTransactions) > 0 {
			log.Printf("[Prepare Phase] Synchronizing missing transactions")
			for _, entry := range resp.MissingTransactions {
				txn := entry.Txn
				err := s.applyTransaction(txn)
				if err != nil {
					log.Printf("Error applying missing transaction: %v", err)
					return promises, err
				}
				s.mu.Lock()
				s.committedTxns = append(s.committedTxns, &DatastoreEntry{
					TxnID: txn.ID,
					Phase: entry.Phase,
					Txn:   txn,
				})
				s.mu.Unlock()
			}
		}
	}

	return promises, nil
}

func (s *Server) Prepare(ctx context.Context, req *pb.PrepareRequest) (*pb.PrepareResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ballot := req.BallotNumber
	log.Printf("[Prepare] Server %s received Prepare request with ballot %d", s.serverID, ballot)
	if ballot > s.currentBallot {
		s.currentBallot = ballot
		missingTxns := s.getMissingTransactions(req.DatastoreVersion)
		return &pb.PrepareResponse{
			Promise:             true,
			DatastoreVersion:    int32(len(s.committedTxns)),
			MissingTransactions: missingTxns,
		}, nil
	}

	return &pb.PrepareResponse{
		Promise: false,
	}, nil
}

func (s *Server) getMissingTransactions(version int32) []*pb.DatastoreEntry {
	if version < int32(len(s.committedTxns)) {
		missingEntries := make([]*pb.DatastoreEntry, 0)
		for _, entry := range s.committedTxns[version:] {
			pbEntry := &pb.DatastoreEntry{
				TxnID: entry.TxnID, // entry.TxnID is now a string
				Phase: entry.Phase,
				Txn: &pb.Transaction{
					ID:       entry.Txn.ID, // Use ID instead of Index
					Sender:   entry.Txn.Sender,
					Receiver: entry.Txn.Receiver,
					Amount:   entry.Txn.Amount,
				},
			}
			missingEntries = append(missingEntries, pbEntry)
		}
		return missingEntries
	}
	return nil
}

func (s *Server) acceptPhase(ballot int32, txn *pb.Transaction, isIntraShard bool) (int, error) {
	accepted := 1 // Include self
	var wg sync.WaitGroup
	var mu sync.Mutex
	success := true

	serverAddresses := s.getClusterServerAddresses()
	for _, addr := range serverAddresses {
		wg.Add(1)
		go func(address string) {
			defer wg.Done()
			conn, err := grpc.Dial(address, grpc.WithInsecure())
			if err != nil {
				log.Printf("[Accept Phase] Failed to connect to %s: %v", address, err)
				success = false
				return
			}
			defer conn.Close()

			client := pb.NewBankingServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			req := &pb.AcceptRequest{
				BallotNumber: ballot,
				Txn:          txn,
				WaitForLocks: isIntraShard, // Pass the flag
			}

			resp, err := client.Accept(ctx, req)
			if err != nil {
				log.Printf("[Accept Phase] Accept RPC to %s failed: %v", address, err)
				success = false
				return
			}

			if resp.Accepted {
				log.Printf("[Accept Phase] Acceptance received from %s", address)
				mu.Lock()
				accepted++
				mu.Unlock()
			} else {
				log.Printf("[Accept Phase] Acceptance rejected by %s", address)
			}
		}(addr)
	}

	wg.Wait()
	if !success {
		return accepted, fmt.Errorf("Accept phase failed")
	}
	return accepted, nil
}

func (s *Server) Accept(ctx context.Context, req *pb.AcceptRequest) (*pb.AcceptResponse, error) {
	ballot := req.BallotNumber
	txn := req.Txn
	//waitForLocks := req.WaitForLocks
	log.Printf("[Accept] Server %s received Accept request with ballot %d", s.serverID, ballot)

	accounts := []int32{}
	if getShard(txn.Sender, s.numClusters) == s.shardID {
		accounts = append(accounts, txn.Sender)
	}
	if getShard(txn.Receiver, s.numClusters) == s.shardID {
		accounts = append(accounts, txn.Receiver)
	}

	// Check per-account acceptedBallot without holding s.mu
	s.mu.Lock()
	acceptedBallotsCopy := make(map[int32]int32)
	for _, accountID := range accounts {
		acceptedBallot, exists := s.acceptedBallots[accountID]
		if exists {
			acceptedBallotsCopy[accountID] = acceptedBallot
		} else {
			acceptedBallotsCopy[accountID] = 0
		}
	}
	s.mu.Unlock()

	for _, accountID := range accounts {
		acceptedBallot := acceptedBallotsCopy[accountID]
		if ballot <= acceptedBallot {
			log.Printf("[Accept] Rejecting accept request for account %d due to lower ballot", accountID)
			return &pb.AcceptResponse{
				Accepted: false,
			}, nil
		}
	}

	// **Add the sufficient funds check here**
	if getShard(txn.Sender, s.numClusters) == s.shardID {
		if !s.checkSufficientFunds(txn) {
			log.Printf("[Accept] Transaction aborted due to insufficient funds")
			return &pb.AcceptResponse{
				Accepted: false,
			}, nil
		}
	}

	// Set per-account acceptedBallot and acceptedValue under s.mu
	s.mu.Lock()
	for _, accountID := range accounts {
		s.acceptedBallots[accountID] = ballot
		s.acceptedValues[accountID] = txn
	}
	s.mu.Unlock()

	return &pb.AcceptResponse{
		Accepted: true,
	}, nil
}

func (s *Server) releaseLockForAccount(accountID int32, txnID string) {
	//s.locksMutex.Lock()
	//defer s.locksMutex.Unlock()
	if lock, exists := s.locks[accountID]; exists && lock.holder == txnID {
		lock.Unlock()
		delete(s.locks, accountID)
		log.Printf("[Locks] Server %s released lock on account %d", s.serverID, accountID)
	}
}

func (s *Server) commitPhase(ballot int32, txn *pb.Transaction) error {
	var wg sync.WaitGroup
	success := true

	serverAddresses := s.getClusterServerAddresses()

	for _, addr := range serverAddresses {
		wg.Add(1)
		go func(address string) {
			defer wg.Done()
			conn, err := grpc.Dial(address, grpc.WithInsecure())
			if err != nil {
				log.Printf("[Commit Phase] Failed to connect to %s: %v", address, err)
				success = false
				return
			}
			defer conn.Close()

			client := pb.NewBankingServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			req := &pb.CommitRequest{
				BallotNumber: ballot,
				Txn:          txn,
			}

			resp, err := client.Commit(ctx, req)
			if err != nil || !resp.Success {
				log.Printf("[Commit Phase] Commit RPC to %s failed: %v", address, err)
				success = false
				return
			}

			log.Printf("[Commit Phase] Commit confirmed by %s", address)
		}(addr)
		//}
	}

	wg.Wait()
	if !success {
		return fmt.Errorf("Commit phase failed")
	}

	// Apply transaction locally
	//err := s.applyTransaction(txn)
	err := s.commitFn(txn, ballot)
	if err != nil {
		return err
	}

	// Release locks after applying transaction
	//s.releaseLocks(txn)

	return nil
}

func (s *Server) Commit(ctx context.Context, req *pb.CommitRequest) (*pb.CommitResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	txn := req.Txn
	log.Printf("[Commit] Server %s received Commit request for transaction (%d, %d, %d)", s.serverID, txn.Sender, txn.Receiver, txn.Amount)

	// Apply transaction without holding s.mu
	err := s.commitFn(txn, req.BallotNumber)
	if err != nil {
		return &pb.CommitResponse{
			Success: false,
		}, err
	}

	return &pb.CommitResponse{
		Success: true,
	}, nil
}

func (s *Server) commitFn(txn *pb.Transaction, ballot int32) error {
	//s.mu.Unlock()
	err := s.applyTransaction(txn)
	if err != nil {
		return err
	}
	//s.mu.Lock()

	// Append to committed transactions
	s.appendToDatastore(txn.ID, "", txn)

	// Clear acceptedBallots and acceptedValues for involved accounts
	accounts := []int32{}
	if getShard(txn.Sender, s.numClusters) == s.shardID {
		accounts = append(accounts, txn.Sender)
	}
	if getShard(txn.Receiver, s.numClusters) == s.shardID {
		accounts = append(accounts, txn.Receiver)
	}
	for _, accountID := range accounts {
		delete(s.acceptedBallots, accountID)
		delete(s.acceptedValues, accountID)
	}
	err = s.logPrepareInWAL(txn)
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) Abort(ctx context.Context, req *pb.AbortRequest) (*pb.AbortResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ballot := req.BallotNumber
	txnID := req.TxnId
	log.Printf("[Abort] Server %s received Abort request with ballot %d for txn %s", s.serverID, ballot, txnID)

	// Retrieve the transaction from WAL
	txn, err := s.getTransactionFromWAL(txnID)
	if err != nil {
		// Transaction not found in WAL; treat as successful abort
		log.Printf("[Abort] Transaction not found in WAL; assuming already aborted.")
		return &pb.AbortResponse{
			Success: true,
			Message: "Transaction not found in WAL; treated as successfully aborted.",
		}, nil
	}

	// Start a database transaction for rollback
	dbTx, err := s.db.Begin()
	if err != nil {
		log.Printf("[Abort] Failed to begin DB transaction for rollback: %v", err)
		return &pb.AbortResponse{
			Success: false,
			Message: "Failed to begin DB transaction for rollback",
		}, nil
	}

	// Reverse the transaction
	if getShard(txn.Sender, s.numClusters) == s.shardID {
		_, err = dbTx.Exec("UPDATE accounts SET balance = balance + ? WHERE account_id = ?;", txn.Amount, txn.Sender)
		if err != nil {
			log.Printf("[Abort] Failed to rollback sender account %d: %v", txn.Sender, err)
			dbTx.Rollback()
			return &pb.AbortResponse{
				Success: false,
				Message: "Failed to rollback sender account",
			}, nil
		}
	}

	if getShard(txn.Receiver, s.numClusters) == s.shardID {
		_, err = dbTx.Exec("UPDATE accounts SET balance = balance - ? WHERE account_id = ?;", txn.Amount, txn.Receiver)
		if err != nil {
			log.Printf("[Abort] Failed to rollback receiver account %d: %v", txn.Receiver, err)
			dbTx.Rollback()
			return &pb.AbortResponse{
				Success: false,
				Message: "Failed to rollback receiver account",
			}, nil
		}
	}

	// Commit the rollback transaction
	err = dbTx.Commit()
	if err != nil {
		log.Printf("[Abort] Failed to commit DB rollback: %v", err)
		return &pb.AbortResponse{
			Success: false,
			Message: "Failed to commit DB rollback",
		}, nil
	}

	// Release locks
	s.releaseLocks(txn, ballot)

	return &pb.AbortResponse{
		Success: true,
		Message: "Aborted and rolled back successfully",
	}, nil
}

// Helper function to check if an account is locked
func (s *Server) isAccountLocked(accountID int32) bool {
	//s.locksMutex.Lock()
	lock, exists := s.locks[accountID]
	if !exists {
		lock = NewTryMutex()
		s.locks[accountID] = lock
	}
	//s.locksMutex.Unlock()

	if lock.TryLock() {
		// Lock acquired, immediately unlock
		lock.Unlock()
		return false
	} else {
		// Lock is already held
		return true
	}
}

// tryLock attempts to lock a mutex without blocking
func tryLock(m *sync.Mutex) bool {
	ch := make(chan bool, 1)
	go func() {
		m.Lock()
		ch <- true
	}()
	select {
	case <-ch:
		m.Unlock()
		return true
	case <-time.After(1 * time.Millisecond):
		return false
	}
}

func (s *Server) acquireLocks(txn *pb.Transaction, waitForLocks bool) bool {
	accounts := []int32{}
	if getShard(txn.Sender, s.numClusters) == s.shardID {
		accounts = append(accounts, txn.Sender)
	}
	if getShard(txn.Receiver, s.numClusters) == s.shardID {
		accounts = append(accounts, txn.Receiver)
	}

	// Sort accounts to lock in order
	sort.Slice(accounts, func(i, j int) bool { return accounts[i] < accounts[j] })

	acquiredLocks := make([]int32, 0)

	for _, accountID := range accounts {
		// Get or create the lock for accountID without holding locksMutex
		s.locksMutex.Lock()
		lock, exists := s.locks[accountID]
		if !exists {
			lock = NewTryMutex()
			s.locks[accountID] = lock
		}
		s.locksMutex.Unlock()
		var lockAcquired bool
		if waitForLocks {
			// Wait for the lock without holding any mutex
			lock.Lock()
			lockAcquired = true
		} else {
			// Try to acquire the lock
			lockAcquired = lock.TryLock()
		}
		if lockAcquired {
			// Set the lock holder
			s.locksMutex.Lock()
			lock.holder = txn.ID
			s.locksMutex.Unlock()
			log.Printf("[Locks] Server %s acquired lock on account %d for txn %s", s.serverID, accountID, txn.ID)
			acquiredLocks = append(acquiredLocks, accountID)
		} else {
			// Release all previously acquired locks
			for _, accID := range acquiredLocks {
				s.releaseLockForAccount(accID, txn.ID)
			}
			log.Printf("[Locks] Failed to acquire lock on account %d for txn %s", accountID, txn.ID)
			return false
		}

	}
	return true
}

func (s *Server) releaseLocks(txn *pb.Transaction, ballot int32) {
	s.locksMutex.Lock()
	defer s.locksMutex.Unlock()

	log.Printf("[Locks] Server %s releasing locks for txn %s ", s.serverID, txn.ID)

	accounts := []int32{}
	if getShard(txn.Sender, s.numClusters) == s.shardID {
		accounts = append(accounts, txn.Sender)
	}
	if getShard(txn.Receiver, s.numClusters) == s.shardID {
		accounts = append(accounts, txn.Receiver)
	}

	for _, accountID := range accounts {
		s.releaseLockForAccount(accountID, txn.ID)
		if s.acceptedBallots[accountID] == ballot || ballot == 0 {

			delete(s.acceptedBallots, accountID)
			delete(s.acceptedValues, accountID)
			log.Printf("[Accept] Timeout reached, for acc :%d txn: %s", accountID, txn.ID)
		}
	}
}
func (s *Server) relaseLocks(txn *pb.Transaction, ballot int32, accounts []int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, accountID := range accounts {
		if s.acceptedBallots[accountID] == ballot {
			s.releaseLockForAccount(accountID, txn.ID)
			delete(s.acceptedBallots, accountID)
			delete(s.acceptedValues, accountID)
			log.Printf("[Accept] Timeout reached, for acc :%d txn: %s", accountID, txn.ID)
		}
	}
}

func (s *Server) checkSufficientFunds(txn *pb.Transaction) bool {
	// Only check if this server is responsible for the sender's account
	if getShard(txn.Sender, s.numClusters) != s.shardID {
		// Not responsible for sender's account, assume sufficient
		return true
	}
	var balance int32
	err := s.db.QueryRow("SELECT balance FROM accounts WHERE account_id = ?;", txn.Sender).Scan(&balance)
	if err != nil {
		log.Printf("Error checking balance: %v", err)
		return false
	}
	return balance >= txn.Amount
}

func (s *Server) applyTransaction(txn *pb.Transaction) error {
	log.Printf("[Apply Transaction] Server %s applying transaction: (%d -> %d, Amount: %d)", s.serverID, txn.Sender, txn.Receiver, txn.Amount)
	// Start a database transaction
	tx, err := s.db.Begin()
	if err != nil {
		log.Printf("Failed to begin database transaction: %v", err)
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p) // Re-throw panic after Rollback
		} else if err != nil {
			tx.Rollback() // err is non-nil; don't change it
		} else {
			err = tx.Commit() // err is nil; if Commit returns error, update err
		}
	}()

	// Check if transaction already exists
	var count int
	err = tx.QueryRow("SELECT COUNT(*) FROM transactions WHERE txn_id = ? AND phase = ?;", txn.ID, "").Scan(&count)
	if err != nil {
		log.Printf("Error checking for existing transaction: %v", err)
		return err
	}
	if count > 0 {
		log.Printf("[Apply Transaction] Transaction already exists, skipping")
		err = tx.Commit()
		if err != nil {
			log.Printf("Failed to commit transaction: %v", err)
			return err
		}
		return nil
	}

	// Update balances only for accounts in this shard
	if getShard(txn.Sender, s.numClusters) == s.shardID {
		_, err = tx.Exec("UPDATE accounts SET balance = balance - ? WHERE account_id = ?;", txn.Amount, txn.Sender)
		if err != nil {
			log.Printf("Error updating sender balance: %v", err)
			return err
		}
	}

	if getShard(txn.Receiver, s.numClusters) == s.shardID {
		_, err = tx.Exec("UPDATE accounts SET balance = balance + ? WHERE account_id = ?;", txn.Amount, txn.Receiver)
		if err != nil {
			log.Printf("Error updating receiver balance: %v", err)
			return err
		}
	}

	// Insert transaction into datastore
	phase := ""
	_, err = tx.Exec("INSERT INTO transactions (txn_id, phase, sender, receiver, amount) VALUES (?, ?, ?, ?, ?);",
		txn.ID, phase, txn.Sender, txn.Receiver, txn.Amount)
	if err != nil {
		log.Printf("Error inserting transaction: %v", err)
		return err
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		log.Printf("Failed to commit database transaction: %v", err)
		return err
	}

	// Append to committed transactions
	//s.mu.Lock()
	entry := &DatastoreEntry{
		TxnID: txn.ID,
		Phase: phase,
		Txn:   txn,
	}
	s.committedTxns = append(s.committedTxns, entry)
	//s.mu.Unlock()

	log.Printf("[Apply Transaction] Transaction applied: (%d, %d, %d)", txn.Sender, txn.Receiver, txn.Amount)
	return nil
}

func (s *Server) getClusterServerAddresses() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var addresses []string
	basePort := 50050

	totalServers := s.numClusters * s.serversPerCluster
	for i := 1; i <= totalServers; i++ {
		serverID := fmt.Sprintf("S%d", i)
		if s.isServerInShard(serverID) && s.liveServers[serverID] {
			address := fmt.Sprintf("localhost:%d", basePort+i)
			if address != s.serverAddress {
				addresses = append(addresses, address)
			}
		}
	}
	return addresses
}

func (s *Server) isServerInShard(serverID string) bool {
	sIDNum, err := strconv.Atoi(strings.TrimPrefix(serverID, "S"))
	if err != nil {
		log.Printf("Invalid server ID: %s", serverID)
		return false
	}
	shardNum := ((sIDNum - 1) / s.serversPerCluster) + 1
	shardID := fmt.Sprintf("D%d", shardNum)
	return shardID == s.shardID
}

func (s *Server) getTotalShardServers() int {
	totalServerCount := 0
	for serverID := range serverAddressMap {
		if s.isServerInShard(serverID) {
			totalServerCount++
		}
	}
	return totalServerCount
}

func (s *Server) getMajority() int {
	totalServerCount := s.getTotalShardServers()
	return (totalServerCount / 2) + 1
}

// Corrected loadCommittedTransactions function
func (s *Server) loadCommittedTransactions() {
	rows, err := s.db.Query("SELECT txn_id, phase, sender, receiver, amount FROM transactions ORDER BY id;")
	if err != nil {
		log.Fatalf("Failed to load committed transactions: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var txnID string // Changed from int32 to string
		var phase string
		var sender, receiver, amount int32
		err := rows.Scan(&txnID, &phase, &sender, &receiver, &amount)
		if err != nil {
			log.Fatalf("Failed to scan transaction: %v", err)
		}
		txn := &pb.Transaction{
			ID:       txnID, // Use ID instead of Index
			Sender:   sender,
			Receiver: receiver,
			Amount:   amount,
		}
		entry := &DatastoreEntry{
			TxnID: txnID,
			Phase: phase,
			Txn:   txn,
		}
		s.committedTxns = append(s.committedTxns, entry)
	}
	log.Printf("Loaded %d committed transactions from the database", len(s.committedTxns))
}

// getShard determines the shard for an account ID
//func getShard(accountID int32) string {
//	if accountID >= 1 && accountID <= 1000 {
//		return "D1"
//	} else if accountID >= 1001 && accountID <= 2000 {
//		return "D2"
//	} else if accountID >= 2001 && accountID <= 3000 {
//		return "D3"
//	}
//	return ""
//}
