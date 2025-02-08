package client

import (
	"context"
	pb "github.com/Manthan0999/apaxos-project/pkg/banking"
	"github.com/Manthan0999/apaxos-project/pkg/utils"
	"google.golang.org/grpc"
	"log"
	"strconv"
	"sync"
	"time"
)

// handleCrossShardTransaction handles a cross-shard transaction
func (c *Client) handleCrossShardTransaction(senderServerID, receiverServerID string, Counter int, txn *utils.Transaction) {
	log.Printf("[Client] Initiating cross-shard transaction (%d, %d, %d) between %s and %s using servers: %s and %s",
		txn.Sender, txn.Receiver, txn.Amount, getShard(txn.Sender, c.numClusters), getShard(txn.Sender, c.numClusters), senderServerID, receiverServerID)

	txnID := int32(Counter) // Generate a unique transaction ID

	// Prepare Phase
	log.Printf("[Client] Cross-shard transaction %d: Starting Prepare Phase", txnID)
	senderPrepared := c.sendCrossShardPrepare(senderServerID, txn, txnID)
	receiverPrepared := c.sendCrossShardPrepare(receiverServerID, txn, txnID)

	if senderPrepared && receiverPrepared {
		// Commit Phase
		log.Printf("[Client] Cross-shard transaction %d: Prepare Phase successful, starting Commit Phase", txnID)
		senderCommitted := c.sendCrossShardCommitLeader(senderServerID, txnID)
		receiverCommitted := c.sendCrossShardCommitLeader(receiverServerID, txnID)
		if senderCommitted && receiverCommitted {
			log.Printf("[Client] Cross-shard transaction %d committed successfully", txnID)
		} else {
			log.Printf("[Client] Cross-shard transaction %d failed to commit", txnID)
		}
	} else {
		// Abort Phase
		log.Printf("[Client] Cross-shard transaction %d: Prepare Phase failed, starting Abort Phase %s %s", txnID, senderServerID, receiverServerID)
		c.sendCrossShardAbortLeader(senderServerID, txnID)
		c.sendCrossShardAbortLeader(receiverServerID, txnID)
		log.Printf("[Client] Cross-shard transaction %d aborted", txnID)
	}
}

// sendCrossShardCommitLeader sends commit requests to all servers within the same shard concurrently.
// It returns true only if all commit requests succeed. If any commit fails, it returns false.
func (c *Client) sendCrossShardCommitLeader(serverID string, txnID int32) bool {
	// Determine the shard of the leader server
	shardID, exists := c.serverShardMap[serverID]
	if !exists {
		log.Printf("[Client] Invalid server ID %s for sendCrossShardCommitLeader", serverID)
		return false
	}

	// Collect all servers within the same shard
	var serversInShard []string
	for svr, shard := range c.serverShardMap {
		if shard == shardID && c.liveServers[svr] {
			serversInShard = append(serversInShard, svr)
		}
	}

	// If no servers are found in the shard, abort
	if len(serversInShard) == 0 {
		log.Printf("[Client] No live servers found in shard %s for commit", shardID)
		return false
	}

	// Channel to collect commit results
	resultCh := make(chan bool, len(serversInShard))
	var wg sync.WaitGroup

	// Send commit requests concurrently
	for _, svr := range serversInShard {
		wg.Add(1)
		go func(svr string) {
			defer wg.Done()
			success := c.sendCrossShardCommit(svr, txnID)
			resultCh <- success
		}(svr)
	}

	// Wait for all commit requests to be sent
	wg.Wait()
	close(resultCh)

	// Aggregate results
	allSuccessful := true
	for success := range resultCh {
		if !success {
			allSuccessful = false
		}
	}

	if allSuccessful {
		log.Printf("[Client] All CrossShardCommit requests succeeded for txn %d in shard %s", txnID, shardID)
	} else {
		log.Printf("[Client] One or more CrossShardCommit requests failed for txn %d in shard %s", txnID, shardID)
	}

	return allSuccessful
}

// sendCrossShardCommit sends a commit request to a server for a cross-shard transaction
func (c *Client) sendCrossShardCommit(serverID string, txnID int32) bool {
	serverAddress, exists := c.ServerMap[serverID]
	if !exists {
		log.Printf("[Client] Invalid server ID %s for cross-shard commit", serverID)
		return false
	}

	log.Printf("[Client] Sending CrossShardCommit to server %s for txn %d", serverID, txnID)

	conn, err := grpc.Dial(serverAddress, grpc.WithInsecure())
	if err != nil {
		log.Printf("[Client] Failed to connect to server %s: %v", serverID, err)
		return false
	}
	defer conn.Close()

	client := pb.NewBankingServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &pb.CrossShardCommitRequest{
		TxnId: strconv.Itoa(int(txnID)),
	}

	resp, err := client.CrossShardCommit(ctx, req)
	if err != nil {
		log.Printf("[Client] Cross-shard commit failed on server %s: %v", serverID, err)
		return false
	}

	if resp.Success {
		log.Printf("[Client] Cross-shard commit successful on server %s", serverID)
		return true
	} else {
		log.Printf("[Client] Cross-shard commit failed on server %s: %s", serverID, resp.Message)
		return false
	}
}

// sendCrossShardAbortLeader sends abort requests to all servers within the same shard concurrently.
// It returns true only if all abort requests succeed. If any abort fails, it returns false.
func (c *Client) sendCrossShardAbortLeader(serverID string, txnID int32) bool {
	// Determine the shard of the leader server
	shardID, exists := c.serverShardMap[serverID]
	if !exists {
		log.Printf("[Client] Invalid server ID %s for sendCrossShardAbortLeader", serverID)
		return false
	}

	// Collect all servers within the same shard
	var serversInShard []string
	for svr, shard := range c.serverShardMap {
		if shard == shardID && c.liveServers[svr] {
			serversInShard = append(serversInShard, svr)
		}
	}

	// If no servers are found in the shard, abort
	if len(serversInShard) == 0 {
		log.Printf("[Client] No live servers found in shard %s for abort", shardID)
		return false
	}

	// Channel to collect abort results
	resultCh := make(chan bool, len(serversInShard))
	var wg sync.WaitGroup

	// Send abort requests concurrently
	for _, svr := range serversInShard {
		wg.Add(1)
		go func(svr string) {
			defer wg.Done()
			success := c.sendCrossShardAbort(svr, txnID)
			resultCh <- success
		}(svr)
	}

	// Wait for all abort requests to be sent
	wg.Wait()
	close(resultCh)

	// Aggregate results
	allSuccessful := true
	for success := range resultCh {
		if !success {
			allSuccessful = false
		}
	}

	if allSuccessful {
		log.Printf("[Client] All CrossShardAbort requests succeeded for txn %d in shard %s", txnID, shardID)
	} else {
		log.Printf("[Client] One or more CrossShardAbort requests failed for txn %d in shard %s", txnID, shardID)
	}

	return allSuccessful
}

// sendCrossShardAbort sends an abort request to a server for a cross-shard transaction
func (c *Client) sendCrossShardAbort(serverID string, txnID int32) bool {
	serverAddress, exists := c.ServerMap[serverID]
	if !exists {
		log.Printf("[Client] Invalid server ID %s for cross-shard abort", serverID)
		return false
	}

	log.Printf("[Client] Sending CrossShardAbort to server %s for txn %d", serverID, txnID)

	conn, err := grpc.Dial(serverAddress, grpc.WithInsecure())
	if err != nil {
		log.Printf("[Client] Failed to connect to server %s: %v", serverID, err)
		return false
	}
	defer conn.Close()

	client := pb.NewBankingServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &pb.CrossShardAbortRequest{
		TxnId: strconv.Itoa(int(txnID)),
	}

	resp, err := client.CrossShardAbort(ctx, req)
	if err != nil {
		log.Printf("[Client] Cross-shard abort failed on server %s: %v", serverID, err)
		return false
	}

	if resp.Success {
		log.Printf("[Client] Cross-shard abort successful on server %s for txn %d", serverID, txnID)
		return true
	} else {
		log.Printf("[Client] Cross-shard abort failed on server %s: %s", serverID, resp.Message)
		return false
	}
}
