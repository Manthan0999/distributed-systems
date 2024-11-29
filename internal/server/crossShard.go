package server

import (
	"context"
	pb "github.com/Manthan0999/apaxos-project/pkg/banking"
	"log"
)

func (s *Server) CrossShardPrepare(ctx context.Context, req *pb.CrossShardPrepareRequest) (*pb.CrossShardPrepareResponse, error) {
	if !s.isContactServer {
		msg := "Server is not the contact server for its shard"
		log.Printf("[CrossShardPrepare] %s", msg)
		return &pb.CrossShardPrepareResponse{
			Success: false,
			Message: msg,
		}, nil
	}

	s.txnMutex.Lock()
	defer s.txnMutex.Unlock()

	txn := &pb.Transaction{
		Sender:   req.Sender,
		Receiver: req.Receiver,
		Amount:   req.Amount,
		ID:       req.TxnId,
	}

	log.Printf("[CrossShardPrepare] Server %s received prepare request for txn %v %s %d", s.serverID, txn, txn.ID, txn.ID)

	// Initiate the consensus protocol for this transaction
	success, err := s.InitiateConsensus(txn, "P")
	if err != nil {
		log.Printf("[CrossShardPrepare] Consensus failed: %v", err)
		return &pb.CrossShardPrepareResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	if success {
		// Log the transaction in the WAL

		if err != nil {
			log.Printf("[CrossShardPrepare] Failed to log transaction in WAL: %v", err)
			return &pb.CrossShardPrepareResponse{
				Success: false,
				Message: "Failed to log transaction in WAL",
			}, nil
		}

		msg := "Prepared successfully"
		log.Printf("[CrossShardPrepare] %s", msg)
		return &pb.CrossShardPrepareResponse{
			Success: true,
			Message: msg,
		}, nil
	} else {
		msg := "Prepare aborted"
		log.Printf("[CrossShardPrepare] %s", msg)
		return &pb.CrossShardPrepareResponse{
			Success: false,
			Message: msg,
		}, nil
	}
}

func (s *Server) transactionCommittedOrLogged(txnID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, inWAL := s.wal[txnID]
	if inWAL {
		return true
	}

	for _, entry := range s.committedTxns {
		if entry.TxnID == txnID {
			return true
		}
	}
	return false
}

func (s *Server) CrossShardCommit(ctx context.Context, req *pb.CrossShardCommitRequest) (*pb.CrossShardCommitResponse, error) {

	s.txnMutex.Lock()
	defer s.txnMutex.Unlock()

	txnID := req.TxnId
	// Retrieve the transaction from WAL
	txn, err := s.getTransactionFromWAL(string(txnID))
	if err != nil {
		msg := "Transaction not found in WAL"
		return &pb.CrossShardCommitResponse{
			Success: false,
			Message: msg,
		}, nil
	}

	s.updateTransactionFromWAL(txn.ID, COMMIT)
	msg := "Committed successfully"
	s.PrintWAL()
	s.PrintDatastore()
	return &pb.CrossShardCommitResponse{
		Success: true,
		Message: msg,
	}, nil

}

const (
	PREP   = "Prepared"
	COMMIT = "Committed"
	ABORT  = "Aborted"
)

func (s *Server) CrossShardAbort(ctx context.Context, req *pb.CrossShardAbortRequest) (*pb.CrossShardAbortResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	txnID := req.TxnId
	log.Printf("[CrossShardAbort] Server %s received abort request for txn %s", s.serverID, txnID)

	// Retrieve the transaction from WAL
	txn, err := s.getTransactionFromWAL(txnID)
	if err != nil {
		// Transaction not found in WAL; treat as successful abort
		log.Printf("[CrossShardAbort] Transaction not found in WAL; assuming already aborted.")
		return &pb.CrossShardAbortResponse{
			Success: true,
			Message: "Transaction not found in WAL; treated as successfully aborted.",
		}, nil
	}
	if txn.Status == ABORT {
		return &pb.CrossShardAbortResponse{
			Success: true,
			Message: "Aborted successfully",
		}, nil
	}

	// Use WAL to undo any partial changes (if necessary)
	log.Printf("[CrossShardAbort] Rolling back transaction using WAL for txn %s", txnID)
	dbTx, err := s.db.Begin()
	if err != nil {
		log.Printf("[CrossShardAbort] Failed to begin DB transaction for rollback: %v", err)
		return &pb.CrossShardAbortResponse{
			Success: false,
			Message: "Failed to begin DB transaction for rollback",
		}, nil
	}

	// Reverse the transaction
	if getShard(txn.Sender) == s.shardID {
		_, err = dbTx.Exec("UPDATE accounts SET balance = balance + ? WHERE account_id = ?;", txn.Amount, txn.Sender)
		if err != nil {
			log.Printf("[CrossShardAbort] Failed to rollback sender account %d: %v", txn.Sender, err)
			dbTx.Rollback()
			return &pb.CrossShardAbortResponse{
				Success: false,
				Message: "Failed to rollback sender account",
			}, nil
		}
	}

	if getShard(txn.Receiver) == s.shardID {
		_, err = dbTx.Exec("UPDATE accounts SET balance = balance - ? WHERE account_id = ?;", txn.Amount, txn.Receiver)
		if err != nil {
			log.Printf("[CrossShardAbort] Failed to rollback receiver account %d: %v", txn.Receiver, err)
			dbTx.Rollback()
			return &pb.CrossShardAbortResponse{
				Success: false,
				Message: "Failed to rollback receiver account",
			}, nil
		}
	}

	// Commit the rollback transaction
	err = dbTx.Commit()
	if err != nil {
		log.Printf("[CrossShardAbort] Failed to commit DB rollback: %v", err)
		return &pb.CrossShardAbortResponse{
			Success: false,
			Message: "Failed to commit DB rollback",
		}, nil
	}
	s.updateTransactionFromWAL(txn.ID, ABORT)

	// Release locks
	s.releaseLocks(txn, 0)

	return &pb.CrossShardAbortResponse{
		Success: true,
		Message: "Aborted successfully",
	}, nil
}
