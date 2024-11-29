package server

import (
	"fmt"
	pb "github.com/Manthan0999/apaxos-project/pkg/banking"
	"log"
)

// WAL functions

func (s *Server) logPrepareInWAL(txn *pb.Transaction) error {
	s.walMutex.Lock()
	defer s.walMutex.Unlock()
	txn.Status = PREP
	s.wal[txn.ID] = txn
	log.Printf("[WAL] Server %s logged prepare in WAL for txn %v", s.serverID, txn.ID)
	return nil
}

// getShard determines the shard for an account ID
func getShard(accountID int32) string {
	if accountID >= 1 && accountID <= 1000 {
		return "D1"
	} else if accountID >= 1001 && accountID <= 2000 {
		return "D2"
	} else if accountID >= 2001 && accountID <= 3000 {
		return "D3"
	}
	return ""
}

// Cross-Shard Transaction Handlers

func (s *Server) getTransactionFromWAL(txnID string) (*pb.Transaction, error) {
	s.walMutex.Lock()
	defer s.walMutex.Unlock()

	txn, exists := s.wal[txnID]
	if !exists {
		log.Printf("[WAL] Server %s could not find txn %s in WAL", s.serverID, txnID)
		return nil, fmt.Errorf("transaction not found in WAL")
	}
	log.Printf("[WAL] Server %s retrieved txn %s from WAL :%v", s.serverID, txnID, txn)
	return txn, nil
}

func (s *Server) updateTransactionFromWAL(txnID string, status string) {
	s.walMutex.Lock()
	defer s.walMutex.Unlock()

	txn, exists := s.wal[txnID]
	if !exists {
		log.Printf("[WAL] Server %s could not find txn %s in WAL", s.serverID, txnID)
		//return nil, fmt.Errorf("transaction not found in WAL")

	} else {
		txn.Status = status
		s.wal[txnID] = txn
	}

	log.Printf("[WAL] Server %s removed txn %s from WAL", s.serverID, txnID)
}

// PrintWAL prints all transactions currently in the Write-Ahead Log
func (s *Server) PrintWAL() {
	s.walMutex.Lock()
	defer s.walMutex.Unlock()

	if len(s.wal) == 0 {
		log.Printf("Server %s WAL is empty.", s.serverID)
		return
	}

	log.Printf("----- WAL Contents for Server %s -----", s.serverID)
	for txnID, txn := range s.wal {
		log.Printf("TxnID: %s, Status: %s, Sender: %d, Receiver: %d, Amount: %d",
			txnID, txn.Status, txn.Sender, txn.Receiver, txn.Amount)
	}
	log.Printf("----- End of WAL -----")
}
