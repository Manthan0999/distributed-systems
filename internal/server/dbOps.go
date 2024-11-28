// internal/server/dbOps.go

package server

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	pb "github.com/Manthan0999/apaxos-project/pkg/banking"
)

var (
	COMMITTED_TXNS = "committed_transactions"
	TXNS           = "transactions"
)

// InsertTxn inserts a transaction into the specified table
func (s *Server) InsertTxn(txn *pb.Transaction, tableName string, tx *sql.Tx) error {
	_, err := tx.Exec(fmt.Sprintf("INSERT INTO %s (sender, receiver, amount) VALUES (?, ?, ?)", tableName), txn.Sender, txn.Receiver, txn.Amount)
	if err != nil {
		return fmt.Errorf("Failed to execute insert statement: %v", err)
	}
	return nil
}

// SelectTxn selects transactions from the specified table
func (s *Server) SelectTxn(index *int, tableName string) ([]*pb.Transaction, error) {
	var transactions []*pb.Transaction
	var query string
	var args []interface{}

	if index != nil {
		query = "SELECT id, sender, receiver, amount FROM " + tableName + " WHERE id = ?"
		args = append(args, *index)
	} else {
		query = "SELECT id, sender, receiver, amount FROM " + tableName
	}

	rows, err := s.db.Query(query, args...)
	if err != nil {
		log.Printf("[SelectTxn] Failed to select transactions: %v", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var txn pb.Transaction
		if err := rows.Scan(&txn.Index, &txn.Sender, &txn.Receiver, &txn.Amount); err != nil {
			log.Printf("[SelectTxn] Failed to scan transaction: %v", err)
			return nil, err
		}
		transactions = append(transactions, &txn)
	}

	if err := rows.Err(); err != nil {
		log.Printf("[SelectTxn] Error during rows iteration: %v", err)
		return nil, err
	}

	return transactions, nil
}

// DeleteTxn deletes a transaction from the specified table
func (s *Server) DeleteTxn(index int32, tableName string) error {
	_, err := s.db.Exec("DELETE FROM "+tableName+" WHERE id = ?", index)
	if err != nil {
		log.Printf("[DeleteTxn] Failed to delete transaction with id %d: %v", index, err)
		return err
	}
	return nil
}

// CheckTxnExists checks if a transaction exists in the specified table
func (s *Server) CheckTxnExists(index int32, tableName string) (bool, error) {
	var exists bool
	query := "SELECT EXISTS(SELECT 1 FROM " + tableName + " WHERE id = ?)"
	err := s.db.QueryRow(query, index).Scan(&exists)
	if err != nil {
		log.Printf("[CheckTxnExists] Failed to check if transaction with id %d exists: %v", index, err)
		return false, err
	}
	return exists, nil
}

// IsTableEmpty checks if a table is empty
func IsTableEmpty(db *sql.DB, tableName string) (bool, error) {
	var count int
	query := "SELECT COUNT(*) FROM " + tableName
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		log.Printf("[IsTableEmpty] Failed to check if table %s is empty: %v", tableName, err)
		return false, err
	}
	return count == 0, nil
}

// PerformanceMetric represents a performance metric
type PerformanceMetric struct {
	ID       int
	Duration time.Duration
}

// CreatePerformanceMetricsTable creates the performance_metrics table
func CreatePerformanceMetricsTable(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS performance_metrics (
			id INTEGER PRIMARY KEY,
			duration INTEGER
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to create performance_metrics table: %v", err)
	}
	return nil
}

// InsertPerformanceMetric inserts a new performance metric
func InsertPerformanceMetric(db *sql.DB, id int32, duration time.Duration) error {
	_, err := db.Exec(`INSERT INTO performance_metrics (id, duration) VALUES (?, ?);`, id, duration.Nanoseconds())
	if err != nil {
		return fmt.Errorf("failed to insert performance metric: %v", err)
	}
	return nil
}

// SelectAllPerformanceMetrics retrieves all performance metrics
func SelectAllPerformanceMetrics(db *sql.DB) ([]PerformanceMetric, error) {
	rows, err := db.Query(`SELECT id, duration FROM performance_metrics;`)
	if err != nil {
		return nil, fmt.Errorf("failed to select performance metrics: %v", err)
	}
	defer rows.Close()

	var metrics []PerformanceMetric
	for rows.Next() {
		var metric PerformanceMetric
		var durationInt int64

		if err := rows.Scan(&metric.ID, &durationInt); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}
		metric.Duration = time.Duration(durationInt)
		metrics = append(metrics, metric)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over rows: %v", err)
	}

	return metrics, nil
}
