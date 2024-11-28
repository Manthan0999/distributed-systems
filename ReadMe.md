# Distributed Banking System with Modified Paxos and 2PC

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Technologies Used](#technologies-used)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Running the Application](#running-the-application)
  - [Starting the Servers](#starting-the-servers)
  - [Running the Client](#running-the-client)
- [Functionality](#functionality)
  - [Server Functions](#server-functions)
  - [Client Functions](#client-functions)
- [Bonus Features Implemented](#bonus-features-implemented)
  - [1. Configurable Clusters and Sharding](#1-configurable-clusters-and-sharding)
  - [2. Performance Metrics for Intra-Shard and Cross-Shard Transactions](#2-performance-metrics-for-intra-shard-and-cross-shard-transactions)
- [Database Details](#database-details)
- [Performance Metrics](#performance-metrics)
- [Testing](#testing)
  - [Testing with Default Configuration](#testing-with-default-configuration)
  - [Testing with Custom Configuration](#testing-with-custom-configuration)
- [Project Structure](#project-structure)
- [Contributing](#contributing)

## Overview

The **Distributed Banking System** is a scalable, fault-tolerant application designed to handle financial transactions across multiple clusters (shards) and servers. It supports configurable clusters and uses sharding to distribute accounts and transactions efficiently. The system ensures data consistency and high availability, making it ideal for environments requiring reliable transaction processing.
ChatGPT's assistance was used for debugging and structure formation of the codebase
## Features

- **Configurable Clusters and Sharding:**
  - Users can specify the number of clusters and the number of servers per cluster via command-line arguments.
  - Dynamic mapping of accounts to shards based on cluster configuration.

- **Intra-Shard and Cross-Shard Transactions:**
  - Supports transactions within the same shard (intra-shard) and across different shards (cross-shard).
  - Efficient handling of cross-shard transactions using two-phase commit protocol.

- **Performance Metrics:**
  - Measures and reports throughput and latency for both intra-shard and cross-shard transactions separately.
  - Provides detailed performance statistics to help optimize the system.

- **Fault Tolerance:**
  - Designed to handle server failures gracefully, maintaining system integrity and availability.
  - Updates live server lists dynamically to reflect the current state of the system.

- **Interactive Client Interface:**
  - Allows users to view account balances, display datastore contents, and show performance metrics.
  - Processes transaction sets from a CSV file with options to pause and inspect the system state.

## Architecture

The system comprises multiple server instances and a client application:

- **Servers:**
  - Each server is assigned to a specific shard (cluster) and maintains its own database.
  - Servers handle transaction requests, participate in consensus protocols, and manage data consistency.
  - Servers communicate with each other to handle cross-shard transactions and update live server lists.

- **Client:**
  - Submits transaction requests by reading from a CSV file.
  - Interacts with servers to perform transactions and retrieve account balances.
  - Provides an interactive interface for users to inspect system performance and data.

## Technologies Used

- **Go (Golang):** Primary programming language for both server and client implementations.
- **gRPC:** Facilitates efficient, low-latency communication between clients and servers.
- **Protocol Buffers:** Defines structured data for RPC communication.
- **SQLite:** Embedded relational database used for storing account balances and transaction logs.
- **Sync and Mutex:** Utilized for concurrent processing and ensuring thread safety.
- **Command-Line Arguments:** Used for configuring clusters and servers dynamically.

## Getting Started

### Prerequisites

- **Go:** Ensure you have Go installed (version 1.16 or higher recommended). [Download Go](https://golang.org/dl/)
- **Git:** Required for cloning the repository and version control. [Download Git](https://git-scm.com/downloads)

### Installation

1. **Clone the Repository:**

   ```bash
   https://github.com/F24-CSE535/2pc-Manthan0999.git
   cd 2pc-Manthan0999
   ```

2. **Install Dependencies:**

   Ensure all Go dependencies are installed. From the root directory, run:

   ```bash
   go mod tidy
   ```

## Running the Application

### Starting the Servers

You can start multiple server instances with configurable clusters and servers per cluster using command-line arguments.

**Command-Line Arguments:**

- `-serverID`: Unique identifier for the server (e.g., `S1`, `S2`).
- `-clusters`: Number of clusters (shards) in the system (default is `3`).
- `-serversPerCluster`: Number of servers per cluster (default is `3`).
- `-port`: Port number for the server (default is calculated based on `serverID`).

**Example Commands:**

Suppose you want to start servers for a system with **3 clusters** and **3 servers per cluster** (default configuration):

```bash
# Start servers S1 to S9

    Example: go run cmd/server/main.go S2 D1 50052 S1,S3

```

For a custom configuration with **4 clusters** and **5 servers per cluster**:

```bash
# Start servers S1 to S20
for i in {1..20}; do
    go run cmd/server/main.go -serverID=S$i -clusters=4 -serversPerCluster=5 &
done
```

**Note:** The `serverID` should match the format `S<number>`, and the ports will be assigned automatically based on the `serverID`.

### Running the Client

The client processes transactions from a CSV file and interacts with the servers.

**Command-Line Arguments:**

- `-input`: Path to the input CSV file containing transaction sets.
- `-clusters`: Number of clusters (shards) (default is `3`).
- `-serversPerCluster`: Number of servers per cluster (default is `3`).

**Example Command:**

```bash
go run cmd/client/main.go -input=transactions.csv
```

For a custom configuration:

```bash
go run cmd/client/main.go -input=transactions.csv -clusters=4 -serversPerCluster=5
```

**Note:** Ensure that the `clusters` and `serversPerCluster` values match those used when starting the servers.

## Functionality

### Server Functions

- **Transaction Handling:**
  - Processes intra-shard transactions directly.
  - Coordinates with other servers for cross-shard transactions using two-phase commit.

- **Account Management:**
  - Maintains account balances in a SQLite database.
  - Handles balance inquiries and updates.

- **Live Server Management:**
  - Updates the list of live servers based on client input.
  - Shares live server information with other servers.

- **Performance Monitoring:**
  - Measures and logs transaction processing times.
  - Provides data for performance metrics.

### Client Functions

- **Transaction Processing:**
  - Reads transaction sets from a CSV file.
  - Processes transactions concurrently for efficiency.
  - Differentiates between intra-shard and cross-shard transactions.

- **Interactive Interface:**
  - Allows users to view account balances across servers.
  - Displays datastore contents and transaction logs.
  - Shows performance metrics for intra-shard and cross-shard transactions.

- **Performance Measurement:**
  - Records latency and throughput for transactions.
  - Separately measures performance for intra-shard and cross-shard transactions.

## Bonus Features Implemented

### 1. Configurable Clusters and Sharding

**Description:**

Implemented the ability to configure the number of clusters (shards) and the number of servers per cluster via command-line arguments. This allows the system to scale and adapt to different sizes and configurations without changing the codebase.

**Implementation Highlights:**

- **Dynamic Server Initialization:**
  - Servers generate their shard IDs and server addresses based on `serverID`, `clusters`, and `serversPerCluster`.

- **Dynamic Account-to-Shard Mapping:**
  - Accounts are assigned to shards using a modulus-based function:
    ```go
    func getShard(accountID int32, numClusters int) string {
        shardNum := ((accountID - 1) % int32(numClusters)) + 1
        return fmt.Sprintf("D%d", shardNum)
    }
    ```

- **Consistent Configuration Across Client and Servers:**
  - Both client and servers use the same cluster configuration parameters to ensure consistency.

### 2. Performance Metrics for Intra-Shard and Cross-Shard Transactions

**Description:**

Enhanced the client to measure and display performance metrics separately for intra-shard and cross-shard transactions. Metrics include throughput and latency, measured from the time a transaction is initiated to when a response is received.

**Implementation Highlights:**

- **Latency Measurement:**
  - Recorded at the transaction level within the `handleIntraShardTransaction` and `handleCrossShardTransaction` functions.

- **Throughput Calculation:**
  - Calculated based on the total number of transactions processed and the total processing time.

- **Interactive Display:**
  - Users can view performance metrics via the interactive client interface by selecting the appropriate option.

## Database Details

**SQLite:**

- **Type:** Embedded Relational Database
- **Usage:** Stores account balances and transaction logs.
- **Advantages:**
  - **Simplicity:** Easy to set up and use without additional dependencies.
  - **Concurrency:** Supports concurrent read operations.
  - **Persistence:** Ensures data is saved between runs.

**Schema Design:**

- **Table:** `accounts`
  - **Columns:**
    - `account_id` (INTEGER PRIMARY KEY)
    - `balance` (INTEGER)

- **Table:** `transactions`
  - **Columns:**
    - `txn_id` (TEXT PRIMARY KEY)
    - `status` (TEXT)
    - `sender` (INTEGER)
    - `receiver` (INTEGER)
    - `amount` (INTEGER)

## Performance Metrics

**Metrics Measured:**

- **Throughput:**
  - Number of transactions processed per second.
  - Measured separately for intra-shard and cross-shard transactions.

- **Latency:**
  - Average time taken to process a transaction.
  - Measured from initiation to response receipt.
  - Calculated separately for intra-shard and cross-shard transactions.

**Example Performance Report:**

```
--- Performance Metrics ---
Total Transactions: 33
Total Latency: 8.684781s
Throughput: 3.80 transactions/second
Average Latency: 0.263175 seconds/transaction

```

**Note:** Actual performance may vary based on system configuration and load.

## Testing

Comprehensive testing ensures the reliability and robustness of the system.

### Testing with Default Configuration

1. **Start Servers:**

   ```bash
   $ go run cmd/server/main.go S5 D2 50055 S4,S6

   ```

2. **Run Client:**

   ```bash
   go run cmd/client/main.go -input=transactions.csv
   ```

3. **Process Transactions:**

   Use the sample CSV provided in the `transactions.csv` file.

4. **Verify Results:**

  - Check account balances.
  - View performance metrics.
  - Inspect datastore contents.

### Testing with Custom Configuration

1. **Start Servers with Custom Configuration:**

   ```bash
   for i in {1..20}; do
       go run cmd/server/main.go -serverID=S$i -clusters=4 -serversPerCluster=5 &
   done
   ```

2. **Modify CSV File:**

   Update `LiveServers` and `ContactServers` in your CSV to include server IDs up to `S20`.

3. **Run Client with Custom Configuration:**

   ```bash
   go run cmd/client/main.go -input=transactions.csv -clusters=4 -serversPerCluster=5
   ```

4. **Process Transactions:**

   Use the updated CSV file.

5. **Verify Results:**

  - Ensure transactions are processed correctly.
  - Verify account-to-shard mappings.
  - Check performance metrics.

**Sample CSV Entry:**

```csv
SetNumber,Transaction,LiveServers,ContactServers
1,"(100, 501, 8)","[S1, S2, ..., S20]","[S1, S6, S11, S16]"
,"(1001, 1650, 2)",,
,"(2800, 2150, 7)",,
```

**Note:** Replace `...` with the rest of the server IDs as appropriate.

## Project Structure

```plaintext
apaxos-Manthan0999/
├── cmd/
│   ├── client/
│   │   ├── main.go               # Client application entry point
│   │   └── interactive.go        # Interactive client logic
│   └── server/
│       └── main.go               # Server application entry point
├── internal/
│   ├── client/
│   │   ├── client.go             # Client logic
│   │   └── crossShardUtils.go    # Cross-shard transaction utilities
│   └── server/
│       ├── server.go             # Server logic
│       ├── crossShard.go             # Server logic
│       └── dbOps.go  
        └── wal.go              # Database operations
├── pkg/
│   ├── banking/
│   │   ├── banking.proto         # Protobuf definitions
│   │   ├── banking.pb.go         # Generated protobuf Go code
│   │   └── banking_grpc.pb.go    # gRPC generated Go code
│   └── utils/
│       └── csv_parser.go         # CSV parsing utility
├── transactions.csv              # Sample CSV file with transaction sets
├── go.mod                        # Go module file
└── go.sum                        # Go module dependencies checksum
```

## Contact

For any queries or support, please contact:

- **Name:** Manthan Singh
- **Email:** manthan.singh@stonybrook.edu

---

**Note:** Ensure that you update the `transactions.csv` file and other relevant files to match your project's actual structure and data.

---

# Additional Notes

- **Documentation:** Keep your code well-documented, especially the functions that handle the new features.
- **Error Handling:** Make sure to handle errors gracefully, providing meaningful messages to the user.
- **Logging:** Utilize logging to help with debugging and monitoring the system's behavior.
- **Testing:** Perform extensive testing with different configurations to ensure reliability.
