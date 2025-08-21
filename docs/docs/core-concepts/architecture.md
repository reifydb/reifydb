---
sidebar_position: 1
---

# Architecture

ReifyDB is built with a modern, modular architecture designed for performance and scalability.

## System Overview

ReifyDB consists of several key components:

- **Query Engine**: Parses and optimizes SQL queries
- **Storage Engine**: Manages data persistence with MVCC
- **Transaction Manager**: Handles ACID transactions
- **Network Layer**: Provides gRPC and WebSocket interfaces
- **Flow Engine**: Manages real-time data flows and CDC

## Storage Architecture

### Multi-Version Concurrency Control (MVCC)

ReifyDB uses MVCC to provide high concurrency without locking:

- Each transaction sees a consistent snapshot of the database
- Writers don't block readers
- Readers don't block writers

### Storage Backends

ReifyDB supports multiple storage backends:

- **Memory**: In-memory storage for development
- **LMDB**: Lightning Memory-Mapped Database for production
- **SQLite**: Embedded SQL database for single-node deployments

## Query Processing

1. **Parse**: SQL is parsed into an Abstract Syntax Tree (AST)
2. **Analyze**: Type checking and validation
3. **Optimize**: Query optimization and plan generation
4. **Execute**: Physical plan execution

## Transaction Model

ReifyDB supports:

- **ACID Guarantees**: Full ACID compliance
- **Isolation Levels**: Read Committed, Repeatable Read, Serializable
- **Optimistic Concurrency**: Conflict detection at commit time

## Network Protocols

- **gRPC**: High-performance RPC for applications
- **WebSocket**: Real-time bidirectional communication
- **HTTP/REST**: RESTful API for web applications
