---
sidebar_position: 1
---

# Rust API

Complete reference for the ReifyDB Rust client.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
reifydb-client = "0.1.0"
tokio = { version = "1", features = ["full"] }
```

## Client

### Creating a Client

```rust
use reifydb_client::Client;

let client = Client::connect("reifydb://localhost:5432").await?;
```

### Connection Options

```rust
use reifydb_client::{Client, Config};

let config = Config::builder()
    .host("localhost")
    .port(5432)
    .user("admin")
    .password("secret")
    .database("mydb")
    .pool_size(10)
    .build();

let client = Client::with_config(config).await?;
```

## Queries

### Execute Statements

```rust
// DDL statements
client.execute("CREATE TABLE users (id INT, name TEXT)").await?;

// DML statements
let affected = client.execute("DELETE FROM users WHERE id = 1").await?;
println!("Deleted {} rows", affected);
```

### Query Data

```rust
// Simple query
let rows = client.query("SELECT * FROM users").await?;

// Query with parameters
let rows = client.query_params(
    "SELECT * FROM users WHERE age > $1",
    &[&25]
).await?;

// Query single row
let row = client.query_one("SELECT * FROM users WHERE id = 1").await?;
```

## Transactions

```rust
let mut tx = client.begin().await?;

tx.execute("INSERT INTO users (name) VALUES ('Alice')").await?;
tx.execute("INSERT INTO users (name) VALUES ('Bob')").await?;

// Commit or rollback
tx.commit().await?;
// or
tx.rollback().await?;
```

## Prepared Statements

```rust
let stmt = client.prepare("SELECT * FROM users WHERE id = $1").await?;

let row = client.query_one(&stmt, &[&1]).await?;
```

## Error Handling

```rust
use reifydb_client::{Error, ErrorKind};

match client.query("SELECT * FROM users").await {
    Ok(rows) => process_rows(rows),
    Err(Error::Connection(e)) => eprintln!("Connection error: {}", e),
    Err(Error::Query(e)) => eprintln!("Query error: {}", e),
    Err(e) => eprintln!("Unknown error: {}", e),
}
```
