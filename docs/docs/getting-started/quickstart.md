---
sidebar_position: 2
---

# Quick Start

Get up and running with ReifyDB in 5 minutes.

## Start the Database

```bash
reifydb start
```

## Connect to ReifyDB

### Using the CLI

```bash
reifydb connect
```

### Using Rust Client

```rust
use reifydb::Client;

#[tokio::main]
async fn main() {
    let client = Client::connect("reifydb://localhost:5432").await?;

    // Create a table
    client.execute("
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE
        )
    ").await?;

    // Insert data
    client.execute("
        INSERT INTO users (name, email)
        VALUES ('Alice', 'alice@example.com')
    ").await?;

    // Query data
    let rows = client.query("SELECT * FROM users").await?;
    for row in rows {
        println!("{:?}", row);
    }
}
```

### Using TypeScript Client

```typescript
import { ReifyDB } from '@reifydb/client';

const db = new ReifyDB('reifydb://localhost:5432');

// Create a table
await db.execute(`
  CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE
  )
`);

// Insert data
await db.execute(`
  INSERT INTO users (name, email) 
  VALUES ('Alice', 'alice@example.com')
`);

// Query data
const users = await db.query('SELECT * FROM users');
console.log(users);
```

## Next Steps

- [Learn about Core Concepts](/docs/core-concepts/architecture)
- [Explore the API Reference](/docs/api)
- [Try the Interactive Playground](/playground)
