# Functional Tests for ReifyDB

This module contains functional tests to ensure the entire database system behaves according to the 
specification.

## 🧪 What’s Tested

Each test simulates real-world usage by executing a full flow:

- Creating schemas and tables
- Inserting data
- Executing queries
- Validating expected results

## ▶️ Running Tests

```bash
cargo test -p functional
```