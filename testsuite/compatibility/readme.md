# Compatibility Tests for ReifyDB
This test suite ensures that ReifyDB behaves correctly across various storage backends, concurrency modes, and transaction isolation levels.

## 🧪 What’s Tested

- ✅ Compatibility across combinations of:
  - Transaction Engines:
      - SVL
      - MVCC (optimistic & serializable)
  - Storage Engines:
      - In-memory
      - LMDB

## ▶️ Running Tests

```bash
cargo test -p compatibilityS
```