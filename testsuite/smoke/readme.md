# Smoke Tests for ReifyDB

This module contains fast, high-level tests that verify ReifyDB's core functionality is not broken.
These tests are meant to catch critical failures early in the development cycle.

## ✅ What’s Covered

Smoke tests focus on the most essential operations:

- Bootstrapping the engine
- Creating schemas and tables
- Inserting minimal data
- Executing basic queries
- Ensuring no panics or major errors

> These tests **do not** cover exhaustive edge cases — they simply ensure the system “doesn’t catch
> fire.”

## ▶️ Running Smoke Tests

```bash
cargo test -p smoke
```