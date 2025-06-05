# Diagnostic Tests for ReifyDB

This module verifies that **diagnostic messages** produced by ReifyDB are clear, correct, and
helpful to the user. It ensures that developers see meaningful, well-formatted errors during query
development —
similar to how compilers like Rust provide rich error feedback.

## 🎯 Purpose

The `diagnostic` crate ensures:

- All error codes render consistently
- Spans and caret markers point to the correct location in the query
- Labels, notes, and help messages are meaningful
- Output formatting is human-readable and informative

These tests assert the visual and structural quality of ReifyDB’s user-facing diagnostics.

```bash
cargo test -p diagnostic
```