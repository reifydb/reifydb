# 🧪 Regression Tests for ReifyDB

This crate contains **regression tests** for ReifyDB — tests that were written specifically to
reproduce previously reported or discovered bugs. These tests ensure that once a bug is fixed, it
stays fixed.

## 🔍 Purpose

- Capture specific bugs with minimal, reproducible test cases.
- Prevent regressions by verifying these cases during every test run.
- Complement smoke and functional tests with targeted, bug-focused coverage.

## 📂 Structure

Each test typically includes:

- A minimal schema setup.
- One or more queries that previously caused incorrect behavior or crashes.
- Assertions to verify correct and expected results.

▶️ Running the Tests

```bash
cargo test -p regression
```

🛠️ Adding a Regression Test

1. Reproduce the bug with a failing test::
2. Commit the test (even if failing).
3. Apply the fix.
4. Ensure the test now passes.
5. Add a clear comment about the bug or a link to an issue (if tracked).

📌 Philosophy

Every fix deserves a test:: If a bug was important enough to fix, it's important enough to protect
against forever.