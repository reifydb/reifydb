# Running Generated Tests

## Overview

The ReifyDB test suite uses an automatic test generation system that creates individual test files from test scripts during the build process. This makes each test individually debuggable in IDEs while maintaining a clean directory structure.

## Building the Tests

The tests are automatically generated when you build the project:

```bash
# Build all tests (triggers the build.rs scripts)
cargo build --tests

# Or simply run tests (which will build if needed)
cargo test
```

The build process will:
1. Execute `build.rs` scripts in each crate
2. Generate test files in `tests/generated_*` directories
3. Create a `generated_tests.rs` file that imports all test modules

## Running Tests

### Run All Tests in a Crate

```bash
# Run all tests for a specific crate
cargo test -p reifydb-transaction
cargo test -p reifydb-rql
cargo test -p reifydb-storage
cargo test -p reifydb-sub-flow
```

### Run Specific Generated Test Suites

For crates with multiple test generators (like `reifydb-transaction`):

```bash
# Run all generated tests
cargo test -p reifydb-transaction --test generated_tests

# Run only optimistic concurrency tests
cargo test -p reifydb-transaction --test generated_tests generated_optimistic::

# Run only serializable isolation tests
cargo test -p reifydb-transaction --test generated_tests generated_serializable::
```

### Run Tests by Module

```bash
# Run all MVCC tests
cargo test -p reifydb-transaction --test generated_tests generated_optimistic::mvcc::

# Run specific MVCC anomaly tests
cargo test -p reifydb-transaction --test generated_tests generated_serializable::mvcc::anomaly::

# Run read skew tests specifically
cargo test -p reifydb-transaction --test generated_tests generated_serializable::mvcc::anomaly::read_skew::
```

### Run Individual Tests

```bash
# Run a specific test by its full path
cargo test -p reifydb-transaction --test generated_tests generated_optimistic::mvcc::bank::bank

# Run with pattern matching (runs all tests matching the pattern)
cargo test -p reifydb-transaction --test generated_tests anomaly_write_skew
```

## Test Structure

### Generated Directory Structure

After building, the test structure looks like this:

```
crates/reifydb-transaction/tests/
├── optimistic.rs          # Test function: test_optimistic()
├── serializable.rs         # Test function: test_serializable()
├── generated_tests.rs      # Auto-generated main test file
├── generated_optimistic/   # Generated tests for optimistic concurrency
│   ├── mod.rs
│   ├── mvcc/
│   │   ├── mod.rs
│   │   ├── anomaly/
│   │   │   ├── dirty_write/
│   │   │   │   ├── 001.rs
│   │   │   │   ├── 002.rs
│   │   │   │   └── mod.rs
│   │   │   └── ...
│   │   └── bank/
│   │       ├── bank.rs
│   │       └── mod.rs
│   └── all/
│       └── ...
└── generated_serializable/ # Generated tests for serializable isolation
    └── [similar structure]
```

### Other Crates

For simpler crates with single generators:

```bash
# Run RQL tests
cargo test -p reifydb-rql --test generated

# Run specific RQL test modules
cargo test -p reifydb-rql --test generated tokenize::
cargo test -p reifydb-rql --test generated ast::
cargo test -p reifydb-rql --test generated logical_plan::

# Run storage tests
cargo test -p reifydb-storage --test generated unversioned_memory::
cargo test -p reifydb-storage --test generated versioned_sqlite::
```

## Debugging Individual Tests

Since each test is generated as a separate file, you can:

1. **Set breakpoints** directly in the generated test files
2. **Run in IDE** by clicking on the test function in your IDE
3. **Use rust-analyzer** for code navigation and debugging

Example locations for debugging:
- `tests/generated_optimistic/mvcc/anomaly/read_skew/001.rs`
- `tests/generated_serializable/mvcc/bank/bank.rs`

## Test Output

### Verbose Output

```bash
# See detailed test output
cargo test -p reifydb-transaction --test generated_tests -- --nocapture

# With test timing
cargo test -p reifydb-transaction --test generated_tests -- --nocapture --show-output
```

### Parallel Execution

```bash
# Run tests sequentially (useful for debugging)
cargo test -p reifydb-transaction --test generated_tests -- --test-threads=1

# Run with specific thread count
cargo test -p reifydb-transaction --test generated_tests -- --test-threads=4
```

## Regenerating Tests

If you modify test scripts or need to regenerate tests:

```bash
# Force rebuild (removes target directory)
cargo clean

# Then rebuild
cargo build --tests

# Or touch the build.rs to trigger regeneration
touch crates/reifydb-transaction/build.rs
cargo build --tests
```

## Adding New Test Scripts

1. Add your test script file to the appropriate `tests/scripts/` directory
2. The build script will automatically discover and generate tests for it
3. Run `cargo build --tests` to generate the new test files
4. Run the tests as described above

## Notes

- Generated test files are in `.gitignore` and should not be committed
- The `generated_tests.rs` file is also auto-generated and should not be edited manually
- Test functions must be public in the source test files (e.g., `pub fn test_optimistic()`)
  - Each test script becomes an individual test function that can be run and debugged independently