# ReifyDB Developer Guide

This guide serves as the comprehensive reference for developers working on ReifyDB. It covers everything from initial setup to advanced development workflows, testing strategies, and contribution guidelines.

## Table of Contents

1. [Getting Started](#1-getting-started)
2. [Development Workflow](#2-development-workflow)
3. [Testing Strategy](#3-testing-strategy)
4. [Code Quality Standards](#4-code-quality-standards)
5. [Project Architecture](#5-project-architecture)
6. [Building and Cleaning](#6-building-and-cleaning)
7. [Language-Specific Development](#7-language-specific-development)
8. [Contributing Guidelines](#8-contributing-guidelines)
9. [Release Process](#9-release-process)
10. [Troubleshooting](#10-troubleshooting)
11. [Technical Design Decisions](#11-technical-design-decisions)
12. [Resources and References](#12-resources-and-references)

---

## 1. Getting Started

### Prerequisites

Before working on ReifyDB, ensure you have the following installed:

**Required:**
- **Rust** (1.92.0 or later) - Install via [rustup](https://rustup.rs/)
- **cargo-nextest** - Fast test runner: `cargo install cargo-nextest`
- **protoc** - Protocol Buffers compiler:
  - Ubuntu/Debian: `sudo apt-get install protobuf-compiler`
  - macOS: `brew install protobuf`
  - Or download from [protobuf releases](https://github.com/protocolbuffers/protobuf/releases)

**For TypeScript development:**
- **Node.js** (18.x or later)
- **pnpm** - Install via `npm install -g pnpm`

**For Python development:**
- **Python 3.8+**
- **maturin** - Install via `pip install maturin`

### Initial Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/reifyworks/reifydb.git
   cd reifydb
   ```

2. **Build the project**
   ```bash
   make build
   ```
   This builds all workspace crates in release mode.

3. **Run tests to verify setup**
   ```bash
   make test-dev
   ```
   This runs fast development tests to ensure everything is working.

### Environment Configuration

ReifyDB supports environment configuration via `.env` files:

```bash
# Create a .env file in the repository root
cat > .env <<EOF
# Example configuration
RUST_LOG=info
RUST_BACKTRACE=1
EOF
```

The Makefile automatically loads and exports variables from `.env` if it exists.

### Offline Mode with Vendor Directory

ReifyDB supports offline development using vendored dependencies:

If a `vendor/` directory exists in the repository root, the Makefile automatically adds the `--offline` flag to cargo commands. This allows building without network access.

**To create a vendor directory:**
```bash
cargo vendor > .cargo/config.toml
```

### Quick Start Commands

```bash
make help          # Show all available commands
make test-dev      # Run fast development tests (2-5 minutes)
make test          # Run full test suite (10-20 minutes)
make format        # Format all code
make build         # Build release version
make clean         # Clean all artifacts
```

---

## 2. Development Workflow

The development workflow is optimized for fast feedback during active development while ensuring comprehensive validation before committing.

### Fast Development Loop

**Use `make test-dev` for rapid iteration:**

```bash
make test-dev
```

This runs:
- Workspace tests (all crates)
- Rust package tests
- Example builds
- Fast development test suites only

**Typical completion time:** 2-3 minutes

**When to use:** During active development when you need quick feedback on your changes.

### Pre-Commit Validation

**Before committing, run the full test suite:**

```bash
make test
```

This runs:
- All workspace tests
- All package tests (Rust + TypeScript)
- All example builds
- Complete integration test suites

**Typical completion time:** 5 minutes

**When to use:** Before committing changes to ensure nothing breaks.

### Full CI Pipeline

**For complete validation, run:**

```bash
make all
```

This executes the complete CI/CD pipeline:
1. `check-code-quality` - Validate no internal re-exports
2. `check` - Verify no uncommitted changes
3. `clean` - Clean all artifacts
4. `build` - Build release version
5. `test-full` - Run all tests
6. `push-testcontainer` - Push Docker test container
7. `push` - Push to git remote

**When to use:** Final validation before opening a PR, or to replicate CI behavior locally.

### Development Best Practices

1. **Iterate quickly** with `make test-dev`
2. **Format before committing** with `make format`
3. **Validate before pushing** with `make test`
4. **Use specific tests** when working on a single crate:
   ```bash
   cargo nextest run -p reifydb-core
   cargo nextest run -p reifydb-engine -- test_name
   ```

---

## 3. Testing Strategy

ReifyDB uses a hierarchical testing strategy designed to balance fast feedback with comprehensive coverage.

### Test Hierarchy Overview

| Command | What it Tests | When to Use | Typical Duration |
|---------|---------------|-------------|------------------|
| `make test-dev` | Workspace + pkg-rust + examples + suite-dev | Active development iteration | Fast (2-5 min) |
| `make test-workspace` | All workspace crates | After modifying internal crates | Medium (3-7 min) |
| `make test-suite` | All integration test suites | Before commits, integration changes | Medium (5-10 min) |
| `make test-suite-dev` | Fast development subset of suites | Quick integration validation | Fast (1-3 min) |
| `make test-pkg-rust` | Rust package tests (`pkg/rust/*`) | After API changes | Fast (1-3 min) |
| `make test-pkg-typescript` | TypeScript package tests | After TS client changes | Fast (1-2 min) |
| `make test-examples` | All example builds and runs | Before releases, API changes | Medium (3-5 min) |
| `make test` (full) | Everything above combined | Before PR, final validation | Slow (10-20 min) |
| `make bench` | Performance benchmarks | Performance optimization work | Variable |

### Test Target Details

#### `make test-workspace`
Runs tests for all crates in the workspace using `cargo nextest`:
```bash
cargo nextest run --workspace --profile ci
```

**Includes:**
- All crates in `crates/*` (core, engine, catalog, transaction, etc.)
- All binaries in `bin/*`
- All packages in `pkg/rust/*`

**When to use:** After making changes to any internal crate.

#### `make test-suite`
Runs complete integration test suites located in `../testsuite`:
- Smoke tests
- Compatibility tests
- End-to-end scenarios
- Cross-version tests

**When to use:** Before committing, when changing behavior that affects integration.

#### `make test-suite-dev`
Runs a fast subset of integration tests suitable for development.

**When to use:** Quick validation during development without waiting for full suite.

#### `make test-pkg-rust`
Tests the public Rust packages:
- `pkg/rust/reifydb` - Main SDK
- `pkg/rust/reifydb-client` - Client library
- `pkg/rust/examples` - Example code
- `pkg/rust/tests/*` - Package-level tests

**When to use:** After modifying public APIs or client code.

#### `make test-pkg-typescript`
Tests TypeScript packages via pnpm:
```bash
cd pkg/typescript && pnpm test
```

**Includes:**
- `@reifydb/core` - Core types
- `@reifydb/client` - WebSocket/HTTP client
- `@reifydb/react` - React hooks

**When to use:** After modifying TypeScript bindings or client code.

#### `make test-examples`
Builds and runs all examples to ensure they compile and execute successfully.

**When to use:** Before releases, after API changes that affect examples.

### Running Specific Tests

**Run tests for a specific crate:**
```bash
cargo nextest run -p reifydb-core
cargo nextest run -p reifydb-engine
```

**Run a specific test by name:**
```bash
cargo nextest run -p reifydb-core -- test_row_encoding
```

**Run tests with output:**
```bash
cargo nextest run -p reifydb-core --no-capture
```

**Run tests in a specific directory:**
```bash
cargo nextest run --manifest-path crates/core/Cargo.toml
```

### Benchmarking

ReifyDB includes performance benchmarks for critical paths:

```bash
make bench              # Run all benchmarks
make bench-core         # Run core benchmarks only
make bench-engine       # Run engine benchmarks only
```

Benchmarks use Criterion.rs and generate reports in `target/criterion/`.

**When to use:** When optimizing performance-critical code or measuring regression.

### Why cargo-nextest?

ReifyDB uses [cargo-nextest](https://nexte.st/) instead of `cargo test` for several benefits:
- **Faster execution** through better parallelization
- **Cleaner output** with per-test status
- **Better CI integration** with JUnit XML reports
- **Improved flaky test detection**
- **Partitioned test runs** for distributed CI

---

## 4. Code Quality Standards

ReifyDB enforces strict code quality standards to maintain a clean, maintainable codebase.

### 4.1 Internal Re-export Policy

ReifyDB enforces code quality standards through automated checks in the Makefile:

**Check for internal re-exports:**
```bash
make check-code-quality
```

This validates that internal crates (`/crates/*`) don't use `pub use` for re-exporting internal types. Internal crates should use full module paths (e.g., `use reifydb_core::row::Row`) instead of re-exporting types via `pub use`. This improves code clarity and IDE navigation.

**Why:** ReifyDB maintains a strict policy against internal re-exports to ensure clear module boundaries and better discoverability of code.

**Public API:** Re-exports in the public API (`/pkg/rust/reifydb/src/lib.rs`) are intentionally preserved for a clean external interface.

**Automatic validation:**
The check runs automatically as the first step of `make all`:
```bash
make all  # Includes check-code-quality as first step
```

**Manual validation:**
You can also run the check script directly:
```bash
./scripts/check-internal-reexports.sh
```

#### Why This Policy Matters

1. **Clear Module Boundaries** - Each crate owns its types without ambiguity
2. **Better IDE Navigation** - "Go to definition" jumps to the actual implementation, not a re-export
3. **Explicit Dependencies** - Import statements clearly show where types come from
4. **Easier Refactoring** - Moving code doesn't break re-export chains
5. **Code Discoverability** - Developers can easily find the source of a type

#### Examples

**Violation (incorrect):**
```rust
// In crates/core/src/lib.rs
pub use crate::row::Row;          // ❌ Don't do this
pub use crate::value::Value;      // ❌ Don't do this
```

**Correct approach:**
```rust
// In crates/engine/src/execute.rs
use reifydb_core::row::Row;       // ✅ Full module path
use reifydb_core::value::Value;   // ✅ Full module path
```

#### Exception: Public API

Re-exports ARE allowed in the public API crate (`pkg/rust/reifydb/src/lib.rs`) to provide a clean external interface:

```rust
// In pkg/rust/reifydb/src/lib.rs
pub use reifydb_core::row::Row;   // ✅ OK for public API
pub use reifydb_engine::Engine;   // ✅ OK for public API
```

This allows external users to write:
```rust
use reifydb::Row;  // Clean public API
```

### 4.2 Top-Level Import Policy

All `use` statements must be at module level — never inside function bodies, match arms, closures, or other code blocks. This keeps imports visible and predictable at the top of each module.

**Automatic validation:**
```bash
./scripts/check-toplevel-imports.sh
```

This also runs as part of `make check-code-quality`.

#### Why This Policy Matters

1. **Predictable Scope** — All imports are visible at the top of the module
2. **Easier Review** — Reviewers can see all dependencies at a glance
3. **Consistent Style** — No mixing of import-at-use-site vs. import-at-top patterns
4. **Better Tooling** — `rustfmt` groups and sorts top-level imports automatically

#### Examples

**Violation (incorrect):**
```rust
fn compute(data: &[u8]) -> Result<()> {
    use std::io::Write;         // ❌ Inside function body
    use crate::util::encode;    // ❌ Inside function body
    // ...
}
```

**Correct approach:**
```rust
use std::io::Write;             // ✅ Module level
use crate::util::encode;        // ✅ Module level

fn compute(data: &[u8]) -> Result<()> {
    // ...
}
```

**Also correct — inside a `mod` block:**
```rust
mod inner {
    use super::*;               // ✅ Top of the module
    // ...
}
```

### 4.3 Code Formatting

ReifyDB uses `rustfmt` with a custom configuration (see `rustfmt.toml`).

**Format all code:**
```bash
make format
```

This runs:
```bash
cargo +nightly fmt --all
```

**Key formatting rules:**
- **Hard tabs** (tab_spaces = 8)
- **Max width**: 120 characters
- **Import grouping**: Std → External → Crate (via `group_imports = "StdExternalCrate"`)
- **Import granularity**: Crate-level (merges imports from same crate)
- **Parameter layout**: Tall (one parameter per line for functions)
- **Brace style**: Same line where clauses
- **Edition**: 2024
- **Comments**: Wrapped at 120 characters, normalized

**Pre-commit requirement:** Always run `make format` before committing to ensure consistent style.

**IDE integration:** Configure your editor to use the project's `rustfmt.toml`:
- **VS Code**: Install rust-analyzer extension (auto-detects rustfmt.toml)
- **IntelliJ/CLion**: Enable rustfmt in Preferences → Languages & Frameworks → Rust → Rustfmt
- **Vim/Neovim**: Use rust.vim or rust-tools.nvim

### 4.4 Pre-commit Checks

The `make check` target validates repository status before pushing:

```bash
make check
```

**What it checks:**
1. **Uncommitted changes** - Ensures working directory is clean
2. **Branch status** - Verifies local branch is up-to-date with remote
3. **Divergence detection** - Catches cases where branch has diverged

**Example output:**
```
🔍 Checking repository status...
📡 Fetching from remote...
✅ Repository check passed.
```

**Integration with CI:**
- `make all` runs `check` automatically
- `make push` requires `check` to pass before pushing

**When to use:** Before pushing to remote, or as part of your pre-commit workflow.

---

## 5. Building and Cleaning

### Build Commands

#### Build release version
```bash
make build
```

This builds all workspace crates in release mode with optimizations:
```bash
cargo build --workspace --release $(CARGO_OFFLINE)
```

**Output:** Binaries in `target/release/`

#### Build specific binary
```bash
cargo build --bin reifydb-server --release
cargo build --bin reifydb-cli --release
```

#### Development build (faster, debug symbols)
```bash
cargo build --workspace
```

### Clean Operations

#### Clean all artifacts
```bash
make clean
```

This removes:
- `target/` directory (all build artifacts)
- Rust package artifacts
- TypeScript build outputs

#### Clean specific crate
```bash
cargo clean -p reifydb-core
```

### Offline Mode

When a `vendor/` directory exists, the Makefile automatically uses `--offline` flag:

```bash
# Create vendor directory for offline builds
cargo vendor > .cargo/config.toml

# Now all builds use vendored dependencies
make build
```

**Use case:** Building in air-gapped environments or reducing network dependency.

### Docker Containers

#### Build test container
```bash
make build-testcontainer
```

This builds a Docker container with ReifyDB for testing purposes.

**Dockerfile location:** `bin/testcontainer/Dockerfile`

#### Push test container
```bash
make push-testcontainer
```

Pushes the test container to the configured registry.

#### Release Commands

```bash
# Auto-increment patch version (0.2.0 → 0.2.1)
make release

# Release specific version
make release VERSION=0.3.0

# Quick release variants
make release-patch        # Increment patch (x.y.Z)
make release-minor        # Increment minor (x.Y.0)
make release-major        # Increment major (X.0.0)

# Dry run (preview without executing)
make release-dry-run VERSION=0.3.0
```

### Release Process Overview

The release process (defined in `release.md`) includes:

1. **Version validation** - Ensure version follows semantic versioning
2. **Version update** - Update version in all Cargo.toml files
3. **Git tagging** - Create and push git tag
4. **Publishing** - Publish to crates.io
5. **NPM publishing** - Publish TypeScript packages to npm
6. **Python publishing** - Publish Python package to PyPI
7. **Verification** - Verify published packages

**Prerequisites:**
- Clean working directory (no uncommitted changes)
- Valid crates.io API token
- Valid npm authentication
- All tests passing

**Versioning:**
- All Rust crates share the same version (unified versioning)
- TypeScript packages have independent versions
- Python package follows Rust version

### When to Release

- **Patch release (x.y.Z)** - Bug fixes, minor improvements
- **Minor release (x.Y.0)** - New features, non-breaking changes
- **Major release (X.0.0)** - Breaking changes, major milestones

**Release cadence:**
- No fixed schedule
- Release when significant changes accumulate
- Critical bug fixes released promptly

### Getting Help with Releases

If you encounter issues during release:

1. Check [release.md](release.md) for detailed troubleshooting
2. Run `make help-release` for additional release commands
3. Open an issue if you find a bug in the release process

---

## 10. Troubleshooting

Common issues and solutions encountered during development.

### Test Failures

#### Issue: Port already in use
```
Error: Address already in use (os error 98)
```

**Solution:** Another process is using the test port.

```bash
# Find and kill the process
lsof -i :8080
kill -9 <PID>

# Or use a different port in tests
TEST_PORT=8081 cargo nextest run
```

#### Issue: cargo-nextest not found
```
cargo: 'nextest' is not a cargo command
```

**Solution:** Install cargo-nextest.

```bash
cargo install cargo-nextest
```

#### Issue: Flaky test
```
Test passes sometimes, fails other times
```

**Solution:**
1. Check for race conditions
2. Use deterministic timing (don't rely on `sleep`)
3. Run test repeatedly to reproduce:
   ```bash
   cargo nextest run --run-ignored all --retries 10 -- flaky_test
   ```

### Build Problems

#### Issue: protoc not found
```
error: failed to run custom build command for `reifydb-core`
Could not find `protoc` installation
```

**Solution:** Install Protocol Buffers compiler.

```bash
# Ubuntu/Debian
sudo apt-get install protobuf-compiler

# macOS
brew install protobuf

# Or download from https://github.com/protocolbuffers/protobuf/releases
```

#### Issue: Vendored dependencies not working
```
error: failed to load manifest for dependency `reifydb-core`
```

**Solution:** Ensure vendor directory is properly configured.

```bash
# Regenerate vendor directory
rm -rf vendor/
cargo vendor > .cargo/config.toml

# Try building again
make build
```

#### Issue: Out of disk space
```
error: no space left on device
```

**Solution:** Clean build artifacts.

```bash
make clean
# or
cargo clean

# Clean Docker images
docker system prune -a
```