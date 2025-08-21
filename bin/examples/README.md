# ReifyDB Examples

A collection of executable examples demonstrating ReifyDB's capabilities.

## Quick Start

```bash
# Run the simplest example using make
make basic-hello-world

# Or use cargo directly
cargo run --bin basic-hello-world
```

## Available Examples

### Basic Operations

| Make Command | Description | Required Features |
|--------------|-------------|-------------------|
| `make basic-hello-world` | Simple hello world example with in-memory database | None |
| `make basic-tables` | Table creation and schema definition example | None |

### RQL (Query Language) Examples

| Make Command | Description | Required Features |
|--------------|-------------|-------------------|
| `make rql-from` | FROM operator: loading data from inline arrays and tables | None |
| `make rql-map` | MAP operator: projection and computed fields | None |
| `make rql-filter` | FILTER operator: filtering data with conditions | None |
| `make rql-sort` | SORT operator: sorting results | None |
| `make rql-take` | TAKE operator: limiting results | None |
| `make rql-aggregate` | AGGREGATE operator: aggregation functions (avg, sum, count) | None |
| `make rql-join` | JOIN operator: inner, left, and natural joins | None |
| `make rql-arithmetic` | Arithmetic expressions in queries | None |
| `make rql-comparison` | Comparison operators (==, !=, <, >, <=, >=, between) | None |
| `make rql-logical` | Logical operators (and, or, not, xor) | None |

## Project Structure

```
bin/examples/
├── Cargo.toml           # Package configuration
├── Makefile            # Convenience targets
├── README.md           # This file
├── src/
│   └── lib.rs          # Shared utilities
└── examples/
    ├── basic/          # Basic operations
    │   ├── 01_hello_world.rs
    │   └── 02_basic_tables.rs
    └── rql/            # RQL query language examples
        ├── 01_from_operator.rs         # FROM operator
        ├── 02_map_operator.rs          # MAP operator
        ├── 03_filter_operator.rs       # FILTER operator
        ├── 04_sort_operator.rs         # SORT operator
        ├── 05_take_operator.rs         # TAKE operator
        ├── 06_aggregate_operator.rs    # AGGREGATE operator
        ├── 07_join_operator.rs         # JOIN operator
        ├── 08_arithmetic_expressions.rs # Arithmetic operations
        ├── 09_comparison_operators.rs  # Comparison operators
        └── 10_logical_operators.rs     # Logical operators
```

## Running Examples

### Using Make (Recommended)
```bash
make basic-tables
```

### Using Cargo Directly
```bash
cargo run --bin basic-tables
```

## Development

### Building Examples
```bash
make build
```

### Clean Build
```bash
make clean
```

## Adding New Examples

As you add more examples:

1. Create the example file in the appropriate category directory
2. Register it in `Cargo.toml` with any required features:
   ```toml
   [[bin]]
   name = "example-name"
   path = "examples/category/filename.rs"
   required-features = ["feature-name"]  # if features are needed
   ```
3. Add a make target in the `Makefile`:
   ```makefile
   .PHONY: example-name
   example-name:
       cargo run --features feature-name --bin example-name
   ```
4. Update this README to document the new example


## License

See the main project LICENSE file.