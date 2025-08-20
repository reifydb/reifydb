# ReifyDB Examples

A collection of executable examples demonstrating ReifyDB's capabilities.

## Quick Start

```bash
# Run the simplest example using make
make hello-world

# Or use cargo directly
cargo run --bin hello-world
```

## Available Examples

### Basic Operations

| Make Command | Description | Required Features |
|--------------|-------------|-------------------|
| `make hello-world` | Simple hello world example with in-memory database | None |
| `make basic-tables` | Table creation and schema definition example | None |

## Project Structure

```
bin/examples/
├── Cargo.toml           # Package configuration
├── Makefile            # Convenience targets
├── README.md           # This file
├── src/
│   └── lib.rs          # Shared utilities
└── examples/
    └── basic/          # Basic operations
        ├── 01_hello_world.rs
        └── 02_basic_tables.rs
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