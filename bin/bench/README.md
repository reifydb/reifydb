# ReifyDB Benchmarks

Performance benchmarking suite for ReifyDB using Criterion.rs.

## Quick Start

```bash
# Run all benchmarks using make
make bench-all

# Or run a specific benchmark
make bench-engine-basic

# Or use cargo directly
cargo bench --bench engine-basic-query
```

## Available Benchmarks

### Engine Benchmarks

| Make Command | Description | Benchmark File |
|--------------|-------------|----------------|
| `make bench-memory-optimistic` | Memory optimistic transaction benchmarks | `benches/engine/01_memory_optimistic.rs` |

The basic engine benchmark measures:
- Simple FROM queries
- FROM with MAP projection
- FROM with FILTER operations
- Complex multi-operator queries
- Query compilation vs execution overhead
- Performance scaling with different data sizes

## Project Structure

```
bin/bench/
├── Cargo.toml              # Package configuration with Criterion
├── Makefile                # Convenience targets for running benchmarks
├── README.md               # This file
├── src/
│   └── lib.rs              # Shared benchmark utilities
└── benches/
    └── engine/             # Engine performance benchmarks
        └── 01_basic_query.rs  # Basic RQL query benchmarks
```

## Running Benchmarks

### Using Make (Recommended)

```bash
# Run all benchmarks
make bench-all

# Run specific benchmarks
make bench-memory-optimistic

# Save results as baseline for comparison
make bench-baseline BASELINE=v1.0.0

# Compare current performance to baseline
make bench-compare BASELINE=v1.0.0

# Open HTML reports in browser
make bench-report
```

### Using Cargo Directly

```bash
# Run specific benchmark
cargo bench --bench engine-memory-optimistic

# Run with additional verbosity
cargo bench --bench engine-memory-optimistic -- --verbose

# Save baseline
cargo bench --bench engine-memory-optimistic -- --save-baseline main

# Compare to baseline
cargo bench --bench engine-memory-optimistic -- --baseline main
```

## Benchmark Results

Results are stored in `target/criterion/` and include:
- **HTML Reports**: Detailed charts and analysis at `target/criterion/report/index.html`
- **Raw Data**: JSON files with timing measurements
- **Baseline Comparisons**: Performance difference analysis

### Reading Results

- **Time**: Lower is better (measured in nanoseconds, microseconds, or milliseconds)
- **Throughput**: Higher is better (operations per second)
- **Confidence Intervals**: Shows measurement reliability (95% confidence)
- **R² Values**: Indicates measurement consistency (closer to 1.0 is better)

## Development

### Building Benchmarks

```bash
make build
# or
cargo build --benches
```

### Adding New Benchmarks

1. Create the benchmark file in the appropriate category directory:
   ```rust
   // benches/engine/02_new_benchmark.rs
   use criterion::{criterion_group, criterion_main, Criterion};
   use reifydb_bench::create_benchmark_db_with_data;
   
   fn bench_new_feature(c: &mut Criterion) {
       let db = create_benchmark_db_with_data();
       c.bench_function("new_feature", |b| {
           b.iter(|| {
               // Your benchmark code here
           })
       });
   }
   
   criterion_group!(benches, bench_new_feature);
   criterion_main!(benches);
   ```

2. Register it in `Cargo.toml`:
   ```toml
   [[bench]]
   name = "engine-new-benchmark"
   path = "benches/engine/02_new_benchmark.rs"
   harness = false
   ```

3. Add a make target in the `Makefile`:
   ```makefile
   .PHONY: bench-engine-new
   bench-engine-new:
       cargo bench --bench engine-new-benchmark $(BENCH_FLAGS)
   ```

4. Update this README to document the new benchmark

### Shared Utilities

The `src/lib.rs` file provides common utilities:
- `create_benchmark_db()`: Creates a clean in-memory database
- `create_benchmark_db_with_data()`: Creates a database with sample data
- `queries::*`: Common RQL queries for consistent benchmarking

### Best Practices

1. **Consistent Setup**: Use shared utilities for database creation
2. **Representative Data**: Include realistic data sizes and types
3. **Complete Execution**: Always consume query iterators fully
4. **Baseline Comparisons**: Save baselines before major changes
5. **Multiple Measurements**: Let Criterion handle statistical analysis

## Performance Goals

Current performance targets (subject to change):
- Simple queries: < 10μs on modern hardware
- Complex queries: < 100μs for small datasets (< 1000 rows)
- Memory usage: Minimal allocation overhead
- Scalability: Linear with data size for most operations

## Troubleshooting

### Common Issues

1. **Benchmark won't compile**: Check that Criterion version matches workspace
2. **Database setup fails**: Verify ReifyDB API compatibility
3. **Inconsistent results**: Ensure system is under low load during benchmarks
4. **Missing reports**: Run benchmarks first, then check `target/criterion/`

### System Requirements

- **CPU**: Modern multi-core processor recommended
- **Memory**: At least 4GB available RAM
- **Disk**: SSD recommended for faster compilation
- **OS**: Linux, macOS, or Windows with proper Rust toolchain

## License

See the main project LICENSE file.